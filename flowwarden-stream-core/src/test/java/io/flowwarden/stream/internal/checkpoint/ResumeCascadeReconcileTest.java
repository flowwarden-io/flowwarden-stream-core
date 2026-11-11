/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.flowwarden.stream.internal.checkpoint;

import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import io.flowwarden.stream.FlowWardenMetrics;
import io.flowwarden.stream.HistoryLostContext;
import io.flowwarden.stream.HistoryLostException;
import io.flowwarden.stream.OnHistoryLost;
import io.flowwarden.stream.ReconciliationFailedException;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.StreamMetricsProvider;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.annotation.AnnotationUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The two rules a {@code @Reconcile} hook adds to cascade level 3, pinned
 * against stubs: no durable write before the hook has returned, and at most
 * two invocations per start attempt. Without a hook, the write sequence is
 * the one every other test already relies on.
 */
class ResumeCascadeReconcileTest {

    private static final String STREAM = "reconcile-cascade";
    private static final BsonDocument DEAD = BsonDocument.parse("{\"_data\": \"DEADBEEF\"}");
    private static final BsonDocument PBRT_1 = BsonDocument.parse("{\"_data\": \"PBRT1\"}");
    private static final BsonDocument PBRT_2 = BsonDocument.parse("{\"_data\": \"PBRT2\"}");
    private static final BsonDocument PBRT_3 = BsonDocument.parse("{\"_data\": \"PBRT3\"}");
    private static final Instant PROCESSED_AT = Instant.parse("2026-11-01T10:00:00Z");
    private static final Instant SEEN_AT = Instant.parse("2026-11-01T12:00:00Z");
    private static final Instant HEARTBEAT_AT = Instant.parse("2026-11-01T12:30:00Z");
    private static final BsonTimestamp OLDEST = new BsonTimestamp(1_761_993_600, 7);

    private RecordingStore store;
    private StubProbe probe;
    private Set<BsonDocument> validTokens;
    private RecordingMetrics metrics;
    private List<HistoryLostContext> hookCalls;

    @BeforeEach
    void setUp() {
        store = new RecordingStore();
        probe = new StubProbe();
        validTokens = new HashSet<>(Set.of(PBRT_1, PBRT_2, PBRT_3));
        metrics = new RecordingMetrics();
        FlowWardenMetrics.setProvider(metrics);
        hookCalls = new CopyOnWriteArrayList<>();
    }

    @AfterEach
    void tearDown() {
        FlowWardenMetrics.setProvider(StreamMetricsProvider.noOp());
    }

    // --- RESUME_FROM_NOW ---

    @Test
    void now_withHook_hookRunsBeforeTheWrite_andTheContextCarriesTheBound() {
        Instant before = Instant.now();
        List<Integer> writesSeenByHook = new ArrayList<>();

        ResumeContext result = resolve(OnHistoryLost.RESUME_FROM_NOW, ctx -> {
            hookCalls.add(ctx);
            writesSeenByHook.add(store.resets.size());
        });

        assertThat(writesSeenByHook).containsExactly(0);
        assertThat(store.resets).hasSize(1);
        assertThat(store.resets.get(0).fresh).isEqualTo(PBRT_1);
        assertThat(store.resets.get(0).expectedDeadProcessed).isEqualTo(DEAD);
        assertThat(result.seedToken()).isEqualTo(PBRT_1);

        HistoryLostContext ctx = hookCalls.get(0);
        assertThat(ctx.streamName()).isEqualTo(STREAM);
        assertThat(ctx.strategy()).isEqualTo(OnHistoryLost.RESUME_FROM_NOW);
        assertThat(ctx.attempt()).isEqualTo(1);
        assertThat(ctx.lastProcessedTimestamp()).contains(PROCESSED_AT);
        assertThat(ctx.lastSeenTimestamp()).contains(SEEN_AT);
        assertThat(ctx.lastHeartbeatTimestamp()).contains(HEARTBEAT_AT);
        assertThat(ctx.lostWindowStart()).contains(HEARTBEAT_AT);
        assertThat(ctx.lostWindowEnd()).isAfterOrEqualTo(before);
        assertThat(metrics.reconciled).containsExactly(1);
        assertThat(metrics.failed).isEmpty();
    }

    @Test
    void now_hookThrows_nothingIsWritten_startFailsLikeFail_withTheCause() {
        IllegalStateException resyncFailure = new IllegalStateException("resync failed");

        assertThatThrownBy(() -> resolve(OnHistoryLost.RESUME_FROM_NOW, ctx -> {
            throw resyncFailure;
        }))
                .isInstanceOf(ReconciliationFailedException.class)
                .isInstanceOf(HistoryLostException.class)
                .hasCause(resyncFailure)
                .hasMessageContaining("resync failed")
                .satisfies(e -> assertThat(((ReconciliationFailedException) e).getAttempt()).isEqualTo(1));

        assertThat(store.resets).isEmpty();
        assertThat(store.seenWrites).isEmpty();
        assertThat(metrics.reconciled).isEmpty();
        assertThat(metrics.failed).hasSize(1);
        assertThat(metrics.failed.get(0).cause).isSameAs(resyncFailure);
    }

    @Test
    void now_positionRollsOutDuringTheHook_oneBoundedSecondCall_thenTheFreshPositionIsWritten() {
        resolve(OnHistoryLost.RESUME_FROM_NOW, ctx -> {
            hookCalls.add(ctx);
            if (ctx.attempt() == 1) {
                validTokens.remove(PBRT_1); // the oplog rolled past it while we reconciled
            }
        });

        assertThat(hookCalls).extracting(HistoryLostContext::attempt).containsExactly(1, 2);
        HistoryLostContext first = hookCalls.get(0);
        HistoryLostContext second = hookCalls.get(1);
        assertThat(second.lostWindowStart()).contains(first.lostWindowEnd());
        assertThat(second.lostWindowEnd()).isAfterOrEqualTo(first.lostWindowEnd());
        assertThat(second.lastHeartbeatTimestamp()).contains(HEARTBEAT_AT);
        assertThat(store.resets).hasSize(1);
        assertThat(store.resets.get(0).fresh).isEqualTo(PBRT_2);
        assertThat(metrics.reconciled).containsExactly(1, 2);
    }

    @Test
    void now_positionRollsOutTwice_failsClearly_doesNotFitInTheOplog() {
        assertThatThrownBy(() -> resolve(OnHistoryLost.RESUME_FROM_NOW, ctx -> {
            hookCalls.add(ctx);
            validTokens.clear(); // every position we capture rolls out while we reconcile
        }))
                .isInstanceOf(ReconciliationFailedException.class)
                .hasNoCause()
                .hasMessageContaining("does not fit in the oplog window")
                .satisfies(e -> assertThat(((ReconciliationFailedException) e).getAttempt()).isEqualTo(2));

        assertThat(hookCalls).extracting(HistoryLostContext::attempt).containsExactly(1, 2);
        assertThat(store.resets).isEmpty();
        assertThat(metrics.reconciled).containsExactly(1, 2);
        assertThat(metrics.failed).hasSize(1);
        assertThat(metrics.failed.get(0).attempt).isEqualTo(2);
    }

    @Test
    void now_metricsProviderThrowsOnFailureSignal_theTerminalExceptionStillCarriesTheHookCause() {
        metrics.failing = true;
        IllegalArgumentException resyncFailure = new IllegalArgumentException("resync failed");

        assertThatThrownBy(() -> resolve(OnHistoryLost.RESUME_FROM_NOW, ctx -> {
            throw resyncFailure;
        }))
                .isInstanceOf(ReconciliationFailedException.class)
                .hasCause(resyncFailure);

        assertThat(store.resets).isEmpty();
        assertThat(store.seenWrites).isEmpty();
        assertThat(metrics.failed).hasSize(1);
    }

    @Test
    void now_metricsProviderThrowsOnSuccessSignal_theStartStillValidatesAndWritesTheFreshPosition() {
        metrics.failing = true;

        ResumeContext result = resolve(OnHistoryLost.RESUME_FROM_NOW, hookCalls::add);

        assertThat(hookCalls).hasSize(1);
        assertThat(result.seedToken()).isEqualTo(PBRT_1);
        assertThat(store.resets).hasSize(1);
        assertThat(store.resets.get(0).fresh).isEqualTo(PBRT_1);
        assertThat(metrics.reconciled).containsExactly(1);
    }

    @Test
    void now_metricsProviderThrows_theDoesNotFitFailureIsStillTheOneReported() {
        metrics.failing = true;

        assertThatThrownBy(() -> resolve(OnHistoryLost.RESUME_FROM_NOW, ctx -> validTokens.clear()))
                .isInstanceOf(ReconciliationFailedException.class)
                .hasNoCause()
                .hasMessageContaining("does not fit in the oplog window");

        assertThat(store.resets).isEmpty();
        assertThat(metrics.reconciled).containsExactly(1, 2);
        assertThat(metrics.failed).extracting(f -> f.attempt).containsExactly(2);
    }

    @Test
    void now_withoutHook_writesImmediately_sequenceUnchanged() {
        ResumeContext result = resolve(OnHistoryLost.RESUME_FROM_NOW, null);

        assertThat(result.seedToken()).isEqualTo(PBRT_1);
        assertThat(store.resets).hasSize(1);
        assertThat(metrics.reconciled).isEmpty();
    }

    // --- RESUME_FROM_OPLOG_START ---

    @Test
    void oplogStart_withHook_boundEndsAtTheOldestEntry_andNothingIsWritten() {
        ResumeContext result = resolve(OnHistoryLost.RESUME_FROM_OPLOG_START, hookCalls::add);

        assertThat(hookCalls).hasSize(1);
        HistoryLostContext ctx = hookCalls.get(0);
        assertThat(ctx.strategy()).isEqualTo(OnHistoryLost.RESUME_FROM_OPLOG_START);
        assertThat(ctx.attempt()).isEqualTo(1);
        assertThat(ctx.lostWindowStart()).contains(HEARTBEAT_AT);
        assertThat(ctx.lostWindowEnd()).isEqualTo(Instant.ofEpochSecond(OLDEST.getTime()));
        // The dead tokens stay as the marker; the establishment chain writes later.
        assertThat(store.resets).isEmpty();
        assertThat(store.seenWrites).isEmpty();
        assertThat(result.seedToken()).isNull();
        assertThat(result.initialOperationTime()).isEqualTo(OLDEST);
        assertThat(result.deadProcessedToken()).isEqualTo(DEAD);
        assertThat(metrics.reconciled).containsExactly(1);
    }

    @Test
    void oplogStart_oldestEntryRollsOutDuringTheHook_degradesToOneBoundedCallOnAFreshPosition() {
        probe.operationTimeOutcomes.add(ProbeOutcome.empty(PBRT_1)); // validation before the hook
        probe.operationTimeOutcomes.add(ProbeOutcome.failed(historyLost())); // re-check after it

        ResumeContext result = resolve(OnHistoryLost.RESUME_FROM_OPLOG_START, hookCalls::add);

        assertThat(hookCalls).extracting(HistoryLostContext::attempt).containsExactly(1, 2);
        HistoryLostContext second = hookCalls.get(1);
        assertThat(second.lostWindowStart()).contains(Instant.ofEpochSecond(OLDEST.getTime()));
        assertThat(result.seedToken()).isEqualTo(PBRT_1);
        assertThat(result.initialOperationTime()).isNull();
        assertThat(store.resets).hasSize(1);
        assertThat(store.resets.get(0).fresh).isEqualTo(PBRT_1);
    }

    @Test
    void oplogStart_degradedBeforeTheHook_stillReconcilesBeforeWriting() {
        // The oldest entry is already gone at validation time: the recovery
        // degrades to a fresh position — with a hook, after reconciling.
        probe.operationTimeOutcomes.add(ProbeOutcome.failed(historyLost()));
        List<Integer> writesSeenByHook = new ArrayList<>();

        resolve(OnHistoryLost.RESUME_FROM_OPLOG_START, ctx -> {
            hookCalls.add(ctx);
            writesSeenByHook.add(store.resets.size());
        });

        assertThat(writesSeenByHook).containsExactly(0);
        assertThat(hookCalls).hasSize(1);
        assertThat(hookCalls.get(0).strategy()).isEqualTo(OnHistoryLost.RESUME_FROM_OPLOG_START);
        assertThat(store.resets).hasSize(1);
    }

    @Test
    void fail_neverCallsTheHook() {
        assertThatThrownBy(() -> resolve(OnHistoryLost.FAIL, hookCalls::add))
                .isInstanceOf(HistoryLostException.class)
                .isNotInstanceOf(ReconciliationFailedException.class);
        assertThat(hookCalls).isEmpty();
    }

    // --- plumbing ---

    private ResumeContext resolve(OnHistoryLost strategy, ResumeCascade.ReconcileHook hook) {
        Checkpoint annotation = AnnotationUtils.synthesizeAnnotation(
                Map.of("onHistoryLost", strategy), Checkpoint.class, null);
        return ResumeCascade.resolve(STREAM, annotation, store, probe,
                token -> validTokens.contains(token), () -> OLDEST, true, hook);
    }

    private static MongoCommandException historyLost() {
        BsonDocument response = new BsonDocument("ok", new BsonInt32(0))
                .append("code", new BsonInt32(286))
                .append("codeName", new BsonString("ChangeStreamHistoryLost"))
                .append("errmsg", new BsonString("resume point may no longer be in the oplog"));
        return new MongoCommandException(response, new ServerAddress());
    }

    /** Hands out PBRT_1, PBRT_2, PBRT_3 in turn; operation-time probes succeed unless scripted. */
    private static final class StubProbe implements HeartbeatProbe {
        final Deque<BsonDocument> positions = new ArrayDeque<>(List.of(PBRT_1, PBRT_2, PBRT_3));
        final Deque<ProbeOutcome> operationTimeOutcomes = new ArrayDeque<>();

        @Override
        public ProbeOutcome probe(BsonDocument resumeAfter) {
            return ProbeOutcome.empty(resumeAfter);
        }

        @Override
        public ProbeOutcome probeFromOperationTime(BsonTimestamp operationTime) {
            ProbeOutcome scripted = operationTimeOutcomes.poll();
            return scripted != null ? scripted : ProbeOutcome.empty(PBRT_1);
        }

        @Override
        public BsonDocument initialPosition() {
            BsonDocument next = positions.poll();
            if (next == null) {
                throw new IllegalStateException("no more positions scripted");
            }
            return next;
        }
    }

    private static final class RecordingStore implements CheckpointStore {
        record Reset(BsonDocument fresh, BsonDocument expectedDeadProcessed) {}

        final List<Reset> resets = new ArrayList<>();
        final List<BsonDocument> seenWrites = new ArrayList<>();

        @Override
        public void save(io.flowwarden.stream.spi.Checkpoint checkpoint) {
            throw new UnsupportedOperationException("not expected in these scenarios");
        }

        @Override
        public Optional<io.flowwarden.stream.spi.Checkpoint> findByStreamName(String streamName) {
            return Optional.of(new io.flowwarden.stream.spi.Checkpoint(
                    STREAM, null, DEAD, SEEN_AT, DEAD, PROCESSED_AT, HEARTBEAT_AT, Map.of()));
        }

        @Override
        public void saveSeen(String streamName, BsonDocument token, Instant timestamp) {
            seenWrites.add(token);
        }

        @Override
        public void saveSeen(String streamName, BsonDocument token, Instant timestamp,
                             Instant heartbeatTimestamp) {
            seenWrites.add(token);
        }

        @Override
        public void resetAfterHistoryLost(String streamName, BsonDocument freshSeenToken,
                                          BsonDocument expectedDeadProcessed, Instant timestamp) {
            resets.add(new Reset(freshSeenToken, expectedDeadProcessed));
        }

        @Override
        public void delete(String streamName) {
        }
    }

    private static final class RecordingMetrics implements StreamMetricsProvider {
        record Failure(int attempt, Throwable cause) {}

        final List<Integer> reconciled = new ArrayList<>();
        final List<Failure> failed = new ArrayList<>();
        /** When set, both reconciliation callbacks record the signal, then throw. */
        boolean failing;

        @Override
        public void onStreamStarted(String streamName, io.flowwarden.stream.spi.StreamConfiguration config) {}

        @Override
        public void onEventReceived(String streamName, io.flowwarden.stream.spi.ChangeEventMetadata metadata) {}

        @Override
        public void onEventProcessed(String streamName, long durationNanos, boolean success) {}

        @Override
        public void onEventError(String streamName, Throwable error, boolean willRetry,
                                 int attemptNumber, io.flowwarden.stream.spi.ChangeEventMetadata metadata) {}

        @Override
        public void onCheckpoint(String streamName, String resumeToken) {}

        @Override
        public void onBufferStatus(String streamName, int currentSize, int maxSize) {}

        @Override
        public void onBackpressure(String streamName, io.flowwarden.stream.spi.BackpressureAction action) {}

        @Override
        public void onEventSentToDlq(String streamName) {}

        @Override
        public void onOplogStats(double logLengthHours, String status) {}

        @Override
        public void onHistoryLostReconciled(String streamName, int attempt, Duration elapsed) {
            reconciled.add(attempt);
            if (failing) {
                throw new IllegalStateException("telemetry down (reconciled)");
            }
        }

        @Override
        public void onHistoryLostReconciliationFailed(String streamName, int attempt, Throwable cause) {
            failed.add(new Failure(attempt, cause));
            if (failing) {
                throw new IllegalStateException("telemetry down (failed)");
            }
        }
    }
}
