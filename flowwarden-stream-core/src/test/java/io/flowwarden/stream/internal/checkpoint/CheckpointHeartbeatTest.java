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

import io.flowwarden.stream.FlowWardenMetrics;
import io.flowwarden.stream.spi.BackpressureAction;
import io.flowwarden.stream.spi.ChangeEventMetadata;
import io.flowwarden.stream.spi.Checkpoint;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.StreamConfiguration;
import io.flowwarden.stream.spi.StreamMetricsProvider;
import org.bson.BsonDocument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the split heartbeat responsibilities:
 *
 * <p>{@code flushTick} (saveIntervalSeconds) — dirty-only event-token
 * coalescing, never probes. {@code idleTick} (idleHeartbeatIntervalSeconds) —
 * probes only past the idle threshold, with the three-outcome contract
 * (EMPTY advance / re-certification, EVENT_PENDING abstention, FAILED
 * signal), the CAS against concurrent events, the monotonicity guard, the
 * SEED/EVENT distinction and the cancellation flag.</p>
 */
class CheckpointHeartbeatTest {

    private static final String STREAM = "hb-test";
    private static final Duration IDLE_THRESHOLD = Duration.ofSeconds(60);
    private static final Instant NOW = Instant.now();
    private static final Instant IDLE_SINCE = NOW.minusSeconds(120);
    // _data values are ordered: T10 < T20 < T30 (lexicographic, like real tokens)
    private static final BsonDocument T10 = BsonDocument.parse("{\"_data\": \"10\"}");
    private static final BsonDocument T20 = BsonDocument.parse("{\"_data\": \"20\"}");
    private static final BsonDocument T30 = BsonDocument.parse("{\"_data\": \"30\"}");

    private RecordingStore store;
    private RecordingMetrics metrics;
    private AtomicReference<TokenSnapshot> ref;
    private List<BsonDocument> probed;

    @BeforeEach
    void setUp() {
        store = new RecordingStore();
        metrics = new RecordingMetrics();
        ref = new AtomicReference<>();
        probed = new ArrayList<>();
        FlowWardenMetrics.setProvider(metrics);
    }

    @AfterEach
    void tearDown() {
        FlowWardenMetrics.setProvider(StreamMetricsProvider.noOp());
    }

    private CheckpointHeartbeat heartbeat(Function<BsonDocument, ProbeOutcome> probeFn) {
        return new CheckpointHeartbeat(STREAM, store, probeOf(probeFn), () -> ref,
                true, IDLE_THRESHOLD);
    }

    private CheckpointHeartbeat latestModeHeartbeat(Function<BsonDocument, ProbeOutcome> probeFn) {
        return new CheckpointHeartbeat(STREAM, store, probeOf(probeFn), () -> ref,
                false, IDLE_THRESHOLD);
    }

    private HeartbeatProbe probeOf(Function<BsonDocument, ProbeOutcome> fn) {
        return new HeartbeatProbe() {
            @Override
            public ProbeOutcome probe(BsonDocument resumeAfter) {
                probed.add(resumeAfter);
                return fn.apply(resumeAfter);
            }

            @Override
            public BsonDocument initialPosition() {
                throw new UnsupportedOperationException("not used by tick tests");
            }
        };
    }

    // --- flushTick: dirty-only coalescing, never a probe ---

    @Test
    void flush_dirtyEventToken_isPersistedWithHeartbeat_withoutProbing() {
        Instant eventTime = NOW.minusSeconds(3);
        ref.set(new TokenSnapshot(T10, eventTime));

        heartbeat(r -> ProbeOutcome.eventPending()).flushTick();

        assertThat(probed).isEmpty();
        assertThat(store.calls).containsExactly("saveSeen+hb:10");
        assertThat(store.lastSeenTimestamp).isEqualTo(eventTime);
        assertThat(store.lastHeartbeatTimestamp).isAfterOrEqualTo(eventTime);
    }

    @Test
    void flush_cleanTick_writesNothing() {
        ref.set(new TokenSnapshot(T10, NOW));
        CheckpointHeartbeat hb = heartbeat(r -> ProbeOutcome.eventPending());

        hb.flushTick(); // dirty → persisted
        hb.flushTick(); // clean → zero writes
        hb.flushTick();

        assertThat(store.calls).containsExactly("saveSeen+hb:10");
        assertThat(probed).isEmpty();
    }

    @Test
    void flush_seedSnapshot_isNeverPersistedAsDelivery() {
        // PROCESSED_FIRST resume: the seed is the OLD processed token (T10)
        // while the persisted seen is fresher (T20).
        store.persisted = checkpointWithSeen(T20);
        ref.set(new TokenSnapshot(T10, NOW, TokenSnapshot.Source.SEED));

        heartbeat(r -> ProbeOutcome.eventPending()).flushTick();

        assertThat(store.calls).as("a resume seed must never be flushed as seen").isEmpty();
        assertThat(probed).isEmpty();
    }

    @Test
    void flush_replayedEventBelowHighWaterMark_doesNotRegressSeen() {
        // Replay after a PROCESSED_FIRST resume: the delivered event's token
        // (T10) is older than the persisted seen (T20).
        store.persisted = checkpointWithSeen(T20);
        ref.set(new TokenSnapshot(T10, NOW)); // EVENT source
        CheckpointHeartbeat hb = heartbeat(r -> ProbeOutcome.eventPending());

        hb.flushTick();
        assertThat(store.calls)
                .as("a replayed event below the high-water mark is heartbeat-only")
                .containsExactly("saveHeartbeat");

        // Once the stream catches up past the mark, seen advances again.
        ref.set(new TokenSnapshot(T30, NOW));
        hb.flushTick();
        assertThat(store.calls).containsExactly("saveHeartbeat", "saveSeen+hb:30");
    }

    // --- idleTick: probes only past the idle threshold ---

    @Test
    void idle_beforeThreshold_neverProbes() {
        ref.set(new TokenSnapshot(T10, NOW)); // fresh activity

        heartbeat(r -> ProbeOutcome.empty(T20)).idleTick();

        assertThat(probed).isEmpty();
        assertThat(store.calls).isEmpty();
    }

    @Test
    void idle_pastThreshold_probeAdvancesPosition() {
        ref.set(new TokenSnapshot(T10, IDLE_SINCE));

        heartbeat(r -> ProbeOutcome.empty(T20)).idleTick();

        assertThat(probed).containsExactly(T10);
        assertThat(store.calls).containsExactly("saveSeen+hb:20");
        assertThat(ref.get().token()).isEqualTo(T20);
        // The probe result re-arms the idle delay (fresh SEED snapshot).
        assertThat(ref.get().source()).isEqualTo(TokenSnapshot.Source.SEED);
    }

    @Test
    void idle_probeReturnsSamePbrt_writesHeartbeatOnly() {
        ref.set(new TokenSnapshot(T10, IDLE_SINCE));

        heartbeat(r -> ProbeOutcome.empty(T10)).idleTick();

        assertThat(store.calls).containsExactly("saveHeartbeat");
        assertThat(ref.get().token()).isEqualTo(T10);
    }

    @Test
    void idle_probeEventPending_abstains_noWrites() {
        ref.set(new TokenSnapshot(T10, IDLE_SINCE));

        heartbeat(r -> ProbeOutcome.eventPending()).idleTick();

        assertThat(store.calls).isEmpty();
    }

    @Test
    void idle_probeFailure_noWrites_emitsDedicatedSignal() {
        ref.set(new TokenSnapshot(T10, IDLE_SINCE));
        RuntimeException boom = new RuntimeException("history lost");

        heartbeat(r -> ProbeOutcome.failed(boom)).idleTick();

        assertThat(store.calls).isEmpty();
        assertThat(metrics.probeFailures).containsExactly(boom);
    }

    @Test
    void idle_eventArrivingDuringProbe_winsTheCas_probeResultDiscarded() {
        ref.set(new TokenSnapshot(T10, IDLE_SINCE));
        CheckpointHeartbeat hb = heartbeat(r -> {
            // Simulate the main stream delivering an event mid-probe.
            ref.set(new TokenSnapshot(T20, Instant.now()));
            return ProbeOutcome.empty(T30);
        });

        hb.idleTick();

        assertThat(store.calls).isEmpty();
        assertThat(ref.get().token()).isEqualTo(T20);

        // The winning event token is persisted by the next flush.
        hb.flushTick();
        assertThat(store.calls).containsExactly("saveSeen+hb:20");
    }

    @Test
    void idle_probePbrtBelowHighWaterMark_isDowngradedToHeartbeatOnly() {
        // Bounded-scan intermediate PBRT below the persisted mark (probe
        // chained from an old processed seed).
        store.persisted = checkpointWithSeen(T30);
        ref.set(new TokenSnapshot(T10, IDLE_SINCE, TokenSnapshot.Source.SEED));

        heartbeat(r -> ProbeOutcome.empty(T20)).idleTick();

        assertThat(store.calls).containsExactly("saveHeartbeat");
    }

    @Test
    void idle_noInMemoryToken_resumeMode_chainsFromPersistedSeen() {
        store.persisted = checkpointWithSeen(T10);

        heartbeat(r -> ProbeOutcome.empty(T20)).idleTick();

        assertThat(probed).containsExactly(T10);
        assertThat(store.calls).containsExactly("saveSeen+hb:20");
    }

    @Test
    void idle_noInMemoryToken_latestMode_neverFallsBackToPersistedCheckpoint() {
        // LATEST semantics ignore persisted tokens: chaining from one would
        // strand the probe on history the main stream will never consume.
        store.persisted = checkpointWithSeen(T10);

        latestModeHeartbeat(r -> ProbeOutcome.empty(T20)).idleTick();

        assertThat(probed).isEmpty();
        assertThat(store.calls).isEmpty();
    }

    @Test
    void idle_noPositionAnywhere_neverProbes() {
        heartbeat(r -> ProbeOutcome.empty(T20)).idleTick();

        assertThat(probed).isEmpty();
        assertThat(store.calls).isEmpty();
    }

    // --- startup validation ---

    @Test
    void startupValidation_probesRegardlessOfIdleness() {
        ref.set(new TokenSnapshot(T10, NOW, TokenSnapshot.Source.SEED)); // fresh

        heartbeat(r -> ProbeOutcome.empty(T10)).startupValidation();

        assertThat(probed).containsExactly(T10);
        assertThat(store.calls).containsExactly("saveHeartbeat"); // re-certification
    }

    @Test
    void startupValidation_incompatiblePipeline_surfacesFailure() {
        ref.set(new TokenSnapshot(T10, NOW, TokenSnapshot.Source.SEED));
        RuntimeException boom = new RuntimeException("unknown pipeline stage");

        heartbeat(r -> ProbeOutcome.failed(boom)).startupValidation();

        assertThat(metrics.probeFailures).containsExactly(boom);
        assertThat(store.calls).isEmpty();
    }

    // --- cancellation ---

    @Test
    void cancelDuringInFlightProbe_preventsAnyWrite() {
        ref.set(new TokenSnapshot(T10, IDLE_SINCE));
        CheckpointHeartbeat[] holder = new CheckpointHeartbeat[1];
        CheckpointHeartbeat hb = heartbeat(r -> {
            // Stream dies while the probe is in flight.
            holder[0].cancel();
            return ProbeOutcome.empty(T20);
        });
        holder[0] = hb;

        hb.idleTick();
        assertThat(store.calls)
                .as("a cancelled heartbeat must never stamp a dead stream")
                .isEmpty();

        // Fully cancelled: neither tick runs anymore.
        ref.set(new TokenSnapshot(T30, NOW));
        hb.flushTick();
        hb.idleTick();
        assertThat(store.calls).isEmpty();
    }

    // --- misc ---

    @Test
    void advancesComparator_ordersTokensByDataString() {
        assertThat(CheckpointHeartbeat.advances(T20, T10)).isTrue();
        assertThat(CheckpointHeartbeat.advances(T10, T20)).isFalse();
        assertThat(CheckpointHeartbeat.advances(T10, T10)).isFalse();
        assertThat(CheckpointHeartbeat.advances(T10, null)).isTrue();
    }

    @Test
    void storeThrow_isContained_andReportedAsCheckpointFailure() {
        ref.set(new TokenSnapshot(T10, NOW));
        store.throwOnWrite = true;

        heartbeat(r -> ProbeOutcome.eventPending()).flushTick();

        assertThat(metrics.checkpointFailures).hasSize(1);
    }

    private static Checkpoint checkpointWithSeen(BsonDocument seen) {
        return new Checkpoint(STREAM, null, seen, NOW, null, null, java.util.Map.of());
    }

    private static final class RecordingStore implements CheckpointStore {
        final List<String> calls = new ArrayList<>();
        Instant lastSeenTimestamp;
        Instant lastHeartbeatTimestamp;
        Checkpoint persisted;
        boolean throwOnWrite;

        @Override
        public void save(Checkpoint checkpoint) {
            calls.add("save");
        }

        @Override
        public Optional<Checkpoint> findByStreamName(String streamName) {
            return Optional.ofNullable(persisted);
        }

        @Override
        public void saveSeen(String streamName, BsonDocument token, Instant timestamp,
                             Instant heartbeatTimestamp) {
            if (throwOnWrite) {
                throw new RuntimeException("store down");
            }
            calls.add("saveSeen+hb:" + token.getString("_data").getValue());
            lastSeenTimestamp = timestamp;
            lastHeartbeatTimestamp = heartbeatTimestamp;
        }

        @Override
        public void saveHeartbeat(String streamName, Instant heartbeatTimestamp) {
            if (throwOnWrite) {
                throw new RuntimeException("store down");
            }
            calls.add("saveHeartbeat");
            lastHeartbeatTimestamp = heartbeatTimestamp;
        }

        @Override
        public void delete(String streamName) {
        }
    }

    private static final class RecordingMetrics implements StreamMetricsProvider {
        final List<Throwable> probeFailures = new ArrayList<>();
        final List<Throwable> checkpointFailures = new ArrayList<>();

        @Override
        public void onHeartbeatProbeFailed(String streamName, Throwable cause) {
            probeFailures.add(cause);
        }

        @Override
        public void onCheckpointFailed(String streamName, Throwable cause) {
            checkpointFailures.add(cause);
        }

        @Override
        public void onStreamStarted(String streamName, StreamConfiguration config) {
        }

        @Override
        public void onEventReceived(String streamName, ChangeEventMetadata metadata) {
        }

        @Override
        public void onEventProcessed(String streamName, long durationNanos, boolean success) {
        }

        @Override
        public void onEventError(String streamName, Throwable error, boolean willRetry,
                                 int attemptNumber, ChangeEventMetadata metadata) {
        }

        @Override
        public void onCheckpoint(String streamName, String resumeToken) {
        }

        @Override
        public void onBufferStatus(String streamName, int currentSize, int maxSize) {
        }

        @Override
        public void onBackpressure(String streamName, BackpressureAction action) {
        }

        @Override
        public void onEventSentToDlq(String streamName) {
        }

        @Override
        public void onOplogStats(double logLengthHours, String status) {
        }
    }
}
