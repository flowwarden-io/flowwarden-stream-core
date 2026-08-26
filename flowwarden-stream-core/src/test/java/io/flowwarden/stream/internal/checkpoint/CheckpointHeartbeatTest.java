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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the split heartbeat responsibilities — one per checkpoint
 * anchor:
 *
 * <p>{@code flushTick} — the time threshold of the processed-anchor policy:
 * dirty-only coalescing of the latest settled event token, never probes,
 * never blocks (skips when this stream's probe holds the lock).
 * {@code idleTick} — the sole writer of {@code lastSeenToken}: probes past
 * the idle threshold and persists exclusively server-certified PBRTs, with
 * the three-outcome contract, the CAS against concurrent events, the
 * persist-then-publish ordering and the cancellation flag.</p>
 */
class CheckpointHeartbeatTest {

    private static final String STREAM = "hb-test";
    private static final Duration IDLE_THRESHOLD = Duration.ofSeconds(60);
    private static final Instant NOW = Instant.now();
    private static final Instant IDLE_SINCE = NOW.minusSeconds(120);
    private static final BsonDocument SEED = BsonDocument.parse("{\"_data\": \"seed\"}");
    private static final BsonDocument EVENT_A = BsonDocument.parse("{\"_data\": \"event-a\"}");
    private static final BsonDocument EVENT_B = BsonDocument.parse("{\"_data\": \"event-b\"}");
    private static final BsonDocument PBRT = BsonDocument.parse("{\"_data\": \"pbrt\"}");
    private static final BsonDocument PERSISTED_SEEN = BsonDocument.parse("{\"_data\": \"persisted\"}");
    private static final BsonDocument DEAD_PROCESSED = BsonDocument.parse("{\"_data\": \"dead-processed\"}");

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

    private final List<org.bson.BsonTimestamp> opTimeProbes = new ArrayList<>();
    private ProcessedAnchorPolicy policy;

    private CheckpointHeartbeat heartbeat(Function<BsonDocument, ProbeOutcome> probeFn) {
        // Count threshold out of reach: flushTick is the only anchor writer.
        policy = new ProcessedAnchorPolicy(STREAM, store, 100);
        return new CheckpointHeartbeat(STREAM, store, probeOf(probeFn), () -> ref,
                true, IDLE_THRESHOLD, null, null, policy);
    }

    private CheckpointHeartbeat noFallbackHeartbeat(Function<BsonDocument, ProbeOutcome> probeFn) {
        policy = new ProcessedAnchorPolicy(STREAM, store, 100);
        return new CheckpointHeartbeat(STREAM, store, probeOf(probeFn), () -> ref,
                false, IDLE_THRESHOLD, null, null, policy);
    }

    private CheckpointHeartbeat opTimeHeartbeat(org.bson.BsonTimestamp opTime,
                                                Function<BsonDocument, ProbeOutcome> probeFn) {
        // RESUME_FROM_OPLOG_START recovery: no fallback, an operation time as
        // last-resort chain source, the dead processed token as the
        // deferred-cleanup guard.
        policy = new ProcessedAnchorPolicy(STREAM, store, 100);
        return new CheckpointHeartbeat(STREAM, store, probeOf(probeFn), () -> ref,
                false, IDLE_THRESHOLD, opTime, DEAD_PROCESSED, policy);
    }

    private HeartbeatProbe probeOf(Function<BsonDocument, ProbeOutcome> fn) {
        return new HeartbeatProbe() {
            @Override
            public ProbeOutcome probe(BsonDocument resumeAfter) {
                probed.add(resumeAfter);
                return fn.apply(resumeAfter);
            }

            @Override
            public ProbeOutcome probeFromOperationTime(org.bson.BsonTimestamp operationTime) {
                opTimeProbes.add(operationTime);
                return fn.apply(null);
            }

            @Override
            public BsonDocument initialPosition() {
                throw new UnsupportedOperationException("not used by tick tests");
            }
        };
    }

    // --- flushTick: the processed-anchor time threshold, dirty-only ---
    // (The policy's own semantics — serialization, counter reset, manual
    // save — are covered in ProcessedAnchorPolicyTest; here only the
    // heartbeat's delegation and its independence from the probe lock.)

    @Test
    void flush_dirtySettledToken_isPersistedAsProcessedWithHeartbeat_withoutProbing() {
        Instant eventTime = NOW.minusSeconds(3);
        CheckpointHeartbeat hb = heartbeat(r -> ProbeOutcome.eventPending());
        policy.onSettled(EVENT_A, eventTime);

        hb.flushTick();

        assertThat(probed).isEmpty();
        assertThat(store.calls).containsExactly("saveProcessed+hb:event-a");
        assertThat(store.lastProcessedTimestamp).isEqualTo(eventTime);
        assertThat(store.lastHeartbeatTimestamp).isAfterOrEqualTo(eventTime);
    }

    @Test
    void flush_cleanTick_writesNothing() {
        CheckpointHeartbeat hb = heartbeat(r -> ProbeOutcome.eventPending());
        policy.onSettled(EVENT_A, NOW);

        hb.flushTick(); // dirty → persisted
        hb.flushTick(); // clean → zero writes
        hb.flushTick();

        assertThat(store.calls).containsExactly("saveProcessed+hb:event-a");
        assertThat(probed).isEmpty();
    }

    @Test
    void flush_neverWritesTheSeenPosition() {
        // The structural #74 invariant: settled event tokens are processed
        // anchors, never seen positions — a resume that replays old events
        // cannot regress the certified position because delivered tokens are
        // no longer candidates for it.
        CheckpointHeartbeat hb = heartbeat(r -> ProbeOutcome.eventPending());
        policy.onSettled(EVENT_A, NOW);

        hb.flushTick();

        assertThat(store.calls).containsExactly("saveProcessed+hb:event-a");
        assertThat(store.seenWrites).as("only the probe writes lastSeenToken").isZero();
    }

    @Test
    void flush_proceedsIndependently_whileThisStreamsProbeIsInFlight() throws Exception {
        // The flush rides the policy's own lock, not the heartbeat lock: a
        // slow in-flight probe (which holds the heartbeat lock for its whole
        // network round-trip) must neither block nor starve the anchor.
        ref.set(new TokenSnapshot(EVENT_A, IDLE_SINCE));
        CountDownLatch probeStarted = new CountDownLatch(1);
        CountDownLatch releaseProbe = new CountDownLatch(1);
        CheckpointHeartbeat hb = heartbeat(r -> {
            probeStarted.countDown();
            try {
                releaseProbe.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return ProbeOutcome.eventPending();
        });
        policy.onSettled(EVENT_A, IDLE_SINCE);

        Thread probeThread = new Thread(hb::idleTick);
        probeThread.start();
        assertThat(probeStarted.await(5, TimeUnit.SECONDS)).isTrue();

        long start = System.nanoTime();
        hb.flushTick();
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        assertThat(elapsedMillis).isLessThan(1_000);
        assertThat(store.calls)
                .as("the anchor is persisted even while the probe holds its lock")
                .containsExactly("saveProcessed+hb:event-a");

        releaseProbe.countDown();
        probeThread.join(5_000);
    }

    // --- idleTick: probes only past the idle threshold, PBRT-only writes ---

    @Test
    void idle_beforeThreshold_neverProbes() {
        ref.set(new TokenSnapshot(EVENT_A, NOW)); // fresh activity

        heartbeat(r -> ProbeOutcome.empty(PBRT)).idleTick();

        assertThat(probed).isEmpty();
        assertThat(store.calls).isEmpty();
    }

    @Test
    void idle_pastThreshold_probeAdvancesTheCertifiedPosition() {
        ref.set(new TokenSnapshot(EVENT_A, IDLE_SINCE));
        CheckpointHeartbeat hb = heartbeat(r -> ProbeOutcome.empty(PBRT));
        policy.onSettled(EVENT_A, IDLE_SINCE);
        hb.flushTick(); // the settled token anchors processed first

        hb.idleTick();

        assertThat(probed).containsExactly(EVENT_A);
        assertThat(store.calls).containsExactly("saveProcessed+hb:event-a", "saveSeen+hb:pbrt");
        // The probe result re-arms the idle delay (fresh SEED snapshot).
        assertThat(ref.get().token()).isEqualTo(PBRT);
        assertThat(ref.get().source()).isEqualTo(TokenSnapshot.Source.SEED);
    }

    @Test
    void idle_probeReturnsSamePosition_installsOnce_thenHeartbeatOnly() {
        // The first certification of a run always installs the certified
        // position durably — the stored seen may be stale (even a
        // pathological leftover from an earlier version), and the chain
        // token is a processed anchor, not the stored seen. Only once the
        // position is known installed does re-certification degrade to a
        // heartbeat-only write.
        ref.set(new TokenSnapshot(EVENT_A, IDLE_SINCE));
        CheckpointHeartbeat hb = heartbeat(r -> ProbeOutcome.empty(EVENT_A));

        hb.idleTick();
        assertThat(store.calls).containsExactly("saveSeen+hb:event-a");
        assertThat(ref.get().token()).isEqualTo(EVENT_A);

        ref.set(new TokenSnapshot(EVENT_A, IDLE_SINCE));
        hb.probeNow(); // bypasses the idle throttle for the second pass
        assertThat(store.calls).containsExactly("saveSeen+hb:event-a", "saveHeartbeat");
    }

    @Test
    void idle_sustainedIdleness_probesStaySpacedAFullInterval() {
        // The idle check fires on a short cadence (to keep the threshold a
        // bound), but abstentions/failures must not retry at check cadence.
        ref.set(new TokenSnapshot(EVENT_A, IDLE_SINCE));
        CheckpointHeartbeat hb = heartbeat(r -> ProbeOutcome.eventPending());

        hb.idleTick(); // idle → probe (abstains)
        hb.idleTick(); // immediately after: throttled, no second probe
        hb.idleTick();

        assertThat(probed).hasSize(1);
    }

    @Test
    void resumeContext_fallbackRequiresAnEstablishedPosition() {
        assertThat(ResumeContext.NONE.allowPersistedFallback()).isFalse();
        assertThat(new ResumeContext(EVENT_A).allowPersistedFallback()).isTrue();
    }

    @Test
    void idle_probeEventPending_abstains_noWrites() {
        ref.set(new TokenSnapshot(EVENT_A, IDLE_SINCE));

        heartbeat(r -> ProbeOutcome.eventPending()).idleTick();

        assertThat(store.calls).isEmpty();
    }

    @Test
    void idle_probeFailure_noWrites_emitsDedicatedSignal() {
        ref.set(new TokenSnapshot(EVENT_A, IDLE_SINCE));
        RuntimeException boom = new RuntimeException("history lost");

        heartbeat(r -> ProbeOutcome.failed(boom)).idleTick();

        assertThat(store.calls).isEmpty();
        assertThat(metrics.probeFailures).containsExactly(boom);
    }

    @Test
    void idle_eventArrivingDuringProbe_staysInMemory_probeResultStillPersisted() {
        ref.set(new TokenSnapshot(EVENT_A, IDLE_SINCE));
        CheckpointHeartbeat hb = heartbeat(r -> {
            // Simulate the main stream settling an event mid-probe.
            ref.set(new TokenSnapshot(EVENT_B, Instant.now()));
            policy.onSettled(EVENT_B, NOW);
            return ProbeOutcome.empty(PBRT);
        });
        policy.onSettled(EVENT_A, IDLE_SINCE);
        hb.flushTick(); // anchor EVENT_A first

        hb.idleTick();

        // The certified PBRT is persisted (safe interval), but the concurrent
        // event wins the in-memory race and stays dirty for the next flush.
        assertThat(store.calls).containsExactly("saveProcessed+hb:event-a", "saveSeen+hb:pbrt");
        assertThat(ref.get().token()).isEqualTo(EVENT_B);

        hb.flushTick();
        assertThat(store.calls).containsExactly(
                "saveProcessed+hb:event-a", "saveSeen+hb:pbrt", "saveProcessed+hb:event-b");
    }

    @Test
    void idle_storeFailureOnProbeResult_keepsEventTokenDirty_flushRetries() {
        // The CAS must not publish the SEED before the store write succeeded,
        // or a failed write silently loses the event token.
        ref.set(new TokenSnapshot(EVENT_A, IDLE_SINCE));
        CheckpointHeartbeat hb = heartbeat(r -> ProbeOutcome.empty(PBRT));
        policy.onSettled(EVENT_A, IDLE_SINCE);

        store.throwOnWrite = true;
        hb.idleTick(); // probe EMPTY → saveSeen throws

        assertThat(ref.get().token())
                .as("a failed store write must not replace the event snapshot")
                .isEqualTo(EVENT_A);
        assertThat(metrics.checkpointFailures).hasSize(1);

        store.throwOnWrite = false;
        hb.flushTick(); // the settled token is still dirty → anchored now

        assertThat(store.calls).containsExactly("saveProcessed+hb:event-a");
    }

    @Test
    void idle_noInMemoryToken_withEstablishedPosition_chainsFromPersistedSeen() {
        store.persisted = checkpointWithSeen(PERSISTED_SEEN);

        heartbeat(r -> ProbeOutcome.empty(PBRT)).idleTick();

        assertThat(probed).containsExactly(PERSISTED_SEEN);
        assertThat(store.calls).containsExactly("saveSeen+hb:pbrt");
    }

    @Test
    void idle_noEstablishedPosition_neverFallsBackToPersistedCheckpoint() {
        // Cascade produced no position (LATEST, or RESUME_FROM_OPLOG_START
        // recovery): the checkpoint is ignored or known-invalid.
        store.persisted = checkpointWithSeen(PERSISTED_SEEN);

        noFallbackHeartbeat(r -> ProbeOutcome.empty(PBRT)).idleTick();

        assertThat(probed).isEmpty();
        assertThat(store.calls).isEmpty();
    }

    @Test
    void idle_noPositionAnywhere_neverProbes() {
        heartbeat(r -> ProbeOutcome.empty(PBRT)).idleTick();

        assertThat(probed).isEmpty();
        assertThat(store.calls).isEmpty();
    }

    // --- OPLOG_START recovery: operation time as last-resort chain source ---

    @Test
    void opTimeRecovery_noPositionAnywhere_probesFromOperationTime_neverFromNow() {
        org.bson.BsonTimestamp opTime = new org.bson.BsonTimestamp(1234, 1);
        CheckpointHeartbeat hb = opTimeHeartbeat(opTime, r -> ProbeOutcome.empty(PBRT));
        assertThat(hb.needsEstablishment()).isTrue();

        hb.probeNow();

        assertThat(opTimeProbes).containsExactly(opTime);
        assertThat(probed).as("never a token probe, never 'now'").isEmpty();
        // Deferred cleanup: the establishment write replaces the dead tokens,
        // guarded on the dead processed value read at detection.
        assertThat(store.calls).containsExactly("reset:pbrt/guard:dead-processed");
        assertThat(hb.needsEstablishment())
                .as("a DURABLE position ends the establishment chain")
                .isFalse();
    }

    @Test
    void opTimeRecovery_deliveredTokenIsOnlyAChainSource_notADurableWrite() {
        // A delivered token must NOT end the chain — only a successful
        // durable write does. Filtered events produce tokens without any
        // guarantee of a processed write, and the flush may be disabled.
        org.bson.BsonTimestamp opTime = new org.bson.BsonTimestamp(1234, 1);
        java.util.concurrent.atomic.AtomicReference<ProbeOutcome> outcome =
                new java.util.concurrent.atomic.AtomicReference<>(ProbeOutcome.eventPending());
        CheckpointHeartbeat hb = opTimeHeartbeat(opTime, r -> outcome.get());

        hb.probeNow(); // replay flowing → abstain
        assertThat(store.calls).isEmpty();
        assertThat(hb.needsEstablishment()).isTrue();

        // The main cursor delivers a (possibly filtered) event: still no
        // durable write — the chain must keep going.
        ref.set(new TokenSnapshot(EVENT_A, NOW));
        assertThat(hb.needsEstablishment())
                .as("a delivered token is only a chain source, not a durable write")
                .isTrue();

        // Next chain probe chains from the delivered token and certifies.
        outcome.set(ProbeOutcome.empty(PBRT));
        hb.probeNow();
        assertThat(probed).contains(EVENT_A);
        assertThat(opTimeProbes).hasSize(1); // op-time source not reused
        assertThat(store.calls).containsExactly("reset:pbrt/guard:dead-processed");
        assertThat(hb.needsEstablishment()).isFalse();
    }

    @Test
    void opTimeRecovery_samePbrtAsChainToken_stillGoesThroughTheReset() {
        // Fast-path guard: during a pending reset, even a PBRT identical to
        // the chained token must NOT degrade to a heartbeat-only write — the
        // deferred cleanup would never run and the chain would never end.
        org.bson.BsonTimestamp opTime = new org.bson.BsonTimestamp(1234, 1);
        CheckpointHeartbeat hb = opTimeHeartbeat(opTime, r -> ProbeOutcome.empty(EVENT_A));
        ref.set(new TokenSnapshot(EVENT_A, NOW)); // delivered during replay

        hb.probeNow();

        assertThat(store.calls).containsExactly("reset:event-a/guard:dead-processed");
        assertThat(hb.needsEstablishment()).isFalse();
    }

    // --- startup probe ---

    @Test
    void probeNow_probesRegardlessOfIdleness() {
        ref.set(new TokenSnapshot(SEED, NOW, TokenSnapshot.Source.SEED)); // fresh
        CheckpointHeartbeat hb = heartbeat(r -> ProbeOutcome.empty(SEED));

        hb.probeNow();
        assertThat(probed).containsExactly(SEED);
        assertThat(store.calls).containsExactly("saveSeen+hb:seed"); // first install

        hb.probeNow();
        assertThat(store.calls)
                .containsExactly("saveSeen+hb:seed", "saveHeartbeat"); // re-certification
    }

    @Test
    void probeNow_incompatiblePipeline_surfacesFailure() {
        ref.set(new TokenSnapshot(SEED, NOW, TokenSnapshot.Source.SEED));
        RuntimeException boom = new RuntimeException("unknown pipeline stage");

        heartbeat(r -> ProbeOutcome.failed(boom)).probeNow();

        assertThat(metrics.probeFailures).containsExactly(boom);
        assertThat(store.calls).isEmpty();
    }

    // --- cancellation ---

    @Test
    void cancelDuringInFlightProbe_preventsAnyWrite() {
        ref.set(new TokenSnapshot(EVENT_A, IDLE_SINCE));
        CheckpointHeartbeat[] holder = new CheckpointHeartbeat[1];
        CheckpointHeartbeat hb = heartbeat(r -> {
            // Stream dies while the probe is in flight.
            holder[0].cancel();
            return ProbeOutcome.empty(PBRT);
        });
        holder[0] = hb;

        hb.idleTick();
        assertThat(store.calls)
                .as("a cancelled heartbeat must never stamp a dead stream")
                .isEmpty();

        // Fully cancelled: neither tick runs anymore.
        ref.set(new TokenSnapshot(EVENT_B, NOW));
        hb.flushTick();
        hb.idleTick();
        assertThat(store.calls).isEmpty();
    }

    @Test
    void storeThrow_isContained_andReportedAsCheckpointFailure() {
        CheckpointHeartbeat hb = heartbeat(r -> ProbeOutcome.eventPending());
        policy.onSettled(EVENT_A, NOW);
        store.throwOnWrite = true;

        hb.flushTick();

        assertThat(metrics.checkpointFailures).hasSize(1);
    }

    private static Checkpoint checkpointWithSeen(BsonDocument seen) {
        return new Checkpoint(STREAM, null, seen, NOW, null, null, java.util.Map.of());
    }

    private static final class RecordingStore implements CheckpointStore {
        final List<String> calls = new ArrayList<>();
        Instant lastProcessedTimestamp;
        Instant lastHeartbeatTimestamp;
        Checkpoint persisted;
        boolean throwOnWrite;
        int seenWrites;

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
            seenWrites++;
            calls.add("saveSeen+hb:" + token.getString("_data").getValue());
            lastHeartbeatTimestamp = heartbeatTimestamp;
        }

        @Override
        public void saveProcessed(String streamName, BsonDocument token, Instant timestamp,
                                  Instant heartbeatTimestamp) {
            if (throwOnWrite) {
                throw new RuntimeException("store down");
            }
            calls.add("saveProcessed+hb:" + token.getString("_data").getValue());
            lastProcessedTimestamp = timestamp;
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
        public void resetAfterHistoryLost(String streamName, BsonDocument freshSeenToken,
                                          BsonDocument expectedDeadProcessed, Instant timestamp) {
            if (throwOnWrite) {
                throw new RuntimeException("store down");
            }
            calls.add("reset:" + (freshSeenToken != null
                    ? freshSeenToken.getString("_data").getValue() : "null")
                    + "/guard:" + (expectedDeadProcessed != null
                    ? expectedDeadProcessed.getString("_data").getValue() : "null"));
            lastHeartbeatTimestamp = freshSeenToken != null ? timestamp : null;
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
