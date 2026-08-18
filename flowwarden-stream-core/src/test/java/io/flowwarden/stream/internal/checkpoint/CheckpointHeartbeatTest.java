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
 * Unit tests for the split heartbeat responsibilities:
 *
 * <p>{@code flushTick} — dirty-only event-token coalescing, never probes,
 * never blocks (skips when this stream's probe holds the lock).
 * {@code idleTick} — probes past the idle threshold or while catching up,
 * with the three-outcome contract, the CAS against concurrent events, the
 * persist-then-publish ordering, the explicit catch-up state (no client-side
 * token ordering) and the cancellation flag.</p>
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
                true, IDLE_THRESHOLD, false);
    }

    private CheckpointHeartbeat catchUpHeartbeat(Function<BsonDocument, ProbeOutcome> probeFn) {
        return new CheckpointHeartbeat(STREAM, store, probeOf(probeFn), () -> ref,
                true, IDLE_THRESHOLD, true);
    }

    private CheckpointHeartbeat noFallbackHeartbeat(Function<BsonDocument, ProbeOutcome> probeFn) {
        return new CheckpointHeartbeat(STREAM, store, probeOf(probeFn), () -> ref,
                false, IDLE_THRESHOLD, false);
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

    // --- flushTick: dirty-only coalescing, never a probe, never blocking ---

    @Test
    void flush_dirtyEventToken_isPersistedWithHeartbeat_withoutProbing() {
        Instant eventTime = NOW.minusSeconds(3);
        ref.set(new TokenSnapshot(EVENT_A, eventTime));

        heartbeat(r -> ProbeOutcome.eventPending()).flushTick();

        assertThat(probed).isEmpty();
        assertThat(store.calls).containsExactly("saveSeen+hb:event-a");
        assertThat(store.lastSeenTimestamp).isEqualTo(eventTime);
        assertThat(store.lastHeartbeatTimestamp).isAfterOrEqualTo(eventTime);
    }

    @Test
    void flush_cleanTick_writesNothing() {
        ref.set(new TokenSnapshot(EVENT_A, NOW));
        CheckpointHeartbeat hb = heartbeat(r -> ProbeOutcome.eventPending());

        hb.flushTick(); // dirty → persisted
        hb.flushTick(); // clean → zero writes
        hb.flushTick();

        assertThat(store.calls).containsExactly("saveSeen+hb:event-a");
        assertThat(probed).isEmpty();
    }

    @Test
    void flush_seedSnapshot_isNeverPersistedAsDelivery() {
        ref.set(new TokenSnapshot(SEED, NOW, TokenSnapshot.Source.SEED));

        heartbeat(r -> ProbeOutcome.eventPending()).flushTick();

        assertThat(store.calls).as("a resume seed must never be flushed as seen").isEmpty();
        assertThat(probed).isEmpty();
    }

    @Test
    void flush_skipsWithoutBlocking_whileThisStreamsProbeIsInFlight() throws Exception {
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

        Thread probeThread = new Thread(hb::idleTick);
        probeThread.start();
        assertThat(probeStarted.await(5, TimeUnit.SECONDS)).isTrue();

        // The flush must return immediately without writing — never wait
        // behind the in-flight probe.
        long start = System.nanoTime();
        hb.flushTick();
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        assertThat(elapsedMillis).isLessThan(1_000);
        assertThat(store.calls).isEmpty();

        releaseProbe.countDown();
        probeThread.join(5_000);

        // The token stayed dirty: the next flush persists it.
        hb.flushTick();
        assertThat(store.calls).containsExactly("saveSeen+hb:event-a");
    }

    // --- idleTick: probes only past the idle threshold ---

    @Test
    void idle_beforeThreshold_neverProbes() {
        ref.set(new TokenSnapshot(EVENT_A, NOW)); // fresh activity

        heartbeat(r -> ProbeOutcome.empty(PBRT)).idleTick();

        assertThat(probed).isEmpty();
        assertThat(store.calls).isEmpty();
    }

    @Test
    void idle_pastThreshold_probeAdvancesPosition() {
        ref.set(new TokenSnapshot(EVENT_A, IDLE_SINCE));
        CheckpointHeartbeat hb = heartbeat(r -> ProbeOutcome.empty(PBRT));
        hb.flushTick(); // persist the event token first

        hb.idleTick();

        assertThat(probed).containsExactly(EVENT_A);
        assertThat(store.calls).containsExactly("saveSeen+hb:event-a", "saveSeen+hb:pbrt");
        // The probe result re-arms the idle delay (fresh SEED snapshot).
        assertThat(ref.get().token()).isEqualTo(PBRT);
        assertThat(ref.get().source()).isEqualTo(TokenSnapshot.Source.SEED);
    }

    @Test
    void idle_probeReturnsSamePosition_writesHeartbeatOnly() {
        ref.set(new TokenSnapshot(EVENT_A, IDLE_SINCE));

        heartbeat(r -> ProbeOutcome.empty(EVENT_A)).idleTick();

        assertThat(store.calls).containsExactly("saveHeartbeat");
        assertThat(ref.get().token()).isEqualTo(EVENT_A);
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
            // Simulate the main stream delivering an event mid-probe.
            ref.set(new TokenSnapshot(EVENT_B, Instant.now()));
            return ProbeOutcome.empty(PBRT);
        });
        hb.flushTick(); // persist EVENT_A first

        hb.idleTick();

        // The certified PBRT is persisted (safe interval), but the concurrent
        // event wins the in-memory race and stays dirty for the next flush.
        assertThat(store.calls).containsExactly("saveSeen+hb:event-a", "saveSeen+hb:pbrt");
        assertThat(ref.get().token()).isEqualTo(EVENT_B);

        hb.flushTick();
        assertThat(store.calls)
                .containsExactly("saveSeen+hb:event-a", "saveSeen+hb:pbrt", "saveSeen+hb:event-b");
    }

    @Test
    void idle_storeFailureOnProbeResult_keepsEventTokenDirty_flushRetries() {
        // Codex addendum: the CAS must not publish the SEED before the store
        // write succeeded, or a failed write silently loses the event token.
        ref.set(new TokenSnapshot(EVENT_A, IDLE_SINCE));
        CheckpointHeartbeat hb = heartbeat(r -> ProbeOutcome.empty(PBRT));

        store.throwOnWrite = true;
        hb.idleTick(); // probe EMPTY → saveSeen throws

        assertThat(ref.get().token())
                .as("a failed store write must not replace the event snapshot")
                .isEqualTo(EVENT_A);
        assertThat(metrics.checkpointFailures).hasSize(1);

        store.throwOnWrite = false;
        hb.flushTick(); // the event token is still dirty → retried and persisted

        assertThat(store.calls).containsExactly("saveSeen+hb:event-a");
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

    // --- catch-up state: resume behind the persisted position ---

    @Test
    void catchUp_replayedEvents_areNeitherPersistedNorHealthRefreshed() {
        store.persisted = checkpointWithSeen(PERSISTED_SEEN);
        ref.set(new TokenSnapshot(EVENT_A, NOW)); // replayed event, EVENT source

        catchUpHeartbeat(r -> ProbeOutcome.eventPending()).flushTick();

        assertThat(store.calls)
                .as("replayed events certify nothing about the persisted position")
                .isEmpty();
    }

    @Test
    void catchUp_probeBypassesIdleness_andEmptyEndsCatchUp() {
        // Continuous replay: the snapshot is FRESH, yet the catch-up probe
        // must still run.
        ref.set(new TokenSnapshot(EVENT_A, NOW));
        CheckpointHeartbeat hb = catchUpHeartbeat(r -> ProbeOutcome.empty(PBRT));

        hb.idleTick();

        assertThat(probed).containsExactly(EVENT_A);
        assertThat(store.calls).containsExactly("saveSeen+hb:pbrt");
        assertThat(hb.isCatchingUp()).isFalse();

        // Normal semantics resumed: delivered events flush again.
        ref.set(new TokenSnapshot(EVENT_B, NOW));
        hb.flushTick();
        assertThat(store.calls).containsExactly("saveSeen+hb:pbrt", "saveSeen+hb:event-b");
    }

    @Test
    void catchUp_probeEventPending_staysCatchingUp() {
        ref.set(new TokenSnapshot(EVENT_A, NOW));
        CheckpointHeartbeat hb = catchUpHeartbeat(r -> ProbeOutcome.eventPending());

        hb.idleTick();

        assertThat(hb.isCatchingUp()).isTrue();
        assertThat(store.calls).isEmpty();
    }

    @Test
    void catchUp_probeReturnsChainPosition_persistsItAndEndsCatchUp() {
        // Backlog consumed and the cluster position did not move: the
        // delivered position itself is the certified safe position.
        ref.set(new TokenSnapshot(EVENT_A, NOW));
        CheckpointHeartbeat hb = catchUpHeartbeat(r -> ProbeOutcome.empty(EVENT_A));

        hb.idleTick();

        assertThat(store.calls).containsExactly("saveSeen+hb:event-a");
        assertThat(hb.isCatchingUp()).isFalse();
    }

    @Test
    void catchUp_storeFailure_keepsCatchingUp_untilAWriteSucceeds() {
        ref.set(new TokenSnapshot(EVENT_A, NOW));
        CheckpointHeartbeat hb = catchUpHeartbeat(r -> ProbeOutcome.empty(PBRT));

        store.throwOnWrite = true;
        hb.idleTick();
        assertThat(hb.isCatchingUp()).isTrue();

        store.throwOnWrite = false;
        hb.idleTick();
        assertThat(hb.isCatchingUp()).isFalse();
        assertThat(store.calls).containsExactly("saveSeen+hb:pbrt");
    }

    // --- startup probe ---

    @Test
    void startupProbe_probesRegardlessOfIdleness() {
        ref.set(new TokenSnapshot(SEED, NOW, TokenSnapshot.Source.SEED)); // fresh

        heartbeat(r -> ProbeOutcome.empty(SEED)).startupProbe();

        assertThat(probed).containsExactly(SEED);
        assertThat(store.calls).containsExactly("saveHeartbeat"); // re-certification
    }

    @Test
    void startupProbe_incompatiblePipeline_surfacesFailure() {
        ref.set(new TokenSnapshot(SEED, NOW, TokenSnapshot.Source.SEED));
        RuntimeException boom = new RuntimeException("unknown pipeline stage");

        heartbeat(r -> ProbeOutcome.failed(boom)).startupProbe();

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
        ref.set(new TokenSnapshot(EVENT_A, NOW));
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
