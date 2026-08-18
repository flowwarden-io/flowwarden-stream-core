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

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the write contract of {@link CheckpointHeartbeat}: fresh
 * EVENT token → position + heartbeat; empty probe with a new PBRT → position +
 * heartbeat (CAS-guarded); empty probe with the same PBRT → heartbeat only;
 * abstention or failure → no write. Plus the correctness guards: SEED
 * snapshots are never persisted as deliveries, seen never regresses below the
 * persisted high-water mark, and a cancelled heartbeat never writes.
 */
class CheckpointHeartbeatTest {

    private static final String STREAM = "hb-test";
    // _data values are ordered: T10 < T20 < T30 (lexicographic, like real tokens)
    private static final BsonDocument T10 = BsonDocument.parse("{\"_data\": \"10\"}");
    private static final BsonDocument T20 = BsonDocument.parse("{\"_data\": \"20\"}");
    private static final BsonDocument T30 = BsonDocument.parse("{\"_data\": \"30\"}");

    private RecordingStore store;
    private RecordingMetrics metrics;
    private AtomicReference<TokenSnapshot> ref;

    @BeforeEach
    void setUp() {
        store = new RecordingStore();
        metrics = new RecordingMetrics();
        ref = new AtomicReference<>();
        FlowWardenMetrics.setProvider(metrics);
    }

    @AfterEach
    void tearDown() {
        FlowWardenMetrics.setProvider(StreamMetricsProvider.noOp());
    }

    private CheckpointHeartbeat heartbeat(HeartbeatProbe probe) {
        return new CheckpointHeartbeat(STREAM, store, probe, () -> ref, true);
    }

    private CheckpointHeartbeat latestModeHeartbeat(HeartbeatProbe probe) {
        return new CheckpointHeartbeat(STREAM, store, probe, () -> ref, false);
    }

    private static HeartbeatProbe probeOf(Function<BsonDocument, ProbeOutcome> fn) {
        return new HeartbeatProbe() {
            @Override
            public ProbeOutcome probe(BsonDocument resumeAfter) {
                return fn.apply(resumeAfter);
            }

            @Override
            public BsonDocument initialPosition() {
                throw new UnsupportedOperationException("not used by tick tests");
            }
        };
    }

    // --- three-outcome write contract ---

    @Test
    void freshEventToken_isPersistedWithHeartbeat_withoutProbing() {
        Instant eventTime = Instant.now().minusSeconds(3);
        ref.set(new TokenSnapshot(T10, eventTime));
        List<BsonDocument> probed = new ArrayList<>();

        heartbeat(probeOf(resumeAfter -> {
            probed.add(resumeAfter);
            return ProbeOutcome.eventPending();
        })).tick();

        assertThat(probed).isEmpty();
        assertThat(store.calls).containsExactly("saveSeen+hb:10");
        assertThat(store.lastSeenTimestamp).isEqualTo(eventTime);
        assertThat(store.lastHeartbeatTimestamp).isAfterOrEqualTo(eventTime);
    }

    @Test
    void unchangedToken_probeReturnsNewPbrt_advancesPositionAtomically() {
        ref.set(new TokenSnapshot(T10, Instant.now()));
        CheckpointHeartbeat hb = heartbeat(probeOf(resumeAfter -> ProbeOutcome.empty(T20)));

        hb.tick(); // persists the seed event token
        hb.tick(); // unchanged → probe → new PBRT

        assertThat(store.calls).containsExactly("saveSeen+hb:10", "saveSeen+hb:20");
        assertThat(ref.get().token()).isEqualTo(T20);
    }

    @Test
    void unchangedToken_probeReturnsSamePbrt_writesHeartbeatOnly() {
        ref.set(new TokenSnapshot(T10, Instant.now()));
        CheckpointHeartbeat hb = heartbeat(probeOf(resumeAfter -> ProbeOutcome.empty(T10)));

        hb.tick(); // persists the seed event token
        hb.tick(); // unchanged → probe → same PBRT → re-certification

        assertThat(store.calls).containsExactly("saveSeen+hb:10", "saveHeartbeat");
        // Position is byte-identical: no seen write happened on the second tick.
        assertThat(ref.get().token()).isEqualTo(T10);
    }

    @Test
    void probeEventPending_abstains_noWrites() {
        ref.set(new TokenSnapshot(T10, Instant.now()));
        CheckpointHeartbeat hb = heartbeat(probeOf(resumeAfter -> ProbeOutcome.eventPending()));

        hb.tick(); // persists the seed event token
        hb.tick(); // unchanged → probe → event pending → abstain

        assertThat(store.calls).containsExactly("saveSeen+hb:10");
    }

    @Test
    void probeFailure_noWrites_emitsDedicatedSignal() {
        ref.set(new TokenSnapshot(T10, Instant.now()));
        RuntimeException boom = new RuntimeException("history lost");
        CheckpointHeartbeat hb = heartbeat(probeOf(resumeAfter -> ProbeOutcome.failed(boom)));

        hb.tick(); // persists the seed event token
        hb.tick(); // unchanged → probe → failure

        assertThat(store.calls).containsExactly("saveSeen+hb:10");
        assertThat(metrics.probeFailures).containsExactly(boom);
    }

    @Test
    void eventArrivingDuringProbe_winsTheCas_probeResultDiscarded() {
        ref.set(new TokenSnapshot(T10, Instant.now()));
        CheckpointHeartbeat hb = heartbeat(probeOf(resumeAfter -> {
            // Simulate the main stream delivering an event mid-probe.
            ref.set(new TokenSnapshot(T20, Instant.now()));
            return ProbeOutcome.empty(T30);
        }));

        hb.tick(); // persists the seed event token
        hb.tick(); // unchanged → probe → CAS loses against T20

        assertThat(store.calls).containsExactly("saveSeen+hb:10");
        assertThat(ref.get().token()).isEqualTo(T20);

        // Next tick: the event token that won the race is persisted normally.
        hb.tick();
        assertThat(store.calls).containsExactly("saveSeen+hb:10", "saveSeen+hb:20");
    }

    // --- chaining sources ---

    @Test
    void noInMemoryToken_resumeMode_chainsFromPersistedSeen() {
        store.persisted = new Checkpoint(STREAM, null, T10, Instant.now(),
                null, null, java.util.Map.of());
        List<BsonDocument> probed = new ArrayList<>();

        heartbeat(probeOf(resumeAfter -> {
            probed.add(resumeAfter);
            return ProbeOutcome.empty(T20);
        })).tick();

        assertThat(probed).containsExactly(T10);
        assertThat(store.calls).containsExactly("saveSeen+hb:20");
    }

    @Test
    void noInMemoryToken_latestMode_neverFallsBackToPersistedCheckpoint() {
        // LATEST semantics ignore persisted tokens: chaining from one would
        // strand the probe on history the main stream will never consume.
        store.persisted = new Checkpoint(STREAM, null, T10, Instant.now(),
                null, null, java.util.Map.of());
        List<BsonDocument> probed = new ArrayList<>();

        latestModeHeartbeat(probeOf(resumeAfter -> {
            probed.add(resumeAfter);
            return ProbeOutcome.empty(T20);
        })).tick();

        assertThat(probed).isEmpty();
        assertThat(store.calls).isEmpty();
    }

    @Test
    void noPositionAnywhere_neverProbes() {
        List<BsonDocument> probed = new ArrayList<>();

        heartbeat(probeOf(resumeAfter -> {
            probed.add(resumeAfter);
            return ProbeOutcome.empty(T20);
        })).tick();

        assertThat(probed).isEmpty();
        assertThat(store.calls).isEmpty();
    }

    // --- seeds are not events, seen never regresses ---

    @Test
    void seedSnapshot_isNeverPersistedAsDelivery_probeChainsFromIt() {
        // PROCESSED_FIRST resume: the seed is the OLD processed token (T10)
        // while the persisted seen is fresher (T20). The first tick must not
        // overwrite T20 with T10.
        store.persisted = new Checkpoint(STREAM, null, T20, Instant.now(),
                T10, Instant.now(), java.util.Map.of());
        ref.set(new TokenSnapshot(T10, Instant.now(), TokenSnapshot.Source.SEED));
        List<BsonDocument> probed = new ArrayList<>();
        CheckpointHeartbeat hb = heartbeat(probeOf(resumeAfter -> {
            probed.add(resumeAfter);
            return ProbeOutcome.eventPending(); // replay in progress
        }));

        hb.tick();

        assertThat(store.calls).as("a resume seed must never be written as seen").isEmpty();
        assertThat(probed).as("the probe chains from the actually-consumed position")
                .containsExactly(T10);
    }

    @Test
    void replayedEventBelowHighWaterMark_doesNotRegressSeen() {
        // Replay after a PROCESSED_FIRST resume: the delivered event's token
        // (T10) is older than the persisted seen (T20).
        store.persisted = new Checkpoint(STREAM, null, T20, Instant.now(),
                null, null, java.util.Map.of());
        ref.set(new TokenSnapshot(T10, Instant.now())); // EVENT source

        CheckpointHeartbeat hb = heartbeat(probeOf(resumeAfter -> ProbeOutcome.eventPending()));
        hb.tick();

        assertThat(store.calls)
                .as("a replayed event below the high-water mark is heartbeat-only")
                .containsExactly("saveHeartbeat");

        // Once the stream catches up past the mark, seen advances again.
        ref.set(new TokenSnapshot(T30, Instant.now()));
        hb.tick();
        assertThat(store.calls).containsExactly("saveHeartbeat", "saveSeen+hb:30");
    }

    @Test
    void probePbrtBelowHighWaterMark_isDowngradedToHeartbeatOnly() {
        // Bounded-scan intermediate PBRT (T10→T20 probe result below the
        // persisted mark T30, e.g. probe chained from an old processed seed).
        store.persisted = new Checkpoint(STREAM, null, T30, Instant.now(),
                null, null, java.util.Map.of());
        ref.set(new TokenSnapshot(T10, Instant.now(), TokenSnapshot.Source.SEED));

        heartbeat(probeOf(resumeAfter -> ProbeOutcome.empty(T20))).tick();

        assertThat(store.calls).containsExactly("saveHeartbeat");
    }

    @Test
    void advancesComparator_ordersTokensByDataString() {
        assertThat(CheckpointHeartbeat.advances(T20, T10)).isTrue();
        assertThat(CheckpointHeartbeat.advances(T10, T20)).isFalse();
        assertThat(CheckpointHeartbeat.advances(T10, T10)).isFalse();
        assertThat(CheckpointHeartbeat.advances(T10, null)).isTrue();
    }

    // --- cancellation ---

    @Test
    void cancelDuringInFlightProbe_preventsAnyWrite() {
        ref.set(new TokenSnapshot(T10, Instant.now()));
        CheckpointHeartbeat[] holder = new CheckpointHeartbeat[1];
        CheckpointHeartbeat hb = heartbeat(probeOf(resumeAfter -> {
            // Stream dies while the probe is in flight.
            holder[0].cancel();
            return ProbeOutcome.empty(T20);
        }));
        holder[0] = hb;

        hb.tick(); // persists the seed event token
        hb.tick(); // probe → cancelled mid-flight → no write

        assertThat(store.calls)
                .as("a cancelled heartbeat must never stamp a dead stream")
                .containsExactly("saveSeen+hb:10");

        hb.tick(); // fully cancelled: not even the event path runs
        assertThat(store.calls).containsExactly("saveSeen+hb:10");
    }

    @Test
    void storeThrow_isContained_andReportedAsCheckpointFailure() {
        ref.set(new TokenSnapshot(T10, Instant.now()));
        store.throwOnWrite = true;

        heartbeat(probeOf(resumeAfter -> ProbeOutcome.eventPending())).tick();

        assertThat(metrics.checkpointFailures).hasSize(1);
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
