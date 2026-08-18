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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the three-outcome write contract of {@link CheckpointHeartbeat}:
 * fresh event token → position + heartbeat; empty probe with a new PBRT →
 * position + heartbeat (CAS-guarded); empty probe with the same PBRT →
 * heartbeat only; abstention or failure → no write at all.
 */
class CheckpointHeartbeatTest {

    private static final String STREAM = "hb-test";
    private static final BsonDocument TOKEN_A = BsonDocument.parse("{\"_data\": \"A\"}");
    private static final BsonDocument TOKEN_B = BsonDocument.parse("{\"_data\": \"B\"}");
    private static final BsonDocument PBRT = BsonDocument.parse("{\"_data\": \"PBRT\"}");

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
        return new CheckpointHeartbeat(STREAM, store, probe, () -> ref);
    }

    @Test
    void freshEventToken_isPersistedWithHeartbeat_withoutProbing() {
        Instant eventTime = Instant.now().minusSeconds(3);
        ref.set(new TokenSnapshot(TOKEN_A, eventTime));
        List<BsonDocument> probed = new ArrayList<>();

        heartbeat(resumeAfter -> {
            probed.add(resumeAfter);
            return ProbeOutcome.eventPending();
        }).tick();

        assertThat(probed).isEmpty();
        assertThat(store.calls).containsExactly("saveSeen+hb:A");
        assertThat(store.lastSeenTimestamp).isEqualTo(eventTime);
        assertThat(store.lastHeartbeatTimestamp).isAfterOrEqualTo(eventTime);
    }

    @Test
    void unchangedToken_probeReturnsNewPbrt_advancesPositionAtomically() {
        ref.set(new TokenSnapshot(TOKEN_A, Instant.now()));
        CheckpointHeartbeat hb = heartbeat(resumeAfter -> ProbeOutcome.empty(PBRT));

        hb.tick(); // persists the seed event token
        hb.tick(); // unchanged → probe → new PBRT

        assertThat(store.calls).containsExactly("saveSeen+hb:A", "saveSeen+hb:PBRT");
        assertThat(ref.get().token()).isEqualTo(PBRT);
    }

    @Test
    void unchangedToken_probeReturnsSamePbrt_writesHeartbeatOnly() {
        ref.set(new TokenSnapshot(TOKEN_A, Instant.now()));
        CheckpointHeartbeat hb = heartbeat(resumeAfter -> ProbeOutcome.empty(TOKEN_A));

        hb.tick(); // persists the seed event token
        hb.tick(); // unchanged → probe → same PBRT → re-certification

        assertThat(store.calls).containsExactly("saveSeen+hb:A", "saveHeartbeat");
        // Position is byte-identical: no seen write happened on the second tick.
        assertThat(ref.get().token()).isEqualTo(TOKEN_A);
    }

    @Test
    void probeEventPending_abstains_noWrites() {
        ref.set(new TokenSnapshot(TOKEN_A, Instant.now()));
        CheckpointHeartbeat hb = heartbeat(resumeAfter -> ProbeOutcome.eventPending());

        hb.tick(); // persists the seed event token
        hb.tick(); // unchanged → probe → event pending → abstain

        assertThat(store.calls).containsExactly("saveSeen+hb:A");
    }

    @Test
    void probeFailure_noWrites_emitsDedicatedSignal() {
        ref.set(new TokenSnapshot(TOKEN_A, Instant.now()));
        RuntimeException boom = new RuntimeException("history lost");
        CheckpointHeartbeat hb = heartbeat(resumeAfter -> ProbeOutcome.failed(boom));

        hb.tick(); // persists the seed event token
        hb.tick(); // unchanged → probe → failure

        assertThat(store.calls).containsExactly("saveSeen+hb:A");
        assertThat(metrics.probeFailures).containsExactly(boom);
    }

    @Test
    void eventArrivingDuringProbe_winsTheCas_probeResultDiscarded() {
        ref.set(new TokenSnapshot(TOKEN_A, Instant.now()));
        CheckpointHeartbeat hb = heartbeat(resumeAfter -> {
            // Simulate the main stream delivering an event mid-probe.
            ref.set(new TokenSnapshot(TOKEN_B, Instant.now()));
            return ProbeOutcome.empty(PBRT);
        });

        hb.tick(); // persists the seed event token
        hb.tick(); // unchanged → probe → CAS loses against TOKEN_B

        assertThat(store.calls).containsExactly("saveSeen+hb:A");
        assertThat(ref.get().token()).isEqualTo(TOKEN_B);

        // Next tick: the event token that won the race is persisted normally.
        hb.tick();
        assertThat(store.calls).containsExactly("saveSeen+hb:A", "saveSeen+hb:B");
    }

    @Test
    void noInMemoryToken_chainsFromPersistedSeen() {
        store.persisted = new Checkpoint(STREAM, null, TOKEN_A, Instant.now(),
                null, null, java.util.Map.of());
        List<BsonDocument> probed = new ArrayList<>();

        heartbeat(resumeAfter -> {
            probed.add(resumeAfter);
            return ProbeOutcome.empty(PBRT);
        }).tick();

        assertThat(probed).containsExactly(TOKEN_A);
        assertThat(store.calls).containsExactly("saveSeen+hb:PBRT");
    }

    @Test
    void noPositionAnywhere_neverProbes() {
        List<BsonDocument> probed = new ArrayList<>();

        heartbeat(resumeAfter -> {
            probed.add(resumeAfter);
            return ProbeOutcome.empty(PBRT);
        }).tick();

        assertThat(probed).isEmpty();
        assertThat(store.calls).isEmpty();
    }

    @Test
    void storeThrow_isContained_andReportedAsCheckpointFailure() {
        ref.set(new TokenSnapshot(TOKEN_A, Instant.now()));
        store.throwOnWrite = true;

        heartbeat(resumeAfter -> ProbeOutcome.eventPending()).tick();

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
