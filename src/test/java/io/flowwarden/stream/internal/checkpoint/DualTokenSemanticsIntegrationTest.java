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

import com.mongodb.client.MongoClients;
import io.flowwarden.stream.spi.Checkpoint;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.bson.BsonDocument;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that {@link MongoCheckpointStore} preserves the dual-token semantics
 * mandated by the resume cascade: {@code saveSeen} and {@code saveProcessed}
 * advance their respective fields independently and never accidentally
 * overwrite the other pair.
 *
 * <p>These are SPI-level tests focused on the storage layer. End-to-end
 * verification of the cascade behavior (timer writing only seen; resume
 * falling back from processed to seen when processed has aged out) lives in
 * {@code ImperativeCheckpointIntegrationTest} / {@code ReactiveCheckpointIntegrationTest}.</p>
 */
class DualTokenSemanticsIntegrationTest {

    private MongoCheckpointStore store;

    @BeforeEach
    void setUp() {
        var mongoTemplate = new MongoTemplate(
                MongoClients.create(SharedMongoContainer.MONGO.getReplicaSetUrl()), "test"
        );
        mongoTemplate.remove(new Query(), MongoCheckpointStore.COLLECTION);
        store = new MongoCheckpointStore(mongoTemplate);
    }

    @Test
    void timerSimulation_repeatedSaveSeen_preservesProcessed() {
        // Initial state: handler success has persisted both tokens at the same value
        var t0 = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        var processedToken = BsonDocument.parse("{\"_data\": \"processed-T0\"}");
        store.save(new Checkpoint("s", null, processedToken, t0, processedToken, t0,
                Collections.emptyMap()));

        // Simulate 5 timer ticks while no handler runs (e.g. filtered events)
        var seenTokens = List.of(
                BsonDocument.parse("{\"_data\": \"seen-T1\"}"),
                BsonDocument.parse("{\"_data\": \"seen-T2\"}"),
                BsonDocument.parse("{\"_data\": \"seen-T3\"}"),
                BsonDocument.parse("{\"_data\": \"seen-T4\"}"),
                BsonDocument.parse("{\"_data\": \"seen-T5\"}")
        );

        for (int i = 0; i < seenTokens.size(); i++) {
            store.saveSeen("s", seenTokens.get(i), t0.plusSeconds(i + 1L));
        }

        // After 5 timer ticks: lastSeenToken at T5, lastProcessedToken still at T0
        var found = store.findByStreamName("s").orElseThrow();
        assertEquals(seenTokens.get(4), found.lastSeenToken(),
                "lastSeenToken should reflect the latest timer write");
        assertEquals(t0.plusSeconds(5), found.lastSeenTimestamp());
        assertEquals(processedToken, found.lastProcessedToken(),
                "lastProcessedToken must not advance during timer-only writes");
        assertEquals(t0, found.lastProcessedTimestamp(),
                "lastProcessedTimestamp must not advance during timer-only writes");
    }

    @Test
    void handlerSuccessSimulation_saveProcessed_advancesProcessedOnly() {
        // Initial state: timer has been running for a while, seen is ahead
        var t0 = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        var seenAhead = BsonDocument.parse("{\"_data\": \"seen-T10\"}");
        var processedOld = BsonDocument.parse("{\"_data\": \"processed-T0\"}");
        store.save(new Checkpoint("s", null, seenAhead, t0.plusSeconds(10),
                processedOld, t0, Collections.emptyMap()));

        // Handler succeeds on a specific event (saveProcessed by the manager)
        var processedNew = BsonDocument.parse("{\"_data\": \"processed-T5\"}");
        store.saveProcessed("s", processedNew, t0.plusSeconds(5));

        var found = store.findByStreamName("s").orElseThrow();
        assertEquals(processedNew, found.lastProcessedToken());
        assertEquals(t0.plusSeconds(5), found.lastProcessedTimestamp());
        assertEquals(seenAhead, found.lastSeenToken(),
                "lastSeenToken must not be rolled back when saveProcessed runs");
        assertEquals(t0.plusSeconds(10), found.lastSeenTimestamp());
    }

    @Test
    void firstEventScenario_saveSeenThenSaveProcessed_buildsCheckpointIncrementally() {
        // Fresh stream — no prior checkpoint.
        // Timer fires first with seen=T1, then handler succeeds with processed=T1.
        var t1 = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        var token = BsonDocument.parse("{\"_data\": \"event-T1\"}");

        store.saveSeen("s-new", token, t1);

        // After timer: seen set, processed null
        var afterTimer = store.findByStreamName("s-new").orElseThrow();
        assertEquals(token, afterTimer.lastSeenToken());
        assertNull(afterTimer.lastProcessedToken());

        // Handler succeeds → saveProcessed
        store.saveProcessed("s-new", token, t1);

        // Final state: both at T1
        var afterHandler = store.findByStreamName("s-new").orElseThrow();
        assertEquals(token, afterHandler.lastSeenToken());
        assertEquals(token, afterHandler.lastProcessedToken());
    }

    @Test
    void writers_areIndependent_acrossMultipleStreams() {
        // Two streams interleaving saveSeen / saveProcessed must not interfere.
        var t0 = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        var tokenA = BsonDocument.parse("{\"_data\": \"a\"}");
        var tokenB = BsonDocument.parse("{\"_data\": \"b\"}");

        store.saveSeen("stream-A", tokenA, t0);
        store.saveProcessed("stream-B", tokenB, t0);

        var a = store.findByStreamName("stream-A").orElseThrow();
        var b = store.findByStreamName("stream-B").orElseThrow();

        assertEquals(tokenA, a.lastSeenToken());
        assertNull(a.lastProcessedToken());

        assertNull(b.lastSeenToken());
        assertEquals(tokenB, b.lastProcessedToken());
    }

    @Test
    void appendOnlySemantics_capturesProgress_oneStreamMultipleStages() {
        // Simulates the full lifecycle of one stream: receive events, handler runs,
        // timer ticks, more events, more timer ticks, more handler runs.
        var t0 = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        // Event 1 received
        store.saveSeen("lifecycle", BsonDocument.parse("{\"_data\": \"e1\"}"), t0.plusSeconds(1));
        // Event 1 handler success
        store.saveProcessed("lifecycle", BsonDocument.parse("{\"_data\": \"e1\"}"), t0.plusSeconds(1));

        // Events 2 and 3 received but filtered out
        store.saveSeen("lifecycle", BsonDocument.parse("{\"_data\": \"e2\"}"), t0.plusSeconds(2));
        store.saveSeen("lifecycle", BsonDocument.parse("{\"_data\": \"e3\"}"), t0.plusSeconds(3));

        var midState = store.findByStreamName("lifecycle").orElseThrow();
        assertEquals(BsonDocument.parse("{\"_data\": \"e3\"}"), midState.lastSeenToken());
        assertEquals(BsonDocument.parse("{\"_data\": \"e1\"}"), midState.lastProcessedToken(),
                "Filtered events e2/e3 must not advance lastProcessedToken");

        // Event 4 received and handler success
        store.saveSeen("lifecycle", BsonDocument.parse("{\"_data\": \"e4\"}"), t0.plusSeconds(4));
        store.saveProcessed("lifecycle", BsonDocument.parse("{\"_data\": \"e4\"}"), t0.plusSeconds(4));

        var finalState = store.findByStreamName("lifecycle").orElseThrow();
        assertEquals(BsonDocument.parse("{\"_data\": \"e4\"}"), finalState.lastSeenToken());
        assertEquals(BsonDocument.parse("{\"_data\": \"e4\"}"), finalState.lastProcessedToken());
    }

    /**
     * Verifies that the default implementation of {@code saveSeen}/{@code saveProcessed}
     * (in {@link CheckpointStore} interface) correctly delegates to
     * {@code findByStreamName + save} without breaking semantics. This is what
     * external store implementations get for free if they only override
     * {@code save} / {@code findByStreamName} / {@code delete}.
     */
    @Test
    void defaultImplementation_preservesDualTokenSemantics() {
        var recorder = new InMemoryStoreWithoutOverrides();

        var t0 = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        var processedToken = BsonDocument.parse("{\"_data\": \"processed-T0\"}");
        recorder.save(new Checkpoint("s", null, processedToken, t0, processedToken, t0,
                Collections.emptyMap()));

        // Default saveSeen should leave processed untouched
        var seenLater = BsonDocument.parse("{\"_data\": \"seen-T5\"}");
        recorder.saveSeen("s", seenLater, t0.plusSeconds(5));

        var cp = recorder.findByStreamName("s").orElseThrow();
        assertEquals(seenLater, cp.lastSeenToken());
        assertEquals(processedToken, cp.lastProcessedToken(),
                "Default saveSeen must preserve lastProcessedToken via read-modify-write");

        // Default saveProcessed should leave seen untouched
        var processedLater = BsonDocument.parse("{\"_data\": \"processed-T7\"}");
        recorder.saveProcessed("s", processedLater, t0.plusSeconds(7));

        var cp2 = recorder.findByStreamName("s").orElseThrow();
        assertEquals(seenLater, cp2.lastSeenToken(),
                "Default saveProcessed must preserve lastSeenToken via read-modify-write");
        assertEquals(processedLater, cp2.lastProcessedToken());

        // Confirm the read-modify-write path was taken (default impls call findByStreamName + save)
        assertTrue(recorder.findCalls() >= 2, "Default impls should consult findByStreamName");
        assertFalse(recorder.saveCalls() < 3, "Default impls should call save(Checkpoint)");
    }

    /**
     * Minimal in-memory CheckpointStore that does NOT override saveSeen/saveProcessed,
     * so it exercises the default implementations from the SPI interface.
     */
    private static final class InMemoryStoreWithoutOverrides implements CheckpointStore {
        private Checkpoint stored;
        private final List<String> findCalls = new CopyOnWriteArrayList<>();
        private final List<String> saveCalls = new CopyOnWriteArrayList<>();

        @Override
        public void save(Checkpoint checkpoint) {
            stored = checkpoint;
            saveCalls.add(checkpoint.streamName());
        }

        @Override
        public java.util.Optional<Checkpoint> findByStreamName(String streamName) {
            findCalls.add(streamName);
            if (stored != null && stored.streamName().equals(streamName)) {
                return java.util.Optional.of(stored);
            }
            return java.util.Optional.empty();
        }

        @Override
        public void delete(String streamName) {
            if (stored != null && stored.streamName().equals(streamName)) {
                stored = null;
            }
        }

        int findCalls() { return findCalls.size(); }
        int saveCalls() { return saveCalls.size(); }
    }
}
