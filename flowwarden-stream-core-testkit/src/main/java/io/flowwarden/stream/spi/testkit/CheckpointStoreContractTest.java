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
package io.flowwarden.stream.spi.testkit;

import io.flowwarden.stream.spi.Checkpoint;
import io.flowwarden.stream.spi.CheckpointStore;
import org.bson.BsonDocument;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Behavior contract that every {@link CheckpointStore} implementation must satisfy.
 *
 * <p>Subclass for each backend (Mongo, Redis, JDBC, …) and provide a fresh
 * {@link CheckpointStore} via {@link #createCheckpointStore()} plus a
 * {@link #cleanState()} hook that resets the backing storage between tests.</p>
 */
public abstract class CheckpointStoreContractTest {

    protected CheckpointStore store;

    /**
     * Returns a fresh {@link CheckpointStore} bound to a clean backend.
     */
    protected abstract CheckpointStore createCheckpointStore();

    /**
     * Clears all state in the backing storage so tests are isolated.
     */
    protected abstract void cleanState();

    @BeforeEach
    void setUpContract() {
        cleanState();
        store = createCheckpointStore();
    }

    @Test
    void saveAndFind() {
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        var token = BsonDocument.parse("{\"_data\": \"resume-1\"}");
        var cp = new Checkpoint("stream-1", "pod-a", token, now, token, now,
                Map.of("env", "test"));

        store.save(cp);

        var found = store.findByStreamName("stream-1");
        assertTrue(found.isPresent());

        var result = found.get();
        assertEquals("stream-1", result.streamName());
        assertEquals("pod-a", result.instanceId());
        assertEquals(token, result.lastSeenToken());
        assertEquals(now, result.lastSeenTimestamp());
        assertEquals(token, result.lastProcessedToken());
        assertEquals(now, result.lastProcessedTimestamp());
        assertEquals("test", result.metadata().get("env"));
    }

    @Test
    void saveUpdatesExisting() {
        var token1 = BsonDocument.parse("{\"_data\": \"t1\"}");
        var token2 = BsonDocument.parse("{\"_data\": \"t2\"}");
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        store.save(new Checkpoint("s", null, token1, now, null, null, Collections.emptyMap()));
        store.save(new Checkpoint("s", "pod-b", token2, now, token2, now, Collections.emptyMap()));

        var found = store.findByStreamName("s");
        assertTrue(found.isPresent());
        assertEquals(token2, found.get().lastSeenToken());
        assertEquals("pod-b", found.get().instanceId());
    }

    @Test
    void findReturnsEmptyWhenNotFound() {
        assertTrue(store.findByStreamName("nonexistent").isEmpty());
    }

    @Test
    void deleteRemovesCheckpoint() {
        store.save(new Checkpoint("del", null, null, null, null, null, Collections.emptyMap()));
        assertTrue(store.findByStreamName("del").isPresent());

        store.delete("del");
        assertTrue(store.findByStreamName("del").isEmpty());
    }

    @Test
    void deleteNonExistentDoesNotThrow() {
        assertDoesNotThrow(() -> store.delete("no-such-stream"));
    }

    @Test
    void saveSeen_doesNotOverwriteProcessed() {
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        var seen1 = BsonDocument.parse("{\"_data\": \"seen-1\"}");
        var processed1 = BsonDocument.parse("{\"_data\": \"processed-1\"}");
        var seen2 = BsonDocument.parse("{\"_data\": \"seen-2\"}");
        var later = now.plusSeconds(10);

        store.save(new Checkpoint("s", null, seen1, now, processed1, now,
                Collections.emptyMap()));

        store.saveSeen("s", seen2, later);

        var found = store.findByStreamName("s").orElseThrow();
        assertEquals(seen2, found.lastSeenToken());
        assertEquals(later, found.lastSeenTimestamp());
        assertEquals(processed1, found.lastProcessedToken());
        assertEquals(now, found.lastProcessedTimestamp());
    }

    @Test
    void saveProcessed_doesNotOverwriteSeen() {
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        var seen1 = BsonDocument.parse("{\"_data\": \"seen-1\"}");
        var processed1 = BsonDocument.parse("{\"_data\": \"processed-1\"}");
        var processed2 = BsonDocument.parse("{\"_data\": \"processed-2\"}");
        var later = now.plusSeconds(10);

        store.save(new Checkpoint("s", null, seen1, now, processed1, now,
                Collections.emptyMap()));

        store.saveProcessed("s", processed2, later);

        var found = store.findByStreamName("s").orElseThrow();
        assertEquals(seen1, found.lastSeenToken());
        assertEquals(now, found.lastSeenTimestamp());
        assertEquals(processed2, found.lastProcessedToken());
        assertEquals(later, found.lastProcessedTimestamp());
    }

    @Test
    void saveSeen_createsDocumentIfMissing() {
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        var seen = BsonDocument.parse("{\"_data\": \"first-seen\"}");

        store.saveSeen("new-stream", seen, now);

        var found = store.findByStreamName("new-stream").orElseThrow();
        assertEquals(seen, found.lastSeenToken());
        assertEquals(now, found.lastSeenTimestamp());
        assertNull(found.lastProcessedToken());
        assertNull(found.lastProcessedTimestamp());
    }

    @Test
    void saveProcessed_createsDocumentIfMissing() {
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        var processed = BsonDocument.parse("{\"_data\": \"first-processed\"}");

        store.saveProcessed("new-stream", processed, now);

        var found = store.findByStreamName("new-stream").orElseThrow();
        assertEquals(processed, found.lastProcessedToken());
        assertEquals(now, found.lastProcessedTimestamp());
        assertNull(found.lastSeenToken());
        assertNull(found.lastSeenTimestamp());
    }

    @Test
    void saveAndFind_roundTripsHeartbeatTimestamp() {
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        var token = BsonDocument.parse("{\"_data\": \"hb-roundtrip\"}");

        store.save(new Checkpoint("s", null, token, now, token, now, now,
                Collections.emptyMap()));

        var found = store.findByStreamName("s").orElseThrow();
        assertEquals(now, found.lastHeartbeatTimestamp());
    }

    @Test
    void saveSeenWithHeartbeat_writesPositionAndConfirmationTogether() {
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        var processed = BsonDocument.parse("{\"_data\": \"processed-1\"}");
        var seen = BsonDocument.parse("{\"_data\": \"seen-2\"}");
        var later = now.plusSeconds(10);
        var heartbeat = now.plusSeconds(11);

        store.save(new Checkpoint("s", null, null, null, processed, now,
                Collections.emptyMap()));

        store.saveSeen("s", seen, later, heartbeat);

        var found = store.findByStreamName("s").orElseThrow();
        assertEquals(seen, found.lastSeenToken());
        assertEquals(later, found.lastSeenTimestamp());
        assertEquals(heartbeat, found.lastHeartbeatTimestamp());
        assertEquals(processed, found.lastProcessedToken());
        assertEquals(now, found.lastProcessedTimestamp());
    }

    @Test
    void saveHeartbeat_touchesOnlyTheHeartbeatTimestamp() {
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        var seen = BsonDocument.parse("{\"_data\": \"seen-1\"}");
        var processed = BsonDocument.parse("{\"_data\": \"processed-1\"}");
        var heartbeat = now.plusSeconds(30);

        store.save(new Checkpoint("s", null, seen, now, processed, now,
                Collections.emptyMap()));

        store.saveHeartbeat("s", heartbeat);

        var found = store.findByStreamName("s").orElseThrow();
        assertEquals(heartbeat, found.lastHeartbeatTimestamp());
        assertEquals(seen, found.lastSeenToken());
        assertEquals(now, found.lastSeenTimestamp());
        assertEquals(processed, found.lastProcessedToken());
        assertEquals(now, found.lastProcessedTimestamp());
    }

    @Test
    void saveHeartbeat_createsDocumentIfMissing() {
        var heartbeat = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        store.saveHeartbeat("new-stream", heartbeat);

        var found = store.findByStreamName("new-stream").orElseThrow();
        assertEquals(heartbeat, found.lastHeartbeatTimestamp());
        assertNull(found.lastSeenToken());
        assertNull(found.lastProcessedToken());
    }

    @Test
    void saveProcessed_preservesHeartbeatTimestamp() {
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        var seen = BsonDocument.parse("{\"_data\": \"seen-1\"}");
        var processed = BsonDocument.parse("{\"_data\": \"processed-2\"}");

        store.save(new Checkpoint("s", null, seen, now, null, null, now,
                Collections.emptyMap()));

        store.saveProcessed("s", processed, now.plusSeconds(5));

        var found = store.findByStreamName("s").orElseThrow();
        assertEquals(now, found.lastHeartbeatTimestamp());
        assertEquals(processed, found.lastProcessedToken());
    }
}
