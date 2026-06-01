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

import com.mongodb.reactivestreams.client.MongoClients;
import io.flowwarden.stream.spi.Checkpoint;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.bson.BsonDocument;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReactiveMongoCheckpointStoreIntegrationTest {

    private ReactiveMongoCheckpointStore store;
    private ReactiveMongoTemplate reactiveMongoTemplate;

    @BeforeEach
    void setUp() {
        reactiveMongoTemplate = new ReactiveMongoTemplate(
                MongoClients.create(SharedMongoContainer.MONGO.getReplicaSetUrl()), "test"
        );
        reactiveMongoTemplate.remove(new Query(), MongoCheckpointStore.COLLECTION).block();
        store = new ReactiveMongoCheckpointStore(reactiveMongoTemplate);
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
    void findReturnsEmptyWhenNotFound() {
        assertTrue(store.findByStreamName("nonexistent").isEmpty());
    }

    @Test
    void deleteRemovesCheckpoint() {
        store.save(new Checkpoint("del", null, null, null, null, null,
                Collections.emptyMap()));
        assertTrue(store.findByStreamName("del").isPresent());

        store.delete("del");
        assertTrue(store.findByStreamName("del").isEmpty());
    }

    @Test
    void deleteNonExistentDoesNotThrow() {
        assertDoesNotThrow(() -> store.delete("no-such-stream"));
    }

    @Test
    void tokenRoundTrip() {
        var seenToken = BsonDocument.parse(
                "{\"_data\": \"8263B5F100000000012B022C0100296E5A100484C0\"}"
        );
        var processedToken = BsonDocument.parse(
                "{\"_data\": \"8263B5F200000000012B022C0100296E5A100484C1\"}"
        );
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        store.save(new Checkpoint("token-test", null, seenToken, now,
                processedToken, now, Collections.emptyMap()));

        var found = store.findByStreamName("token-test").orElseThrow();
        assertEquals(seenToken, found.lastSeenToken());
        assertEquals(processedToken, found.lastProcessedToken());
        assertNotSame(seenToken, found.lastSeenToken());
        assertNotSame(processedToken, found.lastProcessedToken());
    }

    @Test
    void saveSeen_doesNotOverwriteProcessed() {
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        var seen1 = BsonDocument.parse("{\"_data\": \"seen-1\"}");
        var processed1 = BsonDocument.parse("{\"_data\": \"processed-1\"}");
        var seen2 = BsonDocument.parse("{\"_data\": \"seen-2\"}");
        var later = now.plusSeconds(10);

        // Pre-seed with both tokens
        store.save(new Checkpoint("s", null, seen1, now, processed1, now,
                Collections.emptyMap()));

        // Update only the seen pair
        store.saveSeen("s", seen2, later);

        // seen advanced, processed unchanged
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

        // No prior save — upsert should create the doc
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
}
