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
import org.bson.BsonDocument;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import io.flowwarden.stream.test.SharedMongoContainer;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MongoCheckpointStoreIntegrationTest {

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
}
