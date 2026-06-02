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
package io.flowwarden.stream.internal.dlq;

import com.mongodb.client.MongoClients;
import io.flowwarden.stream.spi.DlqPolicy;
import io.flowwarden.stream.spi.FailedEvent;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.query.Query;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class MongoDlqStoreIntegrationTest {

    private static final String DEFAULT_COLLECTION = "_fw_dlq";
    private static final String CUSTOM_COLLECTION = "orders_dlq";

    private static final DlqPolicy DEFAULT_POLICY = new DlqPolicy(30, true, true);

    private MongoTemplate mongoTemplate;
    private MongoDlqStore store;

    @BeforeEach
    void setUp() {
        mongoTemplate = new MongoTemplate(
                MongoClients.create(SharedMongoContainer.MONGO.getReplicaSetUrl()), "test"
        );
        mongoTemplate.remove(new Query(), DEFAULT_COLLECTION);
        mongoTemplate.remove(new Query(), CUSTOM_COLLECTION);

        MongoDlqProperties properties = new MongoDlqProperties();
        store = new MongoDlqStore(mongoTemplate, properties);
    }

    @Test
    void saveAndFindById() {
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        var docKey = new BsonDocument("_id", new BsonString("doc-1"));
        var fullDoc = new Document("status", "NEW").append("amount", 42);
        var resumeToken = BsonDocument.parse("{\"_data\": \"resume-1\"}");
        var error = new FailedEvent.ErrorInfo("RuntimeException", "boom", "stack trace here");
        var metadata = Map.<String, Object>of("env", "test");

        var event = new FailedEvent(
                "evt-1", "my-stream", "INSERT",
                docKey, fullDoc, resumeToken,
                error, 3, FailedEvent.STATUS_PENDING,
                now, now, now, null, metadata
        );

        store.save(event, DEFAULT_POLICY);

        var found = store.findById("evt-1");
        assertTrue(found.isPresent());

        var result = found.get();
        assertEquals("evt-1", result.id());
        assertEquals("my-stream", result.streamName());
        assertEquals("INSERT", result.operationType());
        assertNotNull(result.documentKey());
        assertEquals("NEW", result.fullDocument().getString("status"));
        assertEquals(42, result.fullDocument().getInteger("amount"));
        assertEquals(resumeToken, result.resumeToken());
        assertEquals("RuntimeException", result.error().type());
        assertEquals("boom", result.error().message());
        assertEquals("stack trace here", result.error().stackTrace());
        assertEquals(3, result.attempts());
        assertEquals("PENDING", result.status());
        assertEquals(now, result.firstAttemptAt());
        assertEquals(now, result.lastAttemptAt());
        assertEquals(now, result.createdAt());
        assertNull(result.expiresAt());
        assertEquals("test", result.metadata().get("env"));
    }

    @Test
    void findByIdReturnsEmptyWhenNotFound() {
        assertTrue(store.findById("nonexistent").isEmpty());
    }

    @Test
    void findByStreamName() {
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        store.save(makeEvent("e1", "stream-A", now), DEFAULT_POLICY);
        store.save(makeEvent("e2", "stream-A", now), DEFAULT_POLICY);
        store.save(makeEvent("e3", "stream-B", now), DEFAULT_POLICY);

        var streamAEvents = store.findByStreamName("stream-A");
        assertEquals(2, streamAEvents.size());
        assertTrue(streamAEvents.stream().allMatch(e -> "stream-A".equals(e.streamName())));

        var streamBEvents = store.findByStreamName("stream-B");
        assertEquals(1, streamBEvents.size());
        assertEquals("stream-B", streamBEvents.get(0).streamName());

        var emptyEvents = store.findByStreamName("stream-C");
        assertTrue(emptyEvents.isEmpty());
    }

    @Test
    void roundTripWithNullableFields() {
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        var event = new FailedEvent(
                "evt-null", "stream-x", "DELETE",
                null, null, null,
                new FailedEvent.ErrorInfo("Ex", "msg", null),
                1, FailedEvent.STATUS_PENDING,
                now, now, now, null, Collections.emptyMap()
        );

        store.save(event, DEFAULT_POLICY);

        var found = store.findById("evt-null").orElseThrow();
        assertNull(found.documentKey());
        assertNull(found.fullDocument());
        assertNull(found.resumeToken());
        assertNull(found.error().stackTrace());
        assertNull(found.expiresAt());
    }

    @Test
    void saveOverwritesExisting() {
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        store.save(makeEvent("e1", "s", now), DEFAULT_POLICY);
        store.save(new FailedEvent(
                "e1", "s-updated", "UPDATE",
                null, null, null,
                new FailedEvent.ErrorInfo("Ex", "updated", null),
                5, FailedEvent.STATUS_PENDING,
                now, now, now, null, Collections.emptyMap()
        ), DEFAULT_POLICY);

        var found = store.findById("e1").orElseThrow();
        assertEquals("s-updated", found.streamName());
        assertEquals(5, found.attempts());
    }

    @Test
    void registerStreamRoutesToCustomCollection() {
        store.registerStream("orders-stream", CUSTOM_COLLECTION);

        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        store.save(makeEvent("evt-orders", "orders-stream", now), DEFAULT_POLICY);

        // Document landed in the custom collection, not the default one
        assertThat(mongoTemplate.findAll(Document.class, CUSTOM_COLLECTION)).hasSize(1);
        assertThat(mongoTemplate.findAll(Document.class, DEFAULT_COLLECTION)).isEmpty();

        // findByStreamName routes back to the same custom collection
        List<FailedEvent> found = store.findByStreamName("orders-stream");
        assertEquals(1, found.size());
        assertEquals("evt-orders", found.get(0).id());
    }

    @Test
    void registerStreamWithEmptyCollectionUsesDefault() {
        store.registerStream("default-stream", "");

        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        store.save(makeEvent("evt-default", "default-stream", now), DEFAULT_POLICY);

        assertThat(mongoTemplate.findAll(Document.class, DEFAULT_COLLECTION)).hasSize(1);
    }

    @Test
    void registerStreamCreatesTtlIndexOnExpiresAt() {
        store.registerStream("ttl-stream", CUSTOM_COLLECTION);

        IndexInfo ttlIndex = mongoTemplate.indexOps(CUSTOM_COLLECTION).getIndexInfo().stream()
                .filter(idx -> idx.getIndexFields().stream()
                        .anyMatch(f -> "expiresAt".equals(f.getKey())))
                .findFirst()
                .orElseThrow(() -> new AssertionError("TTL index on expiresAt was not created"));
        assertThat(ttlIndex.getExpireAfter()).isPresent();
    }

    private static FailedEvent makeEvent(String id, String streamName, Instant now) {
        return new FailedEvent(
                id, streamName, "INSERT",
                null, null, null,
                new FailedEvent.ErrorInfo("TestEx", "test", null),
                1, FailedEvent.STATUS_PENDING,
                now, now, now, null, Collections.emptyMap()
        );
    }
}
