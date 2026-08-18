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
package io.flowwarden.stream.internal.imperative;

import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.flowwarden.stream.ChangeStreamContext;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Regression tests for the idle-stream heartbeat: without a single event on
 * the watched collection, {@code lastSeenToken} must stay a usable resume
 * point and {@code lastHeartbeatTimestamp} must stay fresh. Before the fix,
 * the timer only re-persisted the last received event token — a genuinely
 * idle stream froze until its token aged out of the oplog.
 */
@SpringBootTest(classes = ImperativeIdleHeartbeatIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeIdleHeartbeatIntegrationTest {

    private static final String STREAM_NAME = "idle-heartbeat";
    private static final String COLLECTION = "idle_heartbeat";
    private static final String OTHER_COLLECTION = "idle_heartbeat_other";

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired MongoTemplate mongoTemplate;
    @Autowired ImperativeStreamManager streamManager;
    @Autowired CheckpointStore checkpointStore;
    @Autowired IdleHandler handler;

    @BeforeEach
    void setUp() {
        handler.clear();
        mongoTemplate.dropCollection(COLLECTION);
        mongoTemplate.dropCollection(OTHER_COLLECTION);
        checkpointStore.delete(STREAM_NAME);
        streamManager.startStream(STREAM_NAME);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(STREAM_NAME));
    }

    @AfterEach
    void tearDown() {
        try { streamManager.stopStream(STREAM_NAME); } catch (Exception ignored) {}
        checkpointStore.delete(STREAM_NAME);
    }

    @Test
    void idleStream_zeroWrites_heartbeatFreshAndTokenUsable() {
        // Zero writes on the watched collection. The bootstrap + heartbeat
        // must still produce and maintain a checkpoint.
        Instant testStart = Instant.now();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var cp = checkpointStore.findByStreamName(STREAM_NAME).orElseThrow();
            assertThat(cp.lastSeenToken())
                    .as("bootstrap must persist a position before any event")
                    .isNotNull();
            assertThat(cp.lastHeartbeatTimestamp())
                    .as("heartbeat must confirm the position without traffic")
                    .isAfterOrEqualTo(testStart);
        });

        // The heartbeat keeps confirming across ticks (interval = 1s).
        Instant firstHeartbeat = checkpointStore.findByStreamName(STREAM_NAME)
                .orElseThrow().lastHeartbeatTimestamp();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var cp = checkpointStore.findByStreamName(STREAM_NAME).orElseThrow();
            assertThat(cp.lastHeartbeatTimestamp()).isAfter(firstHeartbeat);
        });

        // The persisted token is a working resume point.
        BsonDocument token = checkpointStore.findByStreamName(STREAM_NAME)
                .orElseThrow().lastSeenToken();
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor =
                     mongoTemplate.getCollection(COLLECTION).watch()
                             .resumeAfter(token)
                             .maxAwaitTime(1, TimeUnit.MILLISECONDS)
                             .cursor()) {
            cursor.tryNext(); // forces the server-side resume validation
        }
    }

    @Test
    void idleStream_oplogAdvancedElsewhere_tokenProgresses() {
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(checkpointStore.findByStreamName(STREAM_NAME)
                        .orElseThrow().lastSeenToken()).isNotNull());
        BsonDocument initialToken = checkpointStore.findByStreamName(STREAM_NAME)
                .orElseThrow().lastSeenToken();

        // Advance the oplog through writes on ANOTHER collection — the
        // watched collection stays idle.
        for (int i = 0; i < 20; i++) {
            mongoTemplate.insert(new Document("filler", i), OTHER_COLLECTION);
        }

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var cp = checkpointStore.findByStreamName(STREAM_NAME).orElseThrow();
            assertThat(cp.lastSeenToken())
                    .as("the heartbeat must surf the oplog head even with zero "
                            + "events on the watched collection")
                    .isNotEqualTo(initialToken);
        });
    }

    @Test
    void restartAfterIdlePeriod_resumesFromHeartbeatedToken_andDeliversNextEvent() {
        // Let the heartbeat advance past some unrelated oplog churn.
        for (int i = 0; i < 10; i++) {
            mongoTemplate.insert(new Document("filler", i), OTHER_COLLECTION);
        }
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(checkpointStore.findByStreamName(STREAM_NAME)
                        .orElseThrow().lastSeenToken()).isNotNull());

        streamManager.stopStream(STREAM_NAME);

        // An event lands while the stream is down.
        mongoTemplate.insert(new Document("type", "missed-while-down"), COLLECTION);

        streamManager.startStream(STREAM_NAME);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(STREAM_NAME));

        // The heartbeated token is a real resume point: the event written
        // during the downtime is delivered, not skipped.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(handler.docs()).extracting(d -> d.getString("type"))
                        .contains("missed-while-down"));
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import(IdleHandler.class)
    static class TestApp {}

    @ChangeStream(name = STREAM_NAME, collection = COLLECTION,
            documentType = Document.class, autoStart = false)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1)
    static class IdleHandler {

        private final List<Document> docs = new CopyOnWriteArrayList<>();

        @OnInsert
        void handle(ChangeStreamContext<Document> ctx) {
            ctx.getFullDocument(Document.class).ifPresent(docs::add);
        }

        List<Document> docs() { return docs; }
        void clear() { docs.clear(); }
    }
}
