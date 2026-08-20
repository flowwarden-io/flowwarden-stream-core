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

import io.flowwarden.stream.ChangeStreamContext;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnChange;
import io.flowwarden.stream.annotation.Pipeline;
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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * {@code $project} parity variant of the heartbeat pipeline-parity tests:
 * the probe replicates a pipeline that reshapes events (keeping {@code _id}
 * — a change stream {@code $project} must never drop the resume token). The
 * probe must advance past filtered writes under the projected pipeline, and
 * a matching write must be delivered in its projected shape (stripped fields
 * absent), proving the same stages run on the probe and on the stream.
 */
@SpringBootTest(classes = ImperativeHeartbeatProjectParityIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeHeartbeatProjectParityIntegrationTest {

    private static final String STREAM_NAME = "hb-project-parity";
    private static final String COLLECTION = "hb_project_parity";

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired MongoTemplate mongoTemplate;
    @Autowired ImperativeStreamManager streamManager;
    @Autowired CheckpointStore checkpointStore;
    @Autowired ProjectingHandler handler;

    @BeforeEach
    void setUp() {
        handler.clear();
        mongoTemplate.dropCollection(COLLECTION);
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
    void projectedPipeline_heartbeatAdvancesPastFilteredWrites_matchingWriteDeliveredProjected() {
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(checkpointStore.findByStreamName(STREAM_NAME)
                        .orElseThrow().lastSeenToken()).isNotNull());
        BsonDocument initialToken = checkpointStore.findByStreamName(STREAM_NAME)
                .orElseThrow().lastSeenToken();

        // Filtered writes: the listener sees nothing, the probe (same $match
        // + $project) must still certify past them.
        for (int i = 0; i < 10; i++) {
            mongoTemplate.insert(new Document("status", "inactive")
                    .append("secret", "s-" + i), COLLECTION);
        }

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var cp = checkpointStore.findByStreamName(STREAM_NAME).orElseThrow();
            assertThat(cp.lastSeenToken())
                    .as("the probe must advance under a $project pipeline")
                    .isNotEqualTo(initialToken);
        });
        assertThat(handler.count()).isZero();

        // A matching write arrives in its PROJECTED shape: tag kept, secret
        // stripped — the projection demonstrably ran on the delivery path too.
        mongoTemplate.insert(new Document("status", "active")
                .append("tag", "projected-live")
                .append("secret", "must-not-leak"), COLLECTION);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(handler.tags()).contains("projected-live"));
        assertThat(handler.secrets()).containsOnlyNulls();
    }

    @Test
    void restartFromProbedPosition_matchingWriteWhileDownStillDelivered() {
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(checkpointStore.findByStreamName(STREAM_NAME)
                        .orElseThrow().lastSeenToken()).isNotNull());

        streamManager.stopStream(STREAM_NAME);

        mongoTemplate.insert(new Document("status", "active")
                .append("tag", "while-down")
                .append("secret", "still-stripped"), COLLECTION);

        streamManager.startStream(STREAM_NAME);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(STREAM_NAME));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(handler.tags()).contains("while-down"));
        assertThat(handler.secrets()).containsOnlyNulls();
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import(ProjectingHandler.class)
    static class TestApp {}

    @ChangeStream(name = STREAM_NAME, collection = COLLECTION,
            documentType = Document.class, autoStart = false)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1, idleHeartbeatIntervalSeconds = 1)
    static class ProjectingHandler {

        private final List<Document> events = new CopyOnWriteArrayList<>();

        @Pipeline
        List<Document> pipeline() {
            return List.of(
                    new Document("$match", new Document("fullDocument.status", "active")),
                    // Keeping _id is mandatory: a change stream $project that
                    // drops it kills the cursor (the event _id IS the resume
                    // token). "secret" is deliberately not kept.
                    new Document("$project", new Document("_id", 1)
                            .append("operationType", 1)
                            .append("ns", 1)
                            .append("documentKey", 1)
                            .append("fullDocument.status", 1)
                            .append("fullDocument.tag", 1)));
        }

        @OnChange
        void handle(ChangeStreamContext<Document> ctx) {
            ctx.getFullDocument(Document.class).ifPresent(events::add);
        }

        int count() { return events.size(); }

        List<String> tags() {
            return events.stream().map(d -> d.getString("tag")).toList();
        }

        List<String> secrets() {
            return events.stream().map(d -> d.getString("secret")).toList();
        }

        void clear() { events.clear(); }
    }
}
