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
import io.flowwarden.stream.FullDocumentMode;
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
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Pipeline-parity tests for the heartbeat probe: the probe replicates the
 * stream's {@code @Pipeline} stages and {@code fullDocument} option, so on a
 * server-side-filtered stream (where the listener receives nothing) the
 * heartbeat still advances past non-matching writes — and a later matching
 * write is delivered, both live and after a restart from the probed position.
 */
@SpringBootTest(classes = ImperativeHeartbeatPipelineParityIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeHeartbeatPipelineParityIntegrationTest {

    private static final String STREAM_NAME = "hb-pipeline-parity";
    private static final String COLLECTION = "hb_pipeline_parity";

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired MongoTemplate mongoTemplate;
    @Autowired ImperativeStreamManager streamManager;
    @Autowired CheckpointStore checkpointStore;
    @Autowired FilteredHandler handler;

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
    void serverFilteredWrites_heartbeatAdvancesPastThem_matchingWriteStillDelivered() {
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(checkpointStore.findByStreamName(STREAM_NAME)
                        .orElseThrow().lastSeenToken()).isNotNull());
        BsonDocument initialToken = checkpointStore.findByStreamName(STREAM_NAME)
                .orElseThrow().lastSeenToken();

        // Writes the server-side $match rejects: the listener never sees them,
        // yet the probe (same pipeline) certifies past them.
        for (int i = 0; i < 10; i++) {
            mongoTemplate.insert(new Document("status", "inactive").append("seq", i), COLLECTION);
        }

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var cp = checkpointStore.findByStreamName(STREAM_NAME).orElseThrow();
            assertThat(cp.lastSeenToken())
                    .as("the probe must advance past server-filtered writes")
                    .isNotEqualTo(initialToken);
        });
        assertThat(handler.count()).isZero();

        // A matching write is still delivered — nothing was wrongly skipped.
        mongoTemplate.insert(new Document("status", "active").append("tag", "live"), COLLECTION);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(handler.tags()).contains("live"));
    }

    @Test
    void updateLookupParity_matchingUpdateDeliveredAfterRestartFromProbedPosition() {
        // The $match targets fullDocument.status — for update events this
        // requires UPDATE_LOOKUP on the probe as well, otherwise the probe
        // would certify emptiness incorrectly.
        mongoTemplate.insert(new Document("_id", "doc-1").append("status", "inactive"), COLLECTION);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(checkpointStore.findByStreamName(STREAM_NAME)
                        .orElseThrow().lastSeenToken()).isNotNull());

        streamManager.stopStream(STREAM_NAME);

        // While the stream is down: a matching update lands.
        mongoTemplate.updateFirst(
                Query.query(Criteria.where("_id").is("doc-1")),
                Update.update("status", "active").set("tag", "updated-while-down"),
                COLLECTION);

        streamManager.startStream(STREAM_NAME);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(STREAM_NAME));

        // Resuming from the persisted position must deliver the matching
        // update — the probed position never crossed it.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(handler.tags()).contains("updated-while-down"));
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import(FilteredHandler.class)
    static class TestApp {}

    @ChangeStream(name = STREAM_NAME, collection = COLLECTION,
            documentType = Document.class, autoStart = false,
            fullDocument = FullDocumentMode.UPDATE_LOOKUP)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1, idleHeartbeatIntervalSeconds = 1)
    static class FilteredHandler {

        private final List<Document> events = new CopyOnWriteArrayList<>();

        @Pipeline
        List<Document> pipeline() {
            return List.of(new Document("$match",
                    new Document("fullDocument.status", "active")));
        }

        @OnChange
        void handle(ChangeStreamContext<Document> ctx) {
            ctx.getFullDocument(Document.class).ifPresent(events::add);
        }

        int count() { return events.size(); }

        List<String> tags() {
            return events.stream().map(d -> d.getString("tag")).toList();
        }

        void clear() { events.clear(); }
    }
}
