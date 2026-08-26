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
import io.flowwarden.stream.annotation.Filter;
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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The settled-anchor semantics of {@code lastProcessedToken}: every
 * terminally settled event counts, {@code @Filter} rejections included — a
 * filter-heavy stream stays recoverable at level 1 through its rejected
 * traffic. And the structural counterpart of the old catch-up problem:
 * delivered tokens never touch {@code lastSeenToken}, which belongs
 * exclusively to the idle probe's certified PBRTs.
 */
@SpringBootTest(classes = ImperativeSettledAnchorIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeSettledAnchorIntegrationTest {

    private static final String STREAM_NAME = "settled-anchor";
    private static final String COLLECTION = "settled_anchor";

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired MongoTemplate mongoTemplate;
    @Autowired ImperativeStreamManager streamManager;
    @Autowired CheckpointStore checkpointStore;
    @Autowired AnchorHandler handler;

    @BeforeEach
    void setUp() {
        handler.clear();
        mongoTemplate.dropCollection(COLLECTION);
        checkpointStore.delete(STREAM_NAME);
    }

    @AfterEach
    void tearDown() {
        try { streamManager.stopStream(STREAM_NAME); } catch (Exception ignored) {}
        checkpointStore.delete(STREAM_NAME);
    }

    @Test
    void filteredSettlements_advanceTheProcessedAnchor_neverTheSeenPosition() {
        streamManager.startStream(STREAM_NAME);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(STREAM_NAME));
        // The bootstrap persisted an initial certified position — the only
        // legitimate seen writer besides the idle probe.
        BsonDocument bootstrapSeen = checkpointStore.findByStreamName(STREAM_NAME)
                .orElseThrow().lastSeenToken();

        // 1) An accepted event anchors a known position (saveEveryN = 1).
        mongoTemplate.insert(new Document("type", "keep").append("seq", 1), COLLECTION);
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(handler.size()).isEqualTo(1));
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(checkpointStore.findByStreamName(STREAM_NAME)
                        .map(cp -> cp.lastProcessedToken()).orElse(null)).isNotNull());
        BsonDocument acceptedAnchor = checkpointStore.findByStreamName(STREAM_NAME)
                .orElseThrow().lastProcessedToken();

        // 2) Rejected events are terminal settlements too: the anchor must
        // advance through them — no handler ran, yet a restart from the
        // anchor must not re-deliver the whole rejected backlog.
        for (int i = 2; i <= 6; i++) {
            mongoTemplate.insert(new Document("type", "skip").append("seq", i), COLLECTION);
        }

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> {
                    var cp = checkpointStore.findByStreamName(STREAM_NAME).orElseThrow();
                    assertThat(cp.lastProcessedToken())
                            .as("filtered settlements advance the processed anchor")
                            .isNotEqualTo(acceptedAnchor);
                });

        // 3) The seen position belongs to the probe alone: no delivered
        // token — accepted or rejected — may ever appear there. With idle
        // probing opted out, the certified position must still be the
        // bootstrap PBRT, byte-identical.
        assertThat(checkpointStore.findByStreamName(STREAM_NAME).orElseThrow().lastSeenToken())
                .as("delivered tokens never write lastSeenToken")
                .isEqualTo(bootstrapSeen);
        assertThat(handler.size()).as("the filter kept rejecting").isEqualTo(1);
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import(AnchorHandler.class)
    static class TestApp {}

    @ChangeStream(name = STREAM_NAME, collection = COLLECTION, documentType = Document.class,
            autoStart = false)
    // Idle probing opted out: the startup diagnostic probe could otherwise
    // legitimately advance the seen to a fresher certified PBRT — this test
    // pins that DELIVERED tokens never write it, so the bootstrap position
    // must stay byte-identical.
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1, idleHeartbeatIntervalSeconds = 0)
    static class AnchorHandler {

        private final List<ChangeStreamContext<Document>> events = new CopyOnWriteArrayList<>();

        @OnInsert
        void handle(ChangeStreamContext<Document> ctx) {
            events.add(ctx);
        }

        @Filter
        boolean keepOnly(ChangeStreamContext<Document> ctx) {
            // Reject anything that isn't explicitly tagged "keep": the
            // "skip"-tagged events settle terminally without a handler run.
            return ctx.getFullDocument(Document.class)
                    .map(doc -> "keep".equals(doc.getString("type")))
                    .orElse(false);
        }

        int size() { return events.size(); }
        void clear() { events.clear(); }
    }
}
