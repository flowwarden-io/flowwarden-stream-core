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
 * Regression test for issue #22: a handler-success checkpoint save must not
 * overwrite the {@code lastSeenToken} that the heartbeat timer has been
 * advancing through filtered events.
 *
 * <p>Scenario: a stream with a {@code @Filter} that rejects two thirds of
 * incoming events plus {@code @Checkpoint(saveIntervalSeconds = 1)} for a
 * fast-firing timer. After driving a mix of rejected and accepted events,
 * the persisted checkpoint must reflect the dual-token model — that is,
 * {@code lastSeenTimestamp > lastProcessedTimestamp} (the timer has advanced
 * seen past the latest processed event by capturing rejected events).</p>
 *
 * <p>Before the fix, {@code saveCheckpointIfNeeded} called the legacy
 * {@code CheckpointStore.save(Checkpoint)} with the same token in both pairs
 * of fields, overwriting the heartbeat timer's work on every successful
 * handler invocation. The checkpoint always collapsed to
 * {@code lastSeenTimestamp == lastProcessedTimestamp}.</p>
 */
@SpringBootTest(classes = ImperativeCheckpointDivergencePersistenceIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeCheckpointDivergencePersistenceIntegrationTest {

    private static final String STREAM_NAME = "divergence-persistence";
    private static final String COLLECTION = "divergence_persistence";

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired MongoTemplate mongoTemplate;
    @Autowired ImperativeStreamManager streamManager;
    @Autowired CheckpointStore checkpointStore;
    @Autowired DivergenceHandler handler;

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
    void timerSeenAdvanceIsPreservedAfterHandlerSuccess() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(STREAM_NAME));

        // 1) Drive an accepted event so lastProcessedToken is set at a known position.
        mongoTemplate.insert(new Document("type", "keep").append("seq", 1), COLLECTION);
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(handler.size()).isEqualTo(1));

        // Wait for the checkpoint to be persisted (saveEveryN = 1).
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> {
                    var cp = checkpointStore.findByStreamName(STREAM_NAME);
                    assertThat(cp).isPresent();
                    assertThat(cp.get().lastProcessedToken()).isNotNull();
                    assertThat(cp.get().lastSeenToken()).isNotNull();
                });

        // 2) Drive several rejected events. These bump the in-memory snapshot
        // (handleMessage updates lastSeenToken snapshot before checking the filter),
        // so the heartbeat timer will advance lastSeenToken through them — without
        // any saveCheckpointIfNeeded call (filter rejects → handler never runs).
        for (int i = 2; i <= 6; i++) {
            mongoTemplate.insert(new Document("type", "skip").append("seq", i), COLLECTION);
        }

        // 3) Wait long enough for the timer (saveIntervalSeconds = 1) to fire AT
        // LEAST ONCE after the rejected events landed. We allow several ticks
        // to ensure determinism.
        await().atMost(Duration.ofSeconds(8))
                .pollDelay(Duration.ofSeconds(3))
                .untilAsserted(() -> {
                    var cp = checkpointStore.findByStreamName(STREAM_NAME).orElseThrow();
                    // After timer ticks, lastSeenTimestamp must have advanced past
                    // lastProcessedTimestamp (which is still pinned to the single
                    // accepted event from step 1).
                    assertThat(cp.lastSeenTimestamp())
                            .as("lastSeenTimestamp must be strictly after lastProcessedTimestamp — "
                                    + "the heartbeat timer should advance seen through filtered events, "
                                    + "and the handler-success save must not overwrite it")
                            .isAfter(cp.lastProcessedTimestamp());
                    assertThat(cp.lastSeenToken())
                            .as("lastSeenToken must differ from lastProcessedToken once the timer "
                                    + "has captured a rejected event")
                            .isNotEqualTo(cp.lastProcessedToken());
                });
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import(DivergenceHandler.class)
    static class TestApp {}

    @ChangeStream(name = STREAM_NAME, collection = COLLECTION, documentType = Document.class)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1)
    static class DivergenceHandler {

        private final List<ChangeStreamContext<Document>> events = new CopyOnWriteArrayList<>();

        @OnInsert
        void handle(ChangeStreamContext<Document> ctx) {
            events.add(ctx);
        }

        @Filter
        boolean keepOnly(ChangeStreamContext<Document> ctx) {
            // Reject anything that isn't explicitly tagged "keep". This drops the
            // "skip"-tagged events before they reach the handler — they still
            // traverse handleMessage so the timer captures their resume token.
            return ctx.getFullDocument(Document.class)
                    .map(doc -> "keep".equals(doc.getString("type")))
                    .orElse(false);
        }

        int size() { return events.size(); }
        void clear() { events.clear(); }
    }
}
