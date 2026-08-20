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
import io.flowwarden.stream.OperationType;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnChange;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.bson.Document;
import org.junit.jupiter.api.Assumptions;
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
 * Integration coverage for {@code @OnChange} on the {@code DROP} and
 * {@code INVALIDATE} operations, wired end-to-end through the imperative
 * stream manager (#15).
 *
 * <p>The handler declares only {@code @OnChange} so both DROP and
 * INVALIDATE — which have no typed handler — fall mechanically into the
 * catch-all per the contract documented on {@link OnChange} since the
 * lib was simplified to a pure catch-all.</p>
 *
 * <p>{@code DROP} is reliably emitted by MongoDB 6 against a
 * collection-level change stream when the watched collection is
 * dropped. {@code INVALIDATE} follows in the same flush in most
 * configurations; in Testcontainers single-node replica sets the
 * INVALIDATE event may or may not surface before the cursor is closed.
 * The INVALIDATE assertion uses a short-timeout
 * {@link Assumptions#abort(String)} fallback so the test stays
 * useful on real clusters while not gating CI on a Testcontainers
 * quirk.</p>
 */
@SpringBootTest(classes = ImperativeOnChangeDropInvalidateIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeOnChangeDropInvalidateIntegrationTest {

    private static final String COLLECTION = "onchange_drop_invalidate";
    private static final String STREAM_NAME = "drop-invalidate-watcher";

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired
    MongoTemplate mongoTemplate;

    @Autowired
    DropInvalidateHandler testHandler;

    @Autowired
    ImperativeStreamManager streamManager;

    @org.junit.jupiter.api.AfterEach
    void tearDown() {
        // The managed restart resurrects the stream after the invalidate:
        // without an explicit stop it stays alive in the cached Spring
        // context (non-daemon container threads) and stalls the Surefire
        // fork's shutdown until its 30s self-kill.
        try {
            streamManager.stopStream(STREAM_NAME);
        } catch (Exception ignored) {
        }
    }

    /**
     * DROP dispatches through {@code @OnChange}; INVALIDATE does NOT — it
     * became a lifecycle-internal event (SPI signal + repair/terminal stop,
     * never dispatched: a manual checkpoint on its unusable token would
     * overwrite the post-invalidate repair).
     */
    @Test
    void dropDispatchesToOnChange_invalidateStaysLifecycleInternal() throws Exception {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(STREAM_NAME));

        // Warm-up insert so the stream cursor has at least one event before
        // the drop, ruling out a "stream never started" false negative.
        mongoTemplate.insert(new Document("status", "WARMUP"), COLLECTION);
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(testHandler.events).isNotEmpty());

        // Drop the watched collection — MongoDB emits a `drop` event.
        mongoTemplate.getCollection(COLLECTION).drop();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(testHandler.events)
                        .anyMatch(ctx -> ctx.getOperationType() == OperationType.DROP));

        ChangeStreamContext<Document> dropCtx = testHandler.events.stream()
                .filter(ctx -> ctx.getOperationType() == OperationType.DROP)
                .findFirst()
                .orElseThrow();
        assertThat(dropCtx.getFullDocument(Document.class))
                .as("DROP carries no fullDocument")
                .isEmpty();

        // The INVALIDATE that follows must never reach application handlers.
        Thread.sleep(2_000);
        assertThat(testHandler.events)
                .as("INVALIDATE is lifecycle-internal — never dispatched to @OnChange")
                .noneMatch(ctx -> ctx.getOperationType() == OperationType.INVALIDATE);
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import(ImperativeOnChangeDropInvalidateIntegrationTest.DropInvalidateHandler.class)
    static class TestApp {
    }

    @ChangeStream(name = STREAM_NAME, collection = COLLECTION)
    static class DropInvalidateHandler {
        final List<ChangeStreamContext<Document>> events = new CopyOnWriteArrayList<>();

        @OnChange
        void onAny(ChangeStreamContext<Document> ctx) {
            events.add(ctx);
        }
    }
}
