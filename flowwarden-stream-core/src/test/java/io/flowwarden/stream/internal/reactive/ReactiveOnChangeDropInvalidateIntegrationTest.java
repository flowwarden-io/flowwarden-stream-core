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
package io.flowwarden.stream.internal.reactive;

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
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Reactive twin of
 * {@code ImperativeOnChangeDropInvalidateIntegrationTest} — same
 * scenario, {@link Mono}-returning handler.
 *
 * <p>See the imperative twin's Javadoc for the rationale on the
 * INVALIDATE Testcontainers caveat.</p>
 */
@SpringBootTest(classes = ReactiveOnChangeDropInvalidateIntegrationTest.TestApp.class)
@ActiveProfiles("test-webflux")
class ReactiveOnChangeDropInvalidateIntegrationTest {

    private static final String COLLECTION = "reactive_onchange_drop_invalidate";
    private static final String STREAM_NAME = "reactive-drop-invalidate-watcher";

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired
    ReactiveMongoTemplate reactiveMongoTemplate;

    @Autowired
    DropInvalidateHandler testHandler;

    @Autowired
    ReactiveStreamManager streamManager;

    @org.junit.jupiter.api.AfterEach
    void tearDown() {
        // The managed restart resurrects the stream after the invalidate —
        // stop it explicitly, see the imperative twin.
        try {
            streamManager.stopStream(STREAM_NAME);
        } catch (Exception ignored) {
        }
    }

    /**
     * DROP dispatches through {@code @OnChange}; INVALIDATE does NOT — it
     * became a lifecycle-internal event (SPI signal + repair/terminal stop,
     * never dispatched). See the imperative twin.
     */
    @Test
    void dropDispatchesToOnChange_invalidateStaysLifecycleInternal() throws Exception {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(STREAM_NAME));

        reactiveMongoTemplate.insert(new Document("status", "WARMUP"), COLLECTION).block();
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(testHandler.events).isNotEmpty());

        reactiveMongoTemplate.dropCollection(COLLECTION).block();

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
    @Import(ReactiveOnChangeDropInvalidateIntegrationTest.DropInvalidateHandler.class)
    static class TestApp {
    }

    @ChangeStream(name = STREAM_NAME, collection = COLLECTION)
    static class DropInvalidateHandler {
        final List<ChangeStreamContext<Document>> events = new CopyOnWriteArrayList<>();

        @OnChange
        Mono<Void> onAny(ChangeStreamContext<Document> ctx) {
            events.add(ctx);
            return Mono.empty();
        }
    }
}
