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
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.spi.CheckpointStore;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import io.flowwarden.stream.test.SharedMongoContainer;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = ReactiveCheckpointIntegrationTest.TestApp.class)
@ActiveProfiles("test-webflux")
class ReactiveCheckpointIntegrationTest {

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired
    ReactiveMongoTemplate reactiveMongoTemplate;

    @Autowired
    CheckpointTestHandler handler;

    @Autowired
    ReactiveStreamManager streamManager;

    @Autowired
    CheckpointStore checkpointStore;

    @BeforeEach
    void setUp() {
        handler.clear();
    }

    @Test
    void resumeAfterRestart() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("checkpoint-reactive-test"));

        int before = handler.getEvents().size();

        // Insert 3 documents
        reactiveMongoTemplate.insert(new Document("item", "A"), "orders").block();
        reactiveMongoTemplate.insert(new Document("item", "B"), "orders").block();
        reactiveMongoTemplate.insert(new Document("item", "C"), "orders").block();

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(handler.getEvents()).hasSizeGreaterThanOrEqualTo(before + 3));

        // Stop the stream
        streamManager.stopStream("checkpoint-reactive-test");
        handler.clear();

        // Insert 2 more documents while stream is stopped
        reactiveMongoTemplate.insert(new Document("item", "D"), "orders").block();
        reactiveMongoTemplate.insert(new Document("item", "E"), "orders").block();

        // Restart the stream — should resume from checkpoint
        streamManager.startStream("checkpoint-reactive-test");

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(handler.getEvents()).hasSizeGreaterThanOrEqualTo(2));

        // Confirm we only got the 2 new events (no replay of A, B, C)
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(3))
                .untilAsserted(() ->
                        assertThat(handler.getEvents()).hasSize(2));
    }

    @Test
    void checkpointIsSavedInMongo() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("checkpoint-reactive-test"));

        int before = handler.getEvents().size();

        reactiveMongoTemplate.insert(new Document("item", "X"), "orders").block();

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(handler.getEvents()).hasSizeGreaterThan(before));

        // Give checkpoint save a moment to complete
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    var checkpoint = checkpointStore.findByStreamName("checkpoint-reactive-test");
                    assertThat(checkpoint).isPresent();
                    assertThat(checkpoint.get().lastProcessedToken()).isNotNull();
                });
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import(ReactiveCheckpointIntegrationTest.CheckpointTestHandler.class)
    static class TestApp {
    }

    @ChangeStream(name = "checkpoint-reactive-test", collection = "orders")
    @Checkpoint(saveEveryN = 1)
    static class CheckpointTestHandler {
        private final List<ChangeStreamContext<?>> events = new CopyOnWriteArrayList<>();

        @OnInsert
        Mono<Void> handle(ChangeStreamContext<?> ctx) {
            events.add(ctx);
            return Mono.empty();
        }

        List<ChangeStreamContext<?>> getEvents() {
            return events;
        }

        void clear() {
            events.clear();
        }
    }
}
