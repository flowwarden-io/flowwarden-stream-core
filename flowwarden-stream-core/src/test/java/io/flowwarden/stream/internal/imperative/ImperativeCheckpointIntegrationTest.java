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
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.spi.CheckpointStore;
import org.bson.Document;
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
import io.flowwarden.stream.test.SharedMongoContainer;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = ImperativeCheckpointIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeCheckpointIntegrationTest {

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired
    MongoTemplate mongoTemplate;

    @Autowired
    CheckpointTestHandler handler;

    @Autowired
    ImperativeStreamManager streamManager;

    @Autowired
    CheckpointStore checkpointStore;

    @BeforeEach
    void setUp() {
        handler.clear();
    }

    @Test
    void resumeAfterRestart() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("checkpoint-test"));

        int before = handler.getEvents().size();

        // Insert 3 documents
        mongoTemplate.insert(new Document("item", "A"), "orders");
        mongoTemplate.insert(new Document("item", "B"), "orders");
        mongoTemplate.insert(new Document("item", "C"), "orders");

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(handler.getEvents()).hasSizeGreaterThanOrEqualTo(before + 3));

        // Stop the stream
        streamManager.stopStream("checkpoint-test");
        handler.clear();

        // Insert 2 more documents while stream is stopped
        mongoTemplate.insert(new Document("item", "D"), "orders");
        mongoTemplate.insert(new Document("item", "E"), "orders");

        // Restart the stream — should resume from checkpoint
        streamManager.startStream("checkpoint-test");

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
                .until(() -> streamManager.isRunning("checkpoint-test"));

        int before = handler.getEvents().size();

        mongoTemplate.insert(new Document("item", "X"), "orders");

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(handler.getEvents()).hasSizeGreaterThan(before));

        // Give checkpoint save a moment to complete
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    var checkpoint = checkpointStore.findByStreamName("checkpoint-test");
                    assertThat(checkpoint).isPresent();
                    assertThat(checkpoint.get().lastProcessedToken()).isNotNull();
                });
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import(ImperativeCheckpointIntegrationTest.CheckpointTestHandler.class)
    static class TestApp {
    }

    @ChangeStream(name = "checkpoint-test", collection = "orders")
    @Checkpoint(saveEveryN = 1)
    static class CheckpointTestHandler {
        private final List<ChangeStreamContext<?>> events = new CopyOnWriteArrayList<>();

        @OnInsert
        void handle(ChangeStreamContext<?> ctx) {
            events.add(ctx);
        }

        List<ChangeStreamContext<?>> getEvents() {
            return events;
        }

        void clear() {
            events.clear();
        }
    }
}
