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
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = ReactiveSaveIntervalIntegrationTest.TestApp.class)
@ActiveProfiles("test-webflux")
class ReactiveSaveIntervalIntegrationTest {

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired
    ReactiveMongoTemplate reactiveMongoTemplate;

    @Autowired
    IntervalTestHandler handler;

    @Autowired
    ReactiveStreamManager streamManager;

    @Autowired
    CheckpointStore checkpointStore;

    @BeforeEach
    void setUp() {
        handler.clear();
    }

    @Test
    void periodicSaveWithoutManyEvents() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("interval-reactive-test"));

        // Insert 1 doc — not enough for the count threshold (saveEveryN=100).
        // The time threshold (saveIntervalSeconds=2) is responsible for
        // anchoring the settled token so an active-but-slow stream stays
        // recoverable even when the counter rarely triggers.
        reactiveMongoTemplate.insert(new Document("item", "A"), "interval_orders").block();

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() ->
                        assertThat(handler.getEvents()).hasSizeGreaterThanOrEqualTo(1));

        // Wait for the periodic timer (2s interval) to persist the anchor
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    var checkpoint = checkpointStore.findByStreamName("interval-reactive-test");
                    assertThat(checkpoint).isPresent();
                    assertThat(checkpoint.get().lastProcessedToken()).isNotNull();
                    assertThat(checkpoint.get().lastHeartbeatTimestamp())
                            .as("an anchor write confirms the recoverable position")
                            .isNotNull();
                });
    }

    @Test
    void checkpointUpdatedPeriodically() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("interval-reactive-test"));

        // Insert 1 doc to seed the token tracker
        reactiveMongoTemplate.insert(new Document("item", "B"), "interval_orders").block();

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() ->
                        assertThat(handler.getEvents()).hasSizeGreaterThanOrEqualTo(1));

        // Wait for periodic checkpoint (timer anchors lastProcessedToken)
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    var cp = checkpointStore.findByStreamName("interval-reactive-test");
                    assertThat(cp).isPresent();
                    assertThat(cp.get().lastProcessedToken()).isNotNull();
                });

        Instant firstProcessedTimestamp = checkpointStore.findByStreamName("interval-reactive-test")
                .get().lastProcessedTimestamp();
        assertThat(firstProcessedTimestamp).isNotNull();

        // Insert another doc so the timer has fresher input to persist
        reactiveMongoTemplate.insert(new Document("item", "C"), "interval_orders").block();

        // Wait for a second periodic save — lastProcessedTimestamp advances
        await().atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    var cp = checkpointStore.findByStreamName("interval-reactive-test");
                    assertThat(cp).isPresent();
                    assertThat(cp.get().lastProcessedTimestamp())
                            .isAfterOrEqualTo(firstProcessedTimestamp);
                });
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import(ReactiveSaveIntervalIntegrationTest.IntervalTestHandler.class)
    static class TestApp {
    }

    @ChangeStream(name = "interval-reactive-test", collection = "interval_orders")
    @Checkpoint(saveEveryN = 100, saveIntervalSeconds = 2)
    static class IntervalTestHandler {
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
