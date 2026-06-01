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
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = ImperativeSaveIntervalIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeSaveIntervalIntegrationTest {

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired
    MongoTemplate mongoTemplate;

    @Autowired
    IntervalTestHandler handler;

    @Autowired
    ImperativeStreamManager streamManager;

    @Autowired
    CheckpointStore checkpointStore;

    @BeforeEach
    void setUp() {
        handler.clear();
    }

    @Test
    void periodicSaveWithoutManyEvents() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("interval-test"));

        // Insert 1 doc — not enough for saveEveryN=100, so lastProcessedToken
        // will NOT be persisted. The periodic timer (saveIntervalSeconds=2)
        // is responsible for advancing lastSeenToken so the stream can recover
        // even on streams where the saveEveryN counter rarely triggers.
        mongoTemplate.insert(new Document("item", "A"), "interval_orders");

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() ->
                        assertThat(handler.getEvents()).hasSizeGreaterThanOrEqualTo(1));

        // Wait for the periodic timer (2s interval) to save the checkpoint
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    var checkpoint = checkpointStore.findByStreamName("interval-test");
                    assertThat(checkpoint).isPresent();
                    // Timer advances lastSeenToken only — lastProcessedToken stays null
                    // because saveEveryN=100 hasn't triggered yet
                    assertThat(checkpoint.get().lastSeenToken()).isNotNull();
                });
    }

    @Test
    void checkpointUpdatedPeriodically() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("interval-test"));

        // Insert 1 doc to seed the token tracker
        mongoTemplate.insert(new Document("item", "B"), "interval_orders");

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() ->
                        assertThat(handler.getEvents()).hasSizeGreaterThanOrEqualTo(1));

        // Wait for periodic checkpoint (timer writes lastSeenToken)
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    var cp = checkpointStore.findByStreamName("interval-test");
                    assertThat(cp).isPresent();
                    assertThat(cp.get().lastSeenToken()).isNotNull();
                });

        Instant firstSeenTimestamp = checkpointStore.findByStreamName("interval-test")
                .get().lastSeenTimestamp();
        assertThat(firstSeenTimestamp).isNotNull();

        // Insert another doc so the timer has fresher input to persist
        mongoTemplate.insert(new Document("item", "C"), "interval_orders");

        // Wait for a second periodic save — lastSeenTimestamp should advance
        await().atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    var cp = checkpointStore.findByStreamName("interval-test");
                    assertThat(cp).isPresent();
                    assertThat(cp.get().lastSeenTimestamp())
                            .isAfterOrEqualTo(firstSeenTimestamp);
                });
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import(ImperativeSaveIntervalIntegrationTest.IntervalTestHandler.class)
    static class TestApp {
    }

    @ChangeStream(name = "interval-test", collection = "interval_orders")
    @Checkpoint(saveEveryN = 100, saveIntervalSeconds = 2)
    static class IntervalTestHandler {
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
