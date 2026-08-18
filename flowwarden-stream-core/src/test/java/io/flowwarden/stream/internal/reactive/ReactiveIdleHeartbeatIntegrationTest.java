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
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Reactive twin of the idle-stream heartbeat regression tests: with zero
 * events on the watched collection, {@code lastSeenToken} must keep tracking
 * the oplog head and {@code lastHeartbeatTimestamp} must stay fresh.
 */
@SpringBootTest(classes = ReactiveIdleHeartbeatIntegrationTest.TestApp.class)
@ActiveProfiles("test-webflux")
class ReactiveIdleHeartbeatIntegrationTest {

    private static final String STREAM_NAME = "idle-heartbeat-reactive";
    private static final String COLLECTION = "idle_heartbeat_reactive";
    private static final String OTHER_COLLECTION = "idle_heartbeat_reactive_other";

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired ReactiveMongoTemplate reactiveMongoTemplate;
    @Autowired ReactiveStreamManager streamManager;
    @Autowired CheckpointStore checkpointStore;
    @Autowired ReactiveIdleHandler handler;

    @BeforeEach
    void setUp() {
        handler.clear();
        reactiveMongoTemplate.dropCollection(COLLECTION).onErrorResume(e -> Mono.empty()).block();
        reactiveMongoTemplate.dropCollection(OTHER_COLLECTION).onErrorResume(e -> Mono.empty()).block();
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
    void idleStream_zeroWrites_heartbeatFreshAndPositionBootstrapped() {
        Instant testStart = Instant.now();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var cp = checkpointStore.findByStreamName(STREAM_NAME).orElseThrow();
            assertThat(cp.lastSeenToken())
                    .as("bootstrap must persist a position before any event")
                    .isNotNull();
            assertThat(cp.lastHeartbeatTimestamp())
                    .as("heartbeat must confirm the position without traffic")
                    .isAfterOrEqualTo(testStart);
        });

        Instant firstHeartbeat = checkpointStore.findByStreamName(STREAM_NAME)
                .orElseThrow().lastHeartbeatTimestamp();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var cp = checkpointStore.findByStreamName(STREAM_NAME).orElseThrow();
            assertThat(cp.lastHeartbeatTimestamp()).isAfter(firstHeartbeat);
        });
    }

    @Test
    void idleStream_oplogAdvancedElsewhere_tokenProgresses() {
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(checkpointStore.findByStreamName(STREAM_NAME)
                        .orElseThrow().lastSeenToken()).isNotNull());
        BsonDocument initialToken = checkpointStore.findByStreamName(STREAM_NAME)
                .orElseThrow().lastSeenToken();

        for (int i = 0; i < 20; i++) {
            reactiveMongoTemplate.insert(new Document("filler", i), OTHER_COLLECTION).block();
        }

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var cp = checkpointStore.findByStreamName(STREAM_NAME).orElseThrow();
            assertThat(cp.lastSeenToken())
                    .as("the heartbeat must surf the oplog head even with zero "
                            + "events on the watched collection")
                    .isNotEqualTo(initialToken);
        });
    }

    @Test
    void restartAfterIdlePeriod_resumesFromHeartbeatedToken_andDeliversNextEvent() {
        for (int i = 0; i < 10; i++) {
            reactiveMongoTemplate.insert(new Document("filler", i), OTHER_COLLECTION).block();
        }
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(checkpointStore.findByStreamName(STREAM_NAME)
                        .orElseThrow().lastSeenToken()).isNotNull());

        streamManager.stopStream(STREAM_NAME);

        reactiveMongoTemplate.insert(new Document("type", "missed-while-down"), COLLECTION).block();

        streamManager.startStream(STREAM_NAME);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(STREAM_NAME));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(handler.docs()).extracting(d -> d.getString("type"))
                        .contains("missed-while-down"));
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import(ReactiveIdleHandler.class)
    static class TestApp {}

    @ChangeStream(name = STREAM_NAME, collection = COLLECTION,
            documentType = Document.class, autoStart = false)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1, idleHeartbeatIntervalSeconds = 1)
    static class ReactiveIdleHandler {

        private final List<Document> docs = new CopyOnWriteArrayList<>();

        @OnInsert
        Mono<Void> handle(ChangeStreamContext<Document> ctx) {
            ctx.getFullDocument(Document.class).ifPresent(docs::add);
            return Mono.empty();
        }

        List<Document> docs() { return docs; }
        void clear() { docs.clear(); }
    }
}
