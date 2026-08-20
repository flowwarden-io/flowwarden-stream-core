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
import io.flowwarden.stream.FlowWardenMetrics;
import io.flowwarden.stream.OnHistoryLost;
import io.flowwarden.stream.OperationType;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.StopReason;
import io.flowwarden.stream.spi.StreamMetricsProvider;
import io.flowwarden.stream.test.SharedMongoContainer;
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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Reactive twin of {@code ImperativeInvalidateIntegrationTest}. On the
 * reactive side the invalidate terminates the Flux (an unexpected
 * completion), so the death travels the existing {@code doFinally} path —
 * the invalidate-specific work is the classification, the signal and the
 * synchronous checkpoint self-repair before that termination commits.
 */
@SpringBootTest(classes = ReactiveInvalidateIntegrationTest.TestApp.class)
@ActiveProfiles("test-webflux")
class ReactiveInvalidateIntegrationTest {

    private static final String DROP_STREAM = "rx-invalidate-drop";
    private static final String DROP_COLLECTION = "rx_invalidate_drop";
    private static final String RENAME_STREAM = "rx-invalidate-rename";
    private static final String RENAME_COLLECTION = "rx_invalidate_rename";
    private static final String RENAME_TARGET = "rx_invalidate_rename_target";

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired ReactiveMongoTemplate reactiveMongoTemplate;
    @Autowired ReactiveStreamManager streamManager;
    @Autowired CheckpointStore checkpointStore;
    @Autowired ReactiveDropHandler dropHandler;
    @Autowired ReactiveRenameHandler renameHandler;

    private RecordingMetrics metrics;

    @BeforeEach
    void setUp() {
        dropHandler.clear();
        renameHandler.clear();
        reactiveMongoTemplate.dropCollection(DROP_COLLECTION)
                .onErrorResume(e -> Mono.empty()).block();
        reactiveMongoTemplate.dropCollection(RENAME_COLLECTION)
                .onErrorResume(e -> Mono.empty()).block();
        reactiveMongoTemplate.dropCollection(RENAME_TARGET)
                .onErrorResume(e -> Mono.empty()).block();
        checkpointStore.delete(DROP_STREAM);
        checkpointStore.delete(RENAME_STREAM);
        metrics = new RecordingMetrics();
        FlowWardenMetrics.setProvider(metrics);
    }

    @AfterEach
    void tearDown() {
        try { streamManager.stopStream(DROP_STREAM); } catch (Exception ignored) { }
        try { streamManager.stopStream(RENAME_STREAM); } catch (Exception ignored) { }
        checkpointStore.delete(DROP_STREAM);
        checkpointStore.delete(RENAME_STREAM);
        FlowWardenMetrics.setProvider(StreamMetricsProvider.noOp());
    }

    @Test
    void collectionDrop_underSelfRepairingStrategy_surfacesAndSelfHeals() {
        streamManager.startStream(DROP_STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(DROP_STREAM));
        reactiveMongoTemplate.insert(new Document("seq", 1), DROP_COLLECTION).block();
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(dropHandler.count()).isEqualTo(1));

        reactiveMongoTemplate.dropCollection(DROP_COLLECTION).block();

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(metrics.invalidations)
                        .contains(DROP_STREAM + ":DROP"));
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(metrics.restarts)
                        .anyMatch(s -> s.startsWith(DROP_STREAM + ":")));
        await().atMost(Duration.ofSeconds(10))
                .until(() -> streamManager.isRunning(DROP_STREAM));

        reactiveMongoTemplate.insert(new Document("seq", 2), DROP_COLLECTION).block();
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(dropHandler.count()).isEqualTo(2));
    }

    @Test
    void collectionRename_isTerminal_noAutomaticRestart() throws Exception {
        streamManager.startStream(RENAME_STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(RENAME_STREAM));
        reactiveMongoTemplate.insert(new Document("seq", 1), RENAME_COLLECTION).block();
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(renameHandler.count()).isEqualTo(1));

        String db = reactiveMongoTemplate.getMongoDatabase().block().getName();
        reactiveMongoTemplate.getMongoDatabaseFactory().getMongoDatabase("admin")
                .flatMapMany(admin -> admin.runCommand(
                        new Document("renameCollection", db + "." + RENAME_COLLECTION)
                                .append("to", db + "." + RENAME_TARGET)))
                .blockFirst(Duration.ofSeconds(5));

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(metrics.invalidations)
                        .contains(RENAME_STREAM + ":RENAME"));
        await().atMost(Duration.ofSeconds(10))
                .until(() -> !streamManager.isRunning(RENAME_STREAM));

        Thread.sleep(3_000);
        assertThat(streamManager.isRestartPending(RENAME_STREAM)).isFalse();
        assertThat(streamManager.isRunning(RENAME_STREAM)).isFalse();
        assertThat(metrics.restarts)
                .noneMatch(s -> s.startsWith(RENAME_STREAM + ":"));
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import({ReactiveDropHandler.class, ReactiveRenameHandler.class})
    static class TestApp {}

    @ChangeStream(name = DROP_STREAM, collection = DROP_COLLECTION,
            documentType = Document.class, autoStart = false)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1, idleHeartbeatIntervalSeconds = 0,
            onHistoryLost = OnHistoryLost.RESUME_FROM_NOW)
    static class ReactiveDropHandler {

        private final List<Document> events = new CopyOnWriteArrayList<>();

        @OnInsert
        Mono<Void> handle(ChangeStreamContext<Document> ctx) {
            ctx.getFullDocument(Document.class).ifPresent(events::add);
            return Mono.empty();
        }

        int count() { return events.size(); }
        void clear() { events.clear(); }
    }

    @ChangeStream(name = RENAME_STREAM, collection = RENAME_COLLECTION,
            documentType = Document.class, autoStart = false)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1, idleHeartbeatIntervalSeconds = 0,
            onHistoryLost = OnHistoryLost.RESUME_FROM_NOW)
    static class ReactiveRenameHandler {

        private final List<Document> events = new CopyOnWriteArrayList<>();

        @OnInsert
        Mono<Void> handle(ChangeStreamContext<Document> ctx) {
            ctx.getFullDocument(Document.class).ifPresent(events::add);
            return Mono.empty();
        }

        int count() { return events.size(); }
        void clear() { events.clear(); }
    }

    private static final class RecordingMetrics implements StreamMetricsProvider {
        final List<String> stops = new CopyOnWriteArrayList<>();
        final List<String> restarts = new CopyOnWriteArrayList<>();
        final List<String> invalidations = new CopyOnWriteArrayList<>();

        @Override
        public void onStreamStopped(String streamName, StopReason reason, Throwable cause) {
            stops.add(streamName + ":" + reason + ":"
                    + (cause != null ? cause.getClass().getSimpleName() : "null"));
        }

        @Override
        public void onStreamRestarted(String streamName, int attempt, Throwable cause) {
            restarts.add(streamName + ":" + attempt);
        }

        @Override
        public void onStreamInvalidated(String streamName, OperationType cause) {
            invalidations.add(streamName + ":" + cause);
        }

        @Override
        public void onStreamStarted(String streamName,
                io.flowwarden.stream.spi.StreamConfiguration config) {
        }

        @Override
        public void onEventReceived(String streamName,
                io.flowwarden.stream.spi.ChangeEventMetadata metadata) {
        }

        @Override
        public void onEventProcessed(String streamName, long durationNanos, boolean success) {
        }

        @Override
        public void onEventError(String streamName, Throwable error, boolean willRetry,
                int attemptNumber, io.flowwarden.stream.spi.ChangeEventMetadata metadata) {
        }

        @Override
        public void onCheckpoint(String streamName, String resumeToken) {
        }

        @Override
        public void onBufferStatus(String streamName, int currentSize, int maxSize) {
        }

        @Override
        public void onBackpressure(String streamName,
                io.flowwarden.stream.spi.BackpressureAction action) {
        }

        @Override
        public void onEventSentToDlq(String streamName) {
        }

        @Override
        public void onOplogStats(double logLengthHours, String status) {
        }
    }
}
