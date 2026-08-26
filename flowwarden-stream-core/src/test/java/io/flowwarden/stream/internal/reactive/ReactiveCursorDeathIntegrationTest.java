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
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.StopReason;
import io.flowwarden.stream.spi.StreamMetricsProvider;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Reactive twin of {@code ImperativeCursorDeathIntegrationTest}: a
 * {@code failCommand} failpoint (appName-scoped) injects a non-resumable 286
 * into the stream's next {@code getMore}, terminating the Flux — previously
 * a permanent, silent death. Covers the managed resubscription and the
 * poisoned-checkpoint case: cascade level 3 applies at resubscribe time with
 * the exact boot semantics (history-lost signal + self-repair).
 */
@SpringBootTest(classes = ReactiveCursorDeathIntegrationTest.TestApp.class)
@ActiveProfiles("test-webflux")
class ReactiveCursorDeathIntegrationTest {

    private static final String APP_NAME = "fw-rx-cursor-death";
    private static final String STREAM = "rx-cursor-death";
    private static final String COLLECTION = "rx_cursor_death";
    private static final String POISON_STREAM = "rx-cursor-death-poison";
    private static final String POISON_COLLECTION = "rx_cursor_death_poison";
    private static final BsonDocument EXPIRED_TOKEN =
            BsonDocument.parse("{\"_data\": \"0000DEAD\"}");

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri",
                () -> SharedMongoContainer.MONGO.getReplicaSetUrl() + "?appName=" + APP_NAME);
    }

    @Autowired ReactiveMongoTemplate reactiveMongoTemplate;
    @Autowired ReactiveStreamManager streamManager;
    @Autowired CheckpointStore checkpointStore;
    @Autowired ReactiveCursorDeathHandler handler;
    @Autowired ReactivePoisonHandler poisonHandler;

    private RecordingMetrics metrics;

    @BeforeEach
    void setUp() {
        disableFailPoint();
        handler.clear();
        poisonHandler.clear();
        reactiveMongoTemplate.dropCollection(COLLECTION)
                .onErrorResume(e -> Mono.empty()).block();
        reactiveMongoTemplate.dropCollection(POISON_COLLECTION)
                .onErrorResume(e -> Mono.empty()).block();
        checkpointStore.delete(STREAM);
        checkpointStore.delete(POISON_STREAM);
        metrics = new RecordingMetrics();
        FlowWardenMetrics.setProvider(metrics);
    }

    @AfterEach
    void tearDown() {
        disableFailPoint();
        try { streamManager.stopStream(STREAM); } catch (Exception ignored) { }
        try { streamManager.stopStream(POISON_STREAM); } catch (Exception ignored) { }
        checkpointStore.delete(STREAM);
        checkpointStore.delete(POISON_STREAM);
        FlowWardenMetrics.setProvider(StreamMetricsProvider.noOp());
    }

    @Test
    void cursorDeath_isObserved_streamResubscribesThroughCascade_andKeepsDelivering() {
        streamManager.startStream(STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(STREAM));

        reactiveMongoTemplate.insert(new Document("seq", 1), COLLECTION).block();
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(handler.count()).isEqualTo(1));

        enableFailPointOnce();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(metrics.stops)
                        .as("the reactive pipeline termination must be surfaced")
                        .anyMatch(s -> s.startsWith(STREAM + ":CRASHED")));

        // Down window (the ≥1s backoff before the first attempt): the dead
        // stream must leave NOTHING behind — the lingering checkpoint timer
        // of a dead reactive stream is what masked the original frozen
        // checkpoint for days.
        assertThat(streamManager.hasLatestToken(STREAM)).isFalse();
        assertThat(streamManager.hasIntervalTask(STREAM)).isFalse();
        assertThat(streamManager.hasHeartbeat(STREAM)).isFalse();
        assertThat(streamManager.isRestartPending(STREAM)).isTrue();

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(metrics.restarts)
                        .as("previously: permanent silent death — now a managed resubscribe")
                        .anyMatch(s -> s.startsWith(STREAM + ":1:")));
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(STREAM));

        reactiveMongoTemplate.insert(new Document("seq", 2), COLLECTION).block();
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(handler.count()).isEqualTo(2));
    }

    @Test
    void poisonedCheckpointAtResubscribe_appliesCascadeLevel3_withBootSemantics() {
        streamManager.startStream(POISON_STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(POISON_STREAM));
        reactiveMongoTemplate.insert(new Document("seq", 1), POISON_COLLECTION).block();
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(poisonHandler.count()).isEqualTo(1));
        // Wait for the settled event to be anchored: a pending dirty anchor
        // write after the poison write would overwrite it with a valid
        // position and mask the level-3 scenario.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(checkpointStore.findByStreamName(POISON_STREAM)
                        .orElseThrow().lastProcessedToken()).isNotNull());

        // Poison the checkpoint (the running stream never re-reads it), then
        // kill the cursor: the resubscribe cascade finds both tokens dead and
        // must apply level 3 exactly like a boot — history-lost signal, then
        // the RESUME_FROM_NOW self-repair (fresh certified seen, dead
        // processed cleared).
        Instant past = Instant.now().minusSeconds(86_400);
        checkpointStore.save(new io.flowwarden.stream.spi.Checkpoint(
                POISON_STREAM, null, EXPIRED_TOKEN, past, EXPIRED_TOKEN, past,
                Collections.emptyMap()));
        enableFailPointOnce();

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(metrics.restarts).anyMatch(s -> s.startsWith(POISON_STREAM + ":")));
        assertThat(metrics.historyLostCount.get())
                .as("cascade level 3 must fire at resubscribe time")
                .isEqualTo(1);
        var repaired = checkpointStore.findByStreamName(POISON_STREAM).orElseThrow();
        assertThat(repaired.lastSeenToken()).isNotNull().isNotEqualTo(EXPIRED_TOKEN);
        assertThat(repaired.lastProcessedToken())
                .as("the self-repair clears the dead processed token")
                .isNull();

        // Delivery continues on the repaired stream.
        reactiveMongoTemplate.insert(new Document("seq", 2), POISON_COLLECTION).block();
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(poisonHandler.count()).isEqualTo(2));
    }

    @Test
    void deathHandoffRacingOperatorStop_stopIsTheLastLifecycleOwner() throws Exception {
        // Round 4 blocker, reactive twin: the doFinally death transition is
        // lifecycle-locked — a stop racing a suspended hand-off waits for it
        // and kills the just-armed restart.
        streamManager.startStream(STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(STREAM));
        reactiveMongoTemplate.insert(new Document("seq", 1), COLLECTION).block();
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(handler.count()).isEqualTo(1));

        java.util.concurrent.CountDownLatch hookEntered = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch hookGate = new java.util.concurrent.CountDownLatch(1);
        streamManager.deathHandoffTestHook = () -> {
            hookEntered.countDown();
            try {
                hookGate.await(10, java.util.concurrent.TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
        try {
            enableFailPointOnce();
            assertThat(hookEntered.await(10, java.util.concurrent.TimeUnit.SECONDS))
                    .as("the death transition reached the hand-off point")
                    .isTrue();

            Thread stopThread = new Thread(() -> streamManager.stopStream(STREAM));
            stopThread.start();
            Thread.sleep(300);
            assertThat(stopThread.isAlive())
                    .as("the stop waits for the death transition instead of racing it")
                    .isTrue();

            hookGate.countDown();
            stopThread.join(5_000);
        } finally {
            streamManager.deathHandoffTestHook = null;
            hookGate.countDown();
        }

        assertThat(streamManager.isRestartPending(STREAM))
                .as("the operator stop is the last lifecycle owner")
                .isFalse();
        Thread.sleep(2_500); // past the would-be attempt-1 backoff
        assertThat(streamManager.isRunning(STREAM)).isFalse();
        assertThat(metrics.restarts).noneMatch(s -> s.startsWith(STREAM + ":"));
    }

    @Test
    void throwingMetricsProvider_neverBlocksCleanupNorRestart() {
        // Review round 1: a StreamMetricsProvider is external SPI code — a
        // throw from onStreamStopped inside doFinally must not skip the
        // eviction nor the restart hand-off (lifecycle first, observables
        // after, same hardening class as the election coordinator).
        metrics.throwOnStops = true;
        streamManager.startStream(STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(STREAM));
        reactiveMongoTemplate.insert(new Document("seq", 1), COLLECTION).block();
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(handler.count()).isEqualTo(1));

        enableFailPointOnce();

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(metrics.restarts)
                        .as("the restart must be scheduled despite the throwing provider")
                        .anyMatch(s -> s.startsWith(STREAM + ":")));
        assertThat(streamManager.hasIntervalTask(STREAM) && !streamManager.isRunning(STREAM))
                .as("no zombie timer may survive the throwing provider")
                .isFalse();

        reactiveMongoTemplate.insert(new Document("seq", 2), COLLECTION).block();
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(handler.count()).isEqualTo(2));
    }

    private void enableFailPointOnce() {
        adminCommand(new Document("configureFailPoint", "failCommand")
                .append("mode", new Document("times", 1))
                .append("data", new Document("errorCode", 286)
                        .append("failCommands", List.of("getMore"))
                        .append("appName", APP_NAME)));
    }

    private void disableFailPoint() {
        try {
            adminCommand(new Document("configureFailPoint", "failCommand")
                    .append("mode", "off"));
        } catch (Exception ignored) {
            // best-effort cleanup
        }
    }

    private void adminCommand(Document command) {
        reactiveMongoTemplate.getMongoDatabaseFactory().getMongoDatabase("admin")
                .flatMapMany(db -> db.runCommand(command))
                .blockFirst(Duration.ofSeconds(5));
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import({ReactiveCursorDeathHandler.class, ReactivePoisonHandler.class})
    static class TestApp {}

    @ChangeStream(name = STREAM, collection = COLLECTION,
            documentType = Document.class, autoStart = false)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1, idleHeartbeatIntervalSeconds = 0,
            onHistoryLost = OnHistoryLost.RESUME_FROM_NOW)
    static class ReactiveCursorDeathHandler {

        private final List<Document> events = new CopyOnWriteArrayList<>();

        @OnInsert
        Mono<Void> handle(ChangeStreamContext<Document> ctx) {
            ctx.getFullDocument(Document.class).ifPresent(events::add);
            return Mono.empty();
        }

        int count() { return events.size(); }
        void clear() { events.clear(); }
    }

    @ChangeStream(name = POISON_STREAM, collection = POISON_COLLECTION,
            documentType = Document.class, autoStart = false)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1, idleHeartbeatIntervalSeconds = 0,
            onHistoryLost = OnHistoryLost.RESUME_FROM_NOW)
    static class ReactivePoisonHandler {

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
        final AtomicInteger historyLostCount = new AtomicInteger();
        volatile boolean throwOnStops;

        @Override
        public void onStreamStopped(String streamName, StopReason reason, Throwable cause) {
            stops.add(streamName + ":" + reason + ":"
                    + (cause != null ? cause.getClass().getSimpleName() : "null"));
            if (throwOnStops) {
                throw new RuntimeException("provider boom on stop");
            }
        }

        @Override
        public void onStreamRestarted(String streamName, int attempt, Throwable cause) {
            restarts.add(streamName + ":" + attempt + ":"
                    + (cause != null ? cause.getClass().getSimpleName() : "null"));
        }

        @Override
        public void onResumeHistoryLost(String streamName) {
            historyLostCount.incrementAndGet();
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
