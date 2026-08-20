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
    @Autowired ReactiveCatchUpOptOutHandler catchUpOptOutHandler;
    @Autowired ReactiveOplogStartHandler oplogStartHandler;
    @Autowired ReactiveRedeliveryHandler redeliveryHandler;

    @BeforeEach
    void setUp() {
        handler.clear();
        catchUpOptOutHandler.clear();
        oplogStartHandler.clear();
        redeliveryHandler.reset();
        reactiveMongoTemplate.dropCollection(OPLOG_COLLECTION)
                .onErrorResume(e -> Mono.empty()).block();
        checkpointStore.delete(OPLOG_STREAM);
        reactiveMongoTemplate.dropCollection(COLLECTION).onErrorResume(e -> Mono.empty()).block();
        reactiveMongoTemplate.dropCollection(OTHER_COLLECTION).onErrorResume(e -> Mono.empty()).block();
        reactiveMongoTemplate.dropCollection(CATCHUP_OPTOUT_COLLECTION)
                .onErrorResume(e -> Mono.empty()).block();
        reactiveMongoTemplate.dropCollection(REDELIVERY_COLLECTION)
                .onErrorResume(e -> Mono.empty()).block();
        checkpointStore.delete(CATCHUP_OPTOUT_STREAM);
        checkpointStore.delete(REDELIVERY_STREAM);
        checkpointStore.delete(STREAM_NAME);
        streamManager.startStream(STREAM_NAME);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(STREAM_NAME));
    }

    @AfterEach
    void tearDown() {
        try { streamManager.stopStream(STREAM_NAME); } catch (Exception ignored) {}
        try { streamManager.stopStream(CATCHUP_OPTOUT_STREAM); } catch (Exception ignored) {}
        try { streamManager.stopStream(REDELIVERY_STREAM); } catch (Exception ignored) {}
        checkpointStore.delete(STREAM_NAME);
        checkpointStore.delete(CATCHUP_OPTOUT_STREAM);
        checkpointStore.delete(REDELIVERY_STREAM);
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

    @Test
    void divergenceWithIdleProbingOptedOut_catchUpStillCompletes_flushKeepsWorking() {
        // Reactive twin of the imperative catch-up/opt-out regression: the
        // scheduling logic is duplicated per manager, so is the coverage.
        BsonDocument processedPos = currentPosition(CATCHUP_OPTOUT_COLLECTION);
        reactiveMongoTemplate.insert(new Document("tag", "replayed"), CATCHUP_OPTOUT_COLLECTION)
                .block();
        BsonDocument seenPos = currentPosition(CATCHUP_OPTOUT_COLLECTION);

        Instant savedAt = Instant.now();
        checkpointStore.save(new io.flowwarden.stream.spi.Checkpoint(
                CATCHUP_OPTOUT_STREAM, null, seenPos, savedAt, processedPos, savedAt, savedAt,
                java.util.Collections.emptyMap()));

        streamManager.startStream(CATCHUP_OPTOUT_STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(CATCHUP_OPTOUT_STREAM));

        // Catch-up certification observable through the heartbeat timestamp
        // (the certified PBRT may be byte-identical to seenPos on a quiet
        // cluster — the token itself is not a reliable certification signal).
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(catchUpOptOutHandler.tags()).contains("replayed");
            assertThat(checkpointStore.findByStreamName(CATCHUP_OPTOUT_STREAM)
                    .orElseThrow().lastHeartbeatTimestamp()).isAfter(savedAt);
        });
        BsonDocument certifiedToken = checkpointStore
                .findByStreamName(CATCHUP_OPTOUT_STREAM).orElseThrow().lastSeenToken();

        // With idle=0 and the chain finished, only the flush can move the
        // position again.
        reactiveMongoTemplate.insert(new Document("tag", "live"), CATCHUP_OPTOUT_COLLECTION)
                .block();
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(catchUpOptOutHandler.tags()).contains("live");
            assertThat(checkpointStore.findByStreamName(CATCHUP_OPTOUT_STREAM)
                    .orElseThrow().lastSeenToken())
                    .as("the flush must persist events again once catch-up completed")
                    .isNotEqualTo(certifiedToken);
        });

        streamManager.stopStream(CATCHUP_OPTOUT_STREAM);
        checkpointStore.delete(CATCHUP_OPTOUT_STREAM);
    }

    /**
     * Captures the server's current position from a change stream's initial
     * aggregate reply — the exact mechanism the bootstrap uses.
     */
    private BsonDocument currentPosition(String collection) {
        Document reply = reactiveMongoTemplate.executeCommand(
                new Document("aggregate", collection)
                        .append("pipeline", java.util.List.of(
                                new Document("$changeStream", new Document())))
                        .append("cursor", new Document("batchSize", 1)))
                .block(Duration.ofSeconds(5));
        assertThat(reply).isNotNull();
        Document cursor = reply.get("cursor", Document.class);
        long cursorId = ((Number) cursor.get("id")).longValue();
        Document pbrt = cursor.get("postBatchResumeToken", Document.class);
        if (cursorId != 0) {
            reactiveMongoTemplate.executeCommand(new Document("killCursors", collection)
                    .append("cursors", java.util.List.of(cursorId)))
                    .block(Duration.ofSeconds(5));
        }
        assertThat(pbrt).isNotNull();
        return BsonDocument.parse(pbrt.toJson());
    }

    @Test
    void expiredCheckpoint_oplogStart_selfRepairs_secondRestartIsClean() {
        // Reactive twin of the OPLOG_START self-repair: the reactive
        // probeFromOperationTime is a distinct raw-command path.
        var recordingMetrics = new RecordingResumeMetrics();
        io.flowwarden.stream.FlowWardenMetrics.setProvider(recordingMetrics);
        try {
            Instant past = Instant.now().minusSeconds(86_400);
            BsonDocument expired = BsonDocument.parse("{\"_data\": \"0000DEAD\"}");
            checkpointStore.save(new io.flowwarden.stream.spi.Checkpoint(
                    OPLOG_STREAM, null, expired, past, expired, past,
                    java.util.Collections.emptyMap()));

            streamManager.startStream(OPLOG_STREAM);
            await().atMost(Duration.ofSeconds(5))
                    .until(() -> streamManager.isRunning(OPLOG_STREAM));

            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                var cp = checkpointStore.findByStreamName(OPLOG_STREAM).orElseThrow();
                assertThat(cp.lastProcessedToken()).isNull();
                assertThat(cp.lastSeenToken())
                        .as("the establishment chain must produce a durable fresh position")
                        .isNotNull()
                        .isNotEqualTo(expired);
            });

            streamManager.stopStream(OPLOG_STREAM);
            recordingMetrics.historyLostCount.set(0);
            streamManager.startStream(OPLOG_STREAM);
            await().atMost(Duration.ofSeconds(5))
                    .until(() -> streamManager.isRunning(OPLOG_STREAM));
            assertThat(recordingMetrics.historyLostCount.get())
                    .as("the second restart must not replay the history-lost path")
                    .isZero();

            reactiveMongoTemplate.insert(
                    new Document("tag", "after-repair"), OPLOG_COLLECTION).block();
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(oplogStartHandler.tags()).contains("after-repair"));
        } finally {
            streamManager.stopStream(OPLOG_STREAM);
            checkpointStore.delete(OPLOG_STREAM);
            io.flowwarden.stream.FlowWardenMetrics.setProvider(
                    io.flowwarden.stream.spi.StreamMetricsProvider.noOp());
        }
    }

    @Test
    void oplogStartRecovery_inFlightHandlerAtCrash_eventIsRedeliveredOnRestart() throws Exception {
        // Reactive twin of the settled-token at-least-once test: the first
        // delivery's Mono never completes, so the event never settles and
        // recovery must not certify past it. The "crash" is the stream
        // disposal cancelling the pending Mono — cancellation is not a
        // terminal outcome, nothing may settle from it.
        Instant past = Instant.now().minusSeconds(86_400);
        BsonDocument expired = BsonDocument.parse("{\"_data\": \"0000DEAD\"}");
        checkpointStore.save(new io.flowwarden.stream.spi.Checkpoint(
                REDELIVERY_STREAM, null, expired, past, expired, past,
                java.util.Collections.emptyMap()));
        // The event belongs to the replay the recovery promises to deliver.
        reactiveMongoTemplate.insert(new Document("seq", 1), REDELIVERY_COLLECTION).block();

        streamManager.startStream(REDELIVERY_STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(REDELIVERY_STREAM));
        assertThat(redeliveryHandler.awaitEntered(10)).isTrue();

        // Longer than one chain retry (5s): several establishment attempts
        // ran against the unsettled event — the durable marker must survive.
        Thread.sleep(7_000);
        var atCrash = checkpointStore.findByStreamName(REDELIVERY_STREAM).orElseThrow();
        assertThat(atCrash.lastSeenToken()).isEqualTo(expired);
        assertThat(atCrash.lastProcessedToken()).isEqualTo(expired);
        assertThat(redeliveryHandler.completedCount()).isZero();

        streamManager.stopStream(REDELIVERY_STREAM);

        // Restart: the intact marker re-enters the recovery and the event is
        // REDELIVERED; this time the Mono completes and saveProcessed writes
        // a fresh anchor BEFORE the token settles.
        streamManager.startStream(REDELIVERY_STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(REDELIVERY_STREAM));
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(redeliveryHandler.completedCount()).isEqualTo(1));

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var cp = checkpointStore.findByStreamName(REDELIVERY_STREAM).orElseThrow();
            assertThat(cp.lastSeenToken()).isNotNull().isNotEqualTo(expired);
            assertThat(cp.lastProcessedToken())
                    .as("a processed anchor reacquired during the replay must never be unset")
                    .isNotNull()
                    .isNotEqualTo(expired);
        });
    }

    private static final class RecordingResumeMetrics
            implements io.flowwarden.stream.spi.StreamMetricsProvider {
        final java.util.concurrent.atomic.AtomicInteger historyLostCount =
                new java.util.concurrent.atomic.AtomicInteger();

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

    private static final String OPLOG_STREAM = "idle-hb-reactive-oplog-start";
    private static final String OPLOG_COLLECTION = "idle_hb_reactive_oplog_start";

    private static final String CATCHUP_OPTOUT_STREAM = "idle-hb-reactive-catchup-optout";
    private static final String CATCHUP_OPTOUT_COLLECTION = "idle_hb_reactive_catchup_optout";

    private static final String REDELIVERY_STREAM = "idle-hb-reactive-redelivery";
    private static final String REDELIVERY_COLLECTION = "idle_hb_reactive_redelivery";

    @SpringBootApplication
    @EnableFlowWarden
    @Import({ReactiveIdleHandler.class, ReactiveCatchUpOptOutHandler.class,
            ReactiveOplogStartHandler.class, ReactiveRedeliveryHandler.class})
    static class TestApp {}

    @ChangeStream(name = REDELIVERY_STREAM, collection = REDELIVERY_COLLECTION,
            documentType = Document.class, autoStart = false)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1, idleHeartbeatIntervalSeconds = 1,
            onHistoryLost = io.flowwarden.stream.OnHistoryLost.RESUME_FROM_OPLOG_START)
    static class ReactiveRedeliveryHandler {

        private final java.util.concurrent.atomic.AtomicInteger invocations =
                new java.util.concurrent.atomic.AtomicInteger();
        private volatile java.util.concurrent.CountDownLatch entered =
                new java.util.concurrent.CountDownLatch(1);
        private final List<Document> completed = new CopyOnWriteArrayList<>();

        @OnInsert
        Mono<Void> handle(ChangeStreamContext<Document> ctx) {
            if (invocations.incrementAndGet() == 1) {
                entered.countDown();
                // First delivery: never completes — the stream disposal
                // ("crash") cancels it without settling the event.
                return Mono.never();
            }
            ctx.getFullDocument(Document.class).ifPresent(completed::add);
            return Mono.empty();
        }

        boolean awaitEntered(int seconds) throws InterruptedException {
            return entered.await(seconds, java.util.concurrent.TimeUnit.SECONDS);
        }

        int completedCount() {
            return completed.size();
        }

        void reset() {
            entered = new java.util.concurrent.CountDownLatch(1);
            invocations.set(0);
            completed.clear();
        }
    }

    @ChangeStream(name = OPLOG_STREAM, collection = OPLOG_COLLECTION,
            documentType = Document.class, autoStart = false)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1, idleHeartbeatIntervalSeconds = 1,
            onHistoryLost = io.flowwarden.stream.OnHistoryLost.RESUME_FROM_OPLOG_START)
    static class ReactiveOplogStartHandler {

        private final List<Document> events = new CopyOnWriteArrayList<>();

        @OnInsert
        Mono<Void> handle(ChangeStreamContext<Document> ctx) {
            ctx.getFullDocument(Document.class).ifPresent(events::add);
            return Mono.empty();
        }

        List<String> tags() {
            return events.stream().map(d -> d.getString("tag")).toList();
        }

        void clear() { events.clear(); }
    }

    @ChangeStream(name = CATCHUP_OPTOUT_STREAM, collection = CATCHUP_OPTOUT_COLLECTION,
            documentType = Document.class, autoStart = false)
    @Checkpoint(saveEveryN = 100, saveIntervalSeconds = 1, idleHeartbeatIntervalSeconds = 0)
    static class ReactiveCatchUpOptOutHandler {

        private final List<Document> events = new CopyOnWriteArrayList<>();

        @OnInsert
        Mono<Void> handle(ChangeStreamContext<Document> ctx) {
            ctx.getFullDocument(Document.class).ifPresent(events::add);
            return Mono.empty();
        }

        List<String> tags() {
            return events.stream().map(d -> d.getString("tag")).toList();
        }

        void clear() { events.clear(); }
    }

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
