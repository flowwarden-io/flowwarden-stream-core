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
 * Invalidate handling (collection drop / rename): the underlying cursor dies
 * — the invalidation is surfaced ({@code onStreamInvalidated} + the runtime
 * {@code CRASHED}), a drop under a self-repairing strategy heals through the
 * managed restart (fresh certified position — pre-invalidate tokens would
 * replay the invalidate in a loop), and a rename or any invalidation under
 * {@code FAIL} stops the stream terminally.
 *
 * <p>Single-node Testcontainers caveat: the INVALIDATE event is not always
 * surfaced on the first pass — but convergence does not depend on it: an
 * unobserved invalidate leaves a pre-invalidate resume position, the restart
 * replays into the invalidate and the detection runs on the replay. The
 * assertions therefore await the final state with generous timeouts instead
 * of asserting the first-pass delivery.</p>
 */
@SpringBootTest(classes = ImperativeInvalidateIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeInvalidateIntegrationTest {

    private static final String DROP_STREAM = "imp-invalidate-drop";
    private static final String DROP_COLLECTION = "imp_invalidate_drop";
    private static final String RENAME_STREAM = "imp-invalidate-rename";
    private static final String RENAME_COLLECTION = "imp_invalidate_rename";
    private static final String RENAME_TARGET = "imp_invalidate_rename_target";
    private static final String FAIL_STREAM = "imp-invalidate-fail";
    private static final String FAIL_COLLECTION = "imp_invalidate_fail";

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired MongoTemplate mongoTemplate;
    @Autowired ImperativeStreamManager streamManager;
    @Autowired CheckpointStore checkpointStore;
    @Autowired DropHandler dropHandler;
    @Autowired RenameHandler renameHandler;
    @Autowired FailHandler failHandler;

    private RecordingMetrics metrics;

    @BeforeEach
    void setUp() {
        dropHandler.clear();
        renameHandler.clear();
        failHandler.clear();
        mongoTemplate.dropCollection(DROP_COLLECTION);
        mongoTemplate.dropCollection(RENAME_COLLECTION);
        mongoTemplate.dropCollection(RENAME_TARGET);
        mongoTemplate.dropCollection(FAIL_COLLECTION);
        checkpointStore.delete(DROP_STREAM);
        checkpointStore.delete(RENAME_STREAM);
        checkpointStore.delete(FAIL_STREAM);
        metrics = new RecordingMetrics();
        FlowWardenMetrics.setProvider(metrics);
    }

    @AfterEach
    void tearDown() {
        try { streamManager.stopStream(DROP_STREAM); } catch (Exception ignored) { }
        try { streamManager.stopStream(RENAME_STREAM); } catch (Exception ignored) { }
        try { streamManager.stopStream(FAIL_STREAM); } catch (Exception ignored) { }
        checkpointStore.delete(DROP_STREAM);
        checkpointStore.delete(RENAME_STREAM);
        checkpointStore.delete(FAIL_STREAM);
        FlowWardenMetrics.setProvider(StreamMetricsProvider.noOp());
    }

    @Test
    void collectionDrop_underSelfRepairingStrategy_surfacesAndSelfHeals() {
        streamManager.startStream(DROP_STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(DROP_STREAM));
        mongoTemplate.insert(new Document("seq", 1), DROP_COLLECTION);
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(dropHandler.count()).isEqualTo(1));

        mongoTemplate.dropCollection(DROP_COLLECTION);

        // The invalidation is surfaced (first pass or on the replay) and the
        // cursor death goes through the managed restart.
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(metrics.invalidations)
                        .contains(DROP_STREAM + ":DROP"));
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(metrics.stops)
                        .anyMatch(s -> s.startsWith(DROP_STREAM + ":CRASHED")));
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(metrics.restarts)
                        .anyMatch(s -> s.startsWith(DROP_STREAM + ":")));
        await().atMost(Duration.ofSeconds(10))
                .until(() -> streamManager.isRunning(DROP_STREAM));

        // The re-created collection's events flow again — and the restored
        // history was NOT replayed (fresh position, count stays exact).
        mongoTemplate.insert(new Document("seq", 2), DROP_COLLECTION);
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(dropHandler.count()).isEqualTo(2));
        var repaired = checkpointStore.findByStreamName(DROP_STREAM).orElseThrow();
        assertThat(repaired.lastSeenToken()).isNotNull();
    }

    @Test
    void collectionRename_isTerminal_noAutomaticRestart() throws Exception {
        streamManager.startStream(RENAME_STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(RENAME_STREAM));
        mongoTemplate.insert(new Document("seq", 1), RENAME_COLLECTION);
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(renameHandler.count()).isEqualTo(1));

        String db = mongoTemplate.getDb().getName();
        mongoTemplate.getMongoDatabaseFactory().getMongoDatabase("admin")
                .runCommand(new Document("renameCollection", db + "." + RENAME_COLLECTION)
                        .append("to", db + "." + RENAME_TARGET));

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(metrics.invalidations)
                        .contains(RENAME_STREAM + ":RENAME"));
        await().atMost(Duration.ofSeconds(10))
                .until(() -> !streamManager.isRunning(RENAME_STREAM));

        // Terminal: no restart, ever — the declared collection identity is
        // gone, an operator redeploy is required.
        Thread.sleep(3_000);
        assertThat(streamManager.isRestartPending(RENAME_STREAM)).isFalse();
        assertThat(streamManager.isRunning(RENAME_STREAM)).isFalse();
        assertThat(metrics.restarts)
                .noneMatch(s -> s.startsWith(RENAME_STREAM + ":"));
    }

    @Test
    void collectionDrop_underFail_isTerminal() throws Exception {
        streamManager.startStream(FAIL_STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(FAIL_STREAM));
        mongoTemplate.insert(new Document("seq", 1), FAIL_COLLECTION);
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(failHandler.count()).isEqualTo(1));

        mongoTemplate.dropCollection(FAIL_COLLECTION);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(metrics.invalidations)
                        .contains(FAIL_STREAM + ":DROP"));
        await().atMost(Duration.ofSeconds(10))
                .until(() -> !streamManager.isRunning(FAIL_STREAM));

        // FAIL refuses to skip history — the invalidation is terminal.
        Thread.sleep(3_000);
        assertThat(streamManager.isRestartPending(FAIL_STREAM)).isFalse();
        assertThat(streamManager.isRunning(FAIL_STREAM)).isFalse();
        assertThat(metrics.restarts)
                .noneMatch(s -> s.startsWith(FAIL_STREAM + ":"));

        // The marker is durable: a later boot's cascade escalates to level 3
        // (the invalidate token is unusable and the processed pair was
        // cleared atomically) where FAIL keeps failing loudly.
        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> streamManager.startStream(FAIL_STREAM))
                .isInstanceOf(io.flowwarden.stream.HistoryLostException.class);
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import({DropHandler.class, RenameHandler.class, FailHandler.class})
    static class TestApp {}

    @ChangeStream(name = DROP_STREAM, collection = DROP_COLLECTION,
            documentType = Document.class, autoStart = false)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1, idleHeartbeatIntervalSeconds = 0,
            onHistoryLost = OnHistoryLost.RESUME_FROM_NOW)
    static class DropHandler {

        private final List<Document> events = new CopyOnWriteArrayList<>();

        // Catch-all on purpose: the handler also checkpoints the DROP event
        // (saveEveryN = 1) — a lifecycle token must never become the
        // processed anchor, or the resume would skip past the invalidation
        // classification on later boots.
        @io.flowwarden.stream.annotation.OnChange
        void handle(ChangeStreamContext<Document> ctx) {
            if (ctx.getOperationType() == io.flowwarden.stream.OperationType.INSERT) {
                ctx.getFullDocument(Document.class).ifPresent(events::add);
            }
        }

        int count() { return events.size(); }
        void clear() { events.clear(); }
    }

    @ChangeStream(name = RENAME_STREAM, collection = RENAME_COLLECTION,
            documentType = Document.class, autoStart = false)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1, idleHeartbeatIntervalSeconds = 0,
            onHistoryLost = OnHistoryLost.RESUME_FROM_NOW)
    static class RenameHandler {

        private final List<Document> events = new CopyOnWriteArrayList<>();

        @OnInsert
        void handle(ChangeStreamContext<Document> ctx) {
            ctx.getFullDocument(Document.class).ifPresent(events::add);
        }

        int count() { return events.size(); }
        void clear() { events.clear(); }
    }

    @ChangeStream(name = FAIL_STREAM, collection = FAIL_COLLECTION,
            documentType = Document.class, autoStart = false)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1, idleHeartbeatIntervalSeconds = 0,
            onHistoryLost = OnHistoryLost.FAIL)
    static class FailHandler {

        private final List<Document> events = new CopyOnWriteArrayList<>();

        @OnInsert
        void handle(ChangeStreamContext<Document> ctx) {
            ctx.getFullDocument(Document.class).ifPresent(events::add);
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
