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
import io.flowwarden.stream.DeploymentMode;
import io.flowwarden.stream.FlowWardenMetrics;
import io.flowwarden.stream.OnHistoryLost;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.internal.lock.LeaderElectionCoordinator;
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
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Runtime cursor-death recovery, imperative manager. A {@code failCommand}
 * failpoint injects a non-resumable error (code 286) into the stream's next
 * {@code getMore} — the deterministic equivalent of an oplog rollover or any
 * error the driver's internal resume machinery gives up on. The failpoint is
 * scoped to this test's connection {@code appName}, so cursors owned by other
 * cached Spring contexts cannot consume it.
 *
 * <p>Covers: cursor death observed ({@code onStreamStopped(CRASHED)}), full
 * state eviction, managed resubscription through the resume cascade
 * ({@code onStreamRestarted}), delivery resuming afterwards — and the
 * terminal path: a poisoned checkpoint under {@code FAIL} at resubscribe
 * time stops the loop and, under {@code SINGLE_LEADER}, releases the
 * lock.</p>
 */
@SpringBootTest(classes = ImperativeCursorDeathIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeCursorDeathIntegrationTest {

    private static final String APP_NAME = "fw-imp-cursor-death";
    private static final String STREAM = "imp-cursor-death";
    private static final String COLLECTION = "imp_cursor_death";
    private static final String LEADER_STREAM = "imp-cursor-death-leader";
    private static final String LEADER_COLLECTION = "imp_cursor_death_leader";
    private static final String CRASH_STREAM = "imp-listener-crash";
    private static final String CRASH_COLLECTION = "imp_listener_crash";
    private static final String GHOST_STREAM = "imp-ghost-registration";
    private static final String GHOST_COLLECTION = "imp_ghost_registration";
    private static final BsonDocument EXPIRED_TOKEN =
            BsonDocument.parse("{\"_data\": \"0000DEAD\"}");

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri",
                () -> SharedMongoContainer.MONGO.getReplicaSetUrl() + "?appName=" + APP_NAME);
    }

    @Autowired MongoTemplate mongoTemplate;
    @Autowired ImperativeStreamManager streamManager;
    @Autowired CheckpointStore checkpointStore;
    @Autowired LeaderElectionCoordinator leaderElection;
    @Autowired CursorDeathHandler handler;
    @Autowired LeaderFailHandler leaderHandler;
    @Autowired ListenerCrashHandler crashHandler;
    @Autowired GhostHandler ghostHandler;

    private RecordingMetrics metrics;

    @BeforeEach
    void setUp() {
        disableFailPoint();
        handler.clear();
        leaderHandler.clear();
        crashHandler.clear();
        ghostHandler.clear();
        mongoTemplate.dropCollection(COLLECTION);
        mongoTemplate.dropCollection(LEADER_COLLECTION);
        mongoTemplate.dropCollection(CRASH_COLLECTION);
        mongoTemplate.dropCollection(GHOST_COLLECTION);
        checkpointStore.delete(STREAM);
        checkpointStore.delete(LEADER_STREAM);
        checkpointStore.delete(CRASH_STREAM);
        checkpointStore.delete(GHOST_STREAM);
        metrics = new RecordingMetrics();
        FlowWardenMetrics.setProvider(metrics);
    }

    @AfterEach
    void tearDown() {
        disableFailPoint();
        try { streamManager.stopStream(STREAM); } catch (Exception ignored) { }
        try { leaderElection.stop(LEADER_STREAM); } catch (Exception ignored) { }
        try { streamManager.stopStream(LEADER_STREAM); } catch (Exception ignored) { }
        try { streamManager.stopStream(CRASH_STREAM); } catch (Exception ignored) { }
        try { streamManager.stopStream(GHOST_STREAM); } catch (Exception ignored) { }
        checkpointStore.delete(STREAM);
        checkpointStore.delete(LEADER_STREAM);
        checkpointStore.delete(CRASH_STREAM);
        checkpointStore.delete(GHOST_STREAM);
        FlowWardenMetrics.setProvider(StreamMetricsProvider.noOp());
    }

    @Test
    void cursorDeath_isObserved_streamResubscribesThroughCascade_andKeepsDelivering() {
        streamManager.startStream(STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(STREAM));

        mongoTemplate.insert(new Document("seq", 1), COLLECTION);
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(handler.count()).isEqualTo(1));

        // Kill the cursor: the next getMore of this app's only open cursor
        // fails with a non-resumable 286.
        enableFailPointOnce();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(metrics.stops)
                        .as("cursor death must be surfaced, not silently logged")
                        .anyMatch(s -> s.startsWith(STREAM + ":CRASHED")));

        // Down window (the ≥1s backoff before the first attempt): the dead
        // stream must leave NOTHING behind — a lingering token or interval
        // task is exactly the zombie checkpoint that masked the original
        // frozen-stream bug.
        assertThat(streamManager.hasLatestToken(STREAM)).isFalse();
        assertThat(streamManager.hasIntervalTask(STREAM)).isFalse();
        assertThat(streamManager.hasHeartbeat(STREAM)).isFalse();
        assertThat(streamManager.isRestartPending(STREAM)).isTrue();

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(metrics.restarts)
                        .as("the managed loop resubscribes through the cascade")
                        .anyMatch(s -> s.startsWith(STREAM + ":1:")));
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(STREAM));

        // The resubscription resumed from the persisted position: subsequent
        // events flow without any manual intervention.
        mongoTemplate.insert(new Document("seq", 2), COLLECTION);
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(handler.count()).isEqualTo(2));
    }

    @Test
    void listenerCrash_failStops_noUntrackedConsumerSurvives() throws Exception {
        // Review round 1 blocker: Spring's default ErrorHandler used to
        // cancel the subscription on listener errors — the custom handler
        // must restore that fail-stop (via the wrapper's provenance marker),
        // never leave an evicted-but-reading consumer, and never restart.
        streamManager.startStream(CRASH_STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(CRASH_STREAM));

        // The @Filter throws: the crash escapes the listener (wrapper path).
        mongoTemplate.insert(new Document("seq", 1), CRASH_COLLECTION);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(metrics.stops)
                        .anyMatch(s -> s.startsWith(CRASH_STREAM + ":CRASHED")));
        await().atMost(Duration.ofSeconds(10))
                .until(() -> !streamManager.isRunning(CRASH_STREAM));

        // An untracked consumer would still evaluate this event's filter.
        int filterCallsAtCrash = crashHandler.filterCalls();
        mongoTemplate.insert(new Document("seq", 2), CRASH_COLLECTION);
        Thread.sleep(2_000);
        assertThat(crashHandler.filterCalls())
                .as("the cancelled subscription must not keep consuming")
                .isEqualTo(filterCallsAtCrash);
        assertThat(streamManager.isRunning(CRASH_STREAM)).isFalse();
        assertThat(streamManager.isRestartPending(CRASH_STREAM))
                .as("listener crashes keep their fail-stop semantics — no restart")
                .isFalse();
        assertThat(metrics.restarts).noneMatch(s -> s.startsWith(CRASH_STREAM + ":"));
    }

    @Test
    void listenerCrash_withThrowingMetricsProvider_stillFailStops() throws Exception {
        // Round 3: the wrapper's emission is best-effort — a throwing
        // provider must not strip the provenance marker, or the crash would
        // be misclassified as a cursor death and restarted over a
        // still-active reader.
        metrics.throwOnStops = true;
        streamManager.startStream(CRASH_STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(CRASH_STREAM));

        mongoTemplate.insert(new Document("seq", 1), CRASH_COLLECTION);

        await().atMost(Duration.ofSeconds(10))
                .until(() -> !streamManager.isRunning(CRASH_STREAM));
        int filterCallsAtCrash = crashHandler.filterCalls();
        mongoTemplate.insert(new Document("seq", 2), CRASH_COLLECTION);
        Thread.sleep(2_000);
        assertThat(crashHandler.filterCalls()).isEqualTo(filterCallsAtCrash);
        assertThat(streamManager.isRestartPending(CRASH_STREAM))
                .as("no restart may be armed for a listener crash, provider throwing or not")
                .isFalse();
        assertThat(metrics.restarts).noneMatch(s -> s.startsWith(CRASH_STREAM + ":"));
    }

    @Test
    void stopDuringPendingRestart_operatorWins_streamStaysDown() {
        streamManager.startStream(STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(STREAM));
        mongoTemplate.insert(new Document("seq", 1), COLLECTION);
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(handler.count()).isEqualTo(1));

        enableFailPointOnce();
        await().atMost(Duration.ofSeconds(10))
                .until(() -> streamManager.isRestartPending(STREAM));

        // The operator stops the stream during the backoff window: the stop
        // wins over the loop — no attempt may resurrect the stream.
        streamManager.stopStream(STREAM);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(streamManager.isRestartPending(STREAM)).isFalse());
        try {
            Thread.sleep(3_000); // past several would-be attempts
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertThat(streamManager.isRunning(STREAM)).isFalse();
        assertThat(metrics.restarts).noneMatch(s -> s.startsWith(STREAM + ":"));
    }

    @Test
    void immediateCursorFailureDuringRegistration_noGhostStream_restartRecovers() {
        // Review round 1 blocker: the container submits the reading task
        // before register() returns — an error at cursor OPEN must find the
        // pre-installed state (handshake) instead of reconstructing a ghost
        // stream with schedules over a dead subscription.
        enableFailPointOnce("aggregate");

        streamManager.startStream(GHOST_STREAM);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(metrics.stops)
                        .anyMatch(s -> s.startsWith(GHOST_STREAM + ":CRASHED")));
        // No ghost: nothing installed, nothing scheduled, a restart pending.
        assertThat(streamManager.isRunning(GHOST_STREAM)).isFalse();
        assertThat(streamManager.hasLatestToken(GHOST_STREAM)).isFalse();
        assertThat(streamManager.hasIntervalTask(GHOST_STREAM)).isFalse();
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRestartPending(GHOST_STREAM)
                        || streamManager.isRunning(GHOST_STREAM));

        // The failpoint is consumed: the next attempt opens normally.
        await().atMost(Duration.ofSeconds(15))
                .until(() -> streamManager.isRunning(GHOST_STREAM));
        mongoTemplate.insert(new Document("seq", 1), GHOST_COLLECTION);
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(ghostHandler.count()).isEqualTo(1));
    }

    @Test
    void terminalFailureAtResubscribe_underSingleLeader_stopsLoopAndReleasesLock() throws Exception {
        // Elected leader, running stream.
        leaderElection.startElection(LEADER_STREAM,
                () -> streamManager.startStream(LEADER_STREAM),
                () -> streamManager.stopStream(LEADER_STREAM));
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(LEADER_STREAM));
        assertThat(mongoTemplate.findById(LEADER_STREAM, Document.class, "_fw_locks"))
                .as("the elected leader holds the lock")
                .isNotNull();

        // Poison the checkpoint (the running stream never re-reads it), then
        // kill the cursor: the resubscribe cascade escalates to
        // OnHistoryLost.FAIL — terminal.
        Instant past = Instant.now().minusSeconds(86_400);
        checkpointStore.save(new io.flowwarden.stream.spi.Checkpoint(
                LEADER_STREAM, null, EXPIRED_TOKEN, past, EXPIRED_TOKEN, past,
                Collections.emptyMap()));
        enableFailPointOnce();

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(metrics.stops)
                        .anyMatch(s -> s.equals(LEADER_STREAM + ":CRASHED:HistoryLostException")));
        assertThat(streamManager.isRunning(LEADER_STREAM)).isFalse();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(mongoTemplate.findById(LEADER_STREAM, Document.class, "_fw_locks"))
                        .as("the lease must not be renewed for a stream the loop gave up on")
                        .isNull());

        // No further attempts: the stream stays down (terminal, operator
        // action required).
        Thread.sleep(3_000);
        assertThat(streamManager.isRunning(LEADER_STREAM)).isFalse();
        assertThat(metrics.restarts)
                .noneMatch(s -> s.startsWith(LEADER_STREAM + ":"));
    }

    private void enableFailPointOnce() {
        enableFailPointOnce("getMore");
    }

    private void enableFailPointOnce(String command) {
        mongoTemplate.getMongoDatabaseFactory().getMongoDatabase("admin")
                .runCommand(new Document("configureFailPoint", "failCommand")
                        .append("mode", new Document("times", 1))
                        .append("data", new Document("errorCode", 286)
                                .append("failCommands", List.of(command))
                                .append("appName", APP_NAME)));
    }

    private void disableFailPoint() {
        try {
            mongoTemplate.getMongoDatabaseFactory().getMongoDatabase("admin")
                    .runCommand(new Document("configureFailPoint", "failCommand")
                            .append("mode", "off"));
        } catch (Exception ignored) {
            // best-effort cleanup
        }
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import({CursorDeathHandler.class, LeaderFailHandler.class,
            ListenerCrashHandler.class, GhostHandler.class})
    static class TestApp {}

    @ChangeStream(name = CRASH_STREAM, collection = CRASH_COLLECTION,
            documentType = Document.class, autoStart = false)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1, idleHeartbeatIntervalSeconds = 0,
            onHistoryLost = OnHistoryLost.RESUME_FROM_NOW)
    static class ListenerCrashHandler {

        private final java.util.concurrent.atomic.AtomicInteger filterCalls =
                new java.util.concurrent.atomic.AtomicInteger();

        @OnInsert
        void handle(ChangeStreamContext<Document> ctx) {
        }

        @io.flowwarden.stream.annotation.Filter
        boolean filter(ChangeStreamContext<Document> ctx) {
            filterCalls.incrementAndGet();
            // Escapes the listener (outside the handler retry loop): the
            // wrapper's crash path, not the cursor's.
            throw new IllegalStateException("listener-level crash");
        }

        int filterCalls() {
            return filterCalls.get();
        }

        void clear() {
            filterCalls.set(0);
        }
    }

    @ChangeStream(name = GHOST_STREAM, collection = GHOST_COLLECTION,
            documentType = Document.class, autoStart = false)
    static class GhostHandler {

        private final List<Document> events = new CopyOnWriteArrayList<>();

        @OnInsert
        void handle(ChangeStreamContext<Document> ctx) {
            ctx.getFullDocument(Document.class).ifPresent(events::add);
        }

        int count() { return events.size(); }
        void clear() { events.clear(); }
    }

    @ChangeStream(name = STREAM, collection = COLLECTION,
            documentType = Document.class, autoStart = false)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1, idleHeartbeatIntervalSeconds = 0,
            onHistoryLost = OnHistoryLost.RESUME_FROM_NOW)
    static class CursorDeathHandler {

        private final List<Document> events = new CopyOnWriteArrayList<>();

        @OnInsert
        void handle(ChangeStreamContext<Document> ctx) {
            ctx.getFullDocument(Document.class).ifPresent(events::add);
        }

        int count() { return events.size(); }
        void clear() { events.clear(); }
    }

    @ChangeStream(name = LEADER_STREAM, collection = LEADER_COLLECTION,
            documentType = Document.class, autoStart = false,
            deploymentMode = DeploymentMode.SINGLE_LEADER)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1, idleHeartbeatIntervalSeconds = 0,
            onHistoryLost = OnHistoryLost.FAIL)
    static class LeaderFailHandler {

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
