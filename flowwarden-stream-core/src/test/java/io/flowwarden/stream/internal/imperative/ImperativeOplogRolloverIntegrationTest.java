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
import io.flowwarden.stream.HistoryLostContext;
import io.flowwarden.stream.OnHistoryLost;
import io.flowwarden.stream.ReconciliationFailedException;
import io.flowwarden.stream.annotation.Reconcile;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.spi.BackpressureAction;
import io.flowwarden.stream.spi.ChangeEventMetadata;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.StopReason;
import io.flowwarden.stream.spi.StreamConfiguration;
import io.flowwarden.stream.spi.StreamMetricsProvider;
import io.flowwarden.stream.test.RollingOplogMongoContainer;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Against a replica set whose oplog really rolls over (1 MB oplog, see
 * {@link RollingOplogMongoContainer}). The shared container never rolls over,
 * and a fresh replica set is exempt from the server's fallen-off check (its
 * first oplog entry is the {@code initiating set} noop) — which is why the
 * two behaviours below were invisible to every other integration test.
 */
@SpringBootTest(classes = ImperativeOplogRolloverIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeOplogRolloverIntegrationTest {

    private static final BsonDocument FAKE_TOKEN = BsonDocument.parse("{\"_data\": \"DEADBEEF\"}");
    private static final String OPLOG_START_STREAM = "rollover-oplog-start";
    private static final String PROBE_STREAM = "rollover-probe";
    private static final String RECONCILE_STREAM = "rollover-reconcile";

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", RollingOplogMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired MongoTemplate mongoTemplate;
    @Autowired ImperativeStreamManager streamManager;
    @Autowired CheckpointStore checkpointStore;
    @Autowired OplogStartHandler oplogStartHandler;
    @Autowired ProbeRepairHandler probeRepairHandler;
    @Autowired ReconcileHandler reconcileHandler;

    private final RecordingMetrics metrics = new RecordingMetrics();
    private StreamMetricsProvider previousProvider;

    @BeforeEach
    void installMetrics() {
        previousProvider = FlowWardenMetrics.get();
        FlowWardenMetrics.setProvider(metrics);
    }

    @AfterEach
    void tearDown() {
        try { streamManager.stopStream(OPLOG_START_STREAM); } catch (Exception ignored) {}
        try { streamManager.stopStream(PROBE_STREAM); } catch (Exception ignored) {}
        try { streamManager.stopStream(RECONCILE_STREAM); } catch (Exception ignored) {}
        checkpointStore.delete(OPLOG_START_STREAM);
        checkpointStore.delete(PROBE_STREAM);
        checkpointStore.delete(RECONCILE_STREAM);
        oplogStartHandler.clear();
        probeRepairHandler.clear();
        reconcileHandler.clear();
        FlowWardenMetrics.setProvider(previousProvider);
    }

    @Test
    void oplogStart_afterARealRollover_resumesAtTheOldestEntry_andReplaysWhatSurvived() {
        // Push the "initiating set" noop out of the oplog: from here on the
        // server checks that the start position has not fallen off. Before
        // the fix, the imperative manager asked for (t, 0) — one increment
        // before the oldest entry — and looped on 286 at every attempt.
        RollingOplogMongoContainer.rollOverEverythingSoFar();
        mongoTemplate.insert(new Document("item", "survived"), "rollover_oplog_start");
        saveExpiredCheckpoint(OPLOG_START_STREAM);

        streamManager.startStream(OPLOG_START_STREAM);

        await().atMost(Duration.ofSeconds(10))
                .until(() -> streamManager.isRunning(OPLOG_START_STREAM));
        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThat(oplogStartHandler.items()).contains("survived"));
        mongoTemplate.insert(new Document("item", "after-start"), "rollover_oplog_start");
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(oplogStartHandler.items()).contains("after-start"));
        // Establishment: the dead pair left as the "recovery due" marker is
        // replaced by a certified position.
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            io.flowwarden.stream.spi.Checkpoint cp =
                    checkpointStore.findByStreamName(OPLOG_START_STREAM).orElseThrow();
            assertThat(cp.lastSeenToken()).isNotNull().isNotEqualTo(FAKE_TOKEN);
            assertThat(cp.lastProcessedToken()).isNotEqualTo(FAKE_TOKEN);
        });
        assertThat(streamManager.isRestartPending(OPLOG_START_STREAM)).isFalse();
        assertThat(metrics.crashes).hasValue(0);
    }

    @Test
    void reconcile_outlivesTheOplogOnce_secondBoundedCall_thenTheStreamResumes() {
        saveExpiredCheckpoint(RECONCILE_STREAM);
        reconcileHandler.rollsRemaining.set(1); // the first reconciliation rolls the oplog past the captured position

        streamManager.startStream(RECONCILE_STREAM);

        await().atMost(Duration.ofSeconds(15)).until(() -> streamManager.isRunning(RECONCILE_STREAM));
        assertThat(reconcileHandler.reconciliations).extracting(HistoryLostContext::attempt).containsExactly(1, 2);
        HistoryLostContext first = reconcileHandler.reconciliations.get(0);
        HistoryLostContext second = reconcileHandler.reconciliations.get(1);
        assertThat(second.lostWindowStart()).contains(first.lostWindowEnd());
        assertThat(metrics.reconciled).containsExactly(1, 2);
        assertThat(metrics.reconciliationFailures).hasValue(0);
        io.flowwarden.stream.spi.Checkpoint cp = checkpointStore.findByStreamName(RECONCILE_STREAM).orElseThrow();
        assertThat(cp.lastProcessedToken()).isNotEqualTo(FAKE_TOKEN);
        assertThat(cp.lastSeenToken()).isNotNull().isNotEqualTo(FAKE_TOKEN);

        mongoTemplate.insert(new Document("item", "after-reconcile"), "rollover_reconcile");
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(reconcileHandler.items).contains("after-reconcile"));
    }

    @Test
    void reconcile_outlivesTheOplogTwice_failsClearly_deadTokensKept() {
        saveExpiredCheckpoint(RECONCILE_STREAM);
        reconcileHandler.rollsRemaining.set(2); // both reconciliations roll the oplog past their position

        assertThatThrownBy(() -> streamManager.startStream(RECONCILE_STREAM))
                .isInstanceOf(ReconciliationFailedException.class)
                .hasMessageContaining("does not fit in the oplog window")
                .satisfies(e -> assertThat(((ReconciliationFailedException) e).getAttempt()).isEqualTo(2));

        assertThat(reconcileHandler.reconciliations).extracting(HistoryLostContext::attempt).containsExactly(1, 2);
        assertThat(streamManager.isRunning(RECONCILE_STREAM)).isFalse();
        assertThat(metrics.reconciliationFailures).hasValue(1);
        assertThat(checkpointStore.findByStreamName(RECONCILE_STREAM).orElseThrow().lastProcessedToken())
                .isEqualTo(FAKE_TOKEN);
    }

    @Test
    void idleProbe_positionRolledOut_repairsTheCheckpointInPlace_whileTheCursorKeepsDelivering() {
        streamManager.startStream(PROBE_STREAM);
        await().atMost(Duration.ofSeconds(10)).until(() -> streamManager.isRunning(PROBE_STREAM));
        awaitCertifiedPosition();

        // A noisy neighbour rolls the oplog past the certified position.
        rollUntilTheProbeFindsItsPositionGone();
        awaitPositionSettledAfterTheRoll();

        assertThat(streamManager.isRunning(PROBE_STREAM)).isTrue();
        assertThat(metrics.crashes).as("no restart was needed").hasValue(0);
        io.flowwarden.stream.spi.Checkpoint cp = checkpointStore.findByStreamName(PROBE_STREAM).orElseThrow();
        assertThat(cp.lastSeenToken()).as("a fresh position is stored").isNotNull();
        assertThat(cp.lastProcessedToken()).as("the dead pair is gone").isNull();

        // The open cursor was never affected.
        mongoTemplate.insert(new Document("item", "after-repair"), "rollover_probe");
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(probeRepairHandler.items()).contains("after-repair"));
    }

    @Test
    void idleProbe_repairedPosition_isWhatTheNextRestartResumesFrom() {
        streamManager.startStream(PROBE_STREAM);
        await().atMost(Duration.ofSeconds(10)).until(() -> streamManager.isRunning(PROBE_STREAM));
        awaitCertifiedPosition();
        rollUntilTheProbeFindsItsPositionGone();
        awaitPositionSettledAfterTheRoll();

        // Under OnHistoryLost.FAIL a dead stored position would make this
        // start throw: the repaired position is a valid level-2 resume point.
        streamManager.stopStream(PROBE_STREAM);
        assertThatCode(() -> streamManager.startStream(PROBE_STREAM)).doesNotThrowAnyException();
        await().atMost(Duration.ofSeconds(10)).until(() -> streamManager.isRunning(PROBE_STREAM));
        assertThat(metrics.historyLosses).hasValue(0);

        mongoTemplate.insert(new Document("item", "after-restart"), "rollover_probe");
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(probeRepairHandler.items()).contains("after-restart"));
    }

    private BsonDocument awaitCertifiedPosition() {
        return await().atMost(Duration.ofSeconds(10))
                .until(() -> checkpointStore.findByStreamName(PROBE_STREAM)
                        .map(io.flowwarden.stream.spi.Checkpoint::lastSeenToken).orElse(null),
                        token -> token != null);
    }

    /**
     * The idle probe keeps certifying while the oplog rolls, so a single
     * rollover may leave the latest certified position inside the surviving
     * window. Roll past everything, repeatedly, until a probe actually finds
     * its position gone (the first {@code onHeartbeatProbeFailed}).
     */
    private void rollUntilTheProbeFindsItsPositionGone() {
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(500)).until(() -> {
            if (metrics.probeFailures.get() > 0) {
                return true;
            }
            RollingOplogMongoContainer.rollOverEverythingSoFar();
            return false;
        });
    }

    /**
     * While the burst lasts, a repaired position can roll out again and be
     * repaired again at the next probe — that is the design. The checkpoint
     * is only stable once a probe has written a position after the last
     * filler write: wait for that write.
     */
    private void awaitPositionSettledAfterTheRoll() {
        Instant quiet = Instant.now();
        await().atMost(Duration.ofSeconds(15))
                .until(() -> metrics.lastCheckpointAt != null && metrics.lastCheckpointAt.isAfter(quiet));
    }

    private void saveExpiredCheckpoint(String streamName) {
        Instant past = Instant.now().minusSeconds(86_400);
        checkpointStore.save(new io.flowwarden.stream.spi.Checkpoint(
                streamName, null, FAKE_TOKEN, past, FAKE_TOKEN, past, Collections.emptyMap()));
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import({
            ImperativeOplogRolloverIntegrationTest.OplogStartHandler.class,
            ImperativeOplogRolloverIntegrationTest.ProbeRepairHandler.class,
            ImperativeOplogRolloverIntegrationTest.ReconcileHandler.class
    })
    static class TestApp {}

    @ChangeStream(name = RECONCILE_STREAM, collection = "rollover_reconcile", autoStart = false)
    @Checkpoint(saveEveryN = 1, idleHeartbeatIntervalSeconds = 2, onHistoryLost = OnHistoryLost.RESUME_FROM_NOW)
    static class ReconcileHandler {
        private final List<String> items = new CopyOnWriteArrayList<>();
        final List<HistoryLostContext> reconciliations = new CopyOnWriteArrayList<>();
        /** How many of the next hook calls roll the oplog past everything captured before them. */
        final AtomicInteger rollsRemaining = new AtomicInteger();

        @OnInsert
        void handle(ChangeStreamContext<Document> ctx) {
            ctx.getFullDocument(Document.class).ifPresent(d -> items.add(d.getString("item")));
        }

        @Reconcile
        void reconcile(HistoryLostContext ctx) {
            reconciliations.add(ctx);
            if (rollsRemaining.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                RollingOplogMongoContainer.rollOverEverythingSoFar();
            }
        }

        void clear() {
            items.clear();
            reconciliations.clear();
            rollsRemaining.set(0);
        }
    }

    @ChangeStream(name = OPLOG_START_STREAM, collection = "rollover_oplog_start", autoStart = false)
    @Checkpoint(saveEveryN = 1, idleHeartbeatIntervalSeconds = 2,
            onHistoryLost = OnHistoryLost.RESUME_FROM_OPLOG_START)
    static class OplogStartHandler {
        private final List<String> items = new CopyOnWriteArrayList<>();

        @OnInsert
        void handle(ChangeStreamContext<Document> ctx) {
            ctx.getFullDocument(Document.class).ifPresent(d -> items.add(d.getString("item")));
        }

        List<String> items() { return items; }
        void clear() { items.clear(); }
    }

    @ChangeStream(name = PROBE_STREAM, collection = "rollover_probe", autoStart = false)
    @Checkpoint(saveEveryN = 1, idleHeartbeatIntervalSeconds = 2, onHistoryLost = OnHistoryLost.FAIL)
    static class ProbeRepairHandler {
        private final List<String> items = new CopyOnWriteArrayList<>();

        @OnInsert
        void handle(ChangeStreamContext<Document> ctx) {
            ctx.getFullDocument(Document.class).ifPresent(d -> items.add(d.getString("item")));
        }

        List<String> items() { return items; }
        void clear() { items.clear(); }
    }

    static final class RecordingMetrics implements StreamMetricsProvider {
        final AtomicInteger probeFailures = new AtomicInteger();
        final AtomicInteger crashes = new AtomicInteger();
        final AtomicInteger historyLosses = new AtomicInteger();
        final List<Integer> reconciled = new CopyOnWriteArrayList<>();
        final AtomicInteger reconciliationFailures = new AtomicInteger();
        volatile Instant lastCheckpointAt;

        @Override
        public void onHistoryLostReconciled(String streamName, int attempt, Duration elapsed) {
            reconciled.add(attempt);
        }

        @Override
        public void onHistoryLostReconciliationFailed(String streamName, int attempt, Throwable cause) {
            reconciliationFailures.incrementAndGet();
        }

        @Override
        public void onHeartbeatProbeFailed(String streamName, Throwable cause) {
            probeFailures.incrementAndGet();
        }

        @Override
        public void onCheckpoint(String streamName, String resumeToken) {
            lastCheckpointAt = Instant.now();
        }

        @Override
        public void onStreamStopped(String streamName, StopReason reason, Throwable cause) {
            if (reason == StopReason.CRASHED) {
                crashes.incrementAndGet();
            }
        }

        @Override
        public void onResumeHistoryLost(String streamName) {
            historyLosses.incrementAndGet();
        }

        @Override
        public void onStreamStarted(String streamName, StreamConfiguration config) {}

        @Override
        public void onEventReceived(String streamName, ChangeEventMetadata metadata) {}

        @Override
        public void onEventProcessed(String streamName, long durationNanos, boolean success) {}

        @Override
        public void onEventError(String streamName, Throwable error, boolean willRetry,
                                 int attemptNumber, ChangeEventMetadata metadata) {}

        @Override
        public void onBufferStatus(String streamName, int currentSize, int maxSize) {}

        @Override
        public void onBackpressure(String streamName, BackpressureAction action) {}

        @Override
        public void onEventSentToDlq(String streamName) {}

        @Override
        public void onOplogStats(double logLengthHours, String status) {}
    }
}
