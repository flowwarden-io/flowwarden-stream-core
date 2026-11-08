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
import io.flowwarden.stream.HistoryLostContext;
import io.flowwarden.stream.HistoryLostException;
import io.flowwarden.stream.OnHistoryLost;
import io.flowwarden.stream.ReconciliationFailedException;
import io.flowwarden.stream.annotation.Reconcile;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.spi.CheckpointStore;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = ImperativeHistoryLostIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeHistoryLostIntegrationTest {

    private static final BsonDocument FAKE_TOKEN =
            BsonDocument.parse("{\"_data\": \"DEADBEEF\"}");

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired MongoTemplate mongoTemplate;
    @Autowired ImperativeStreamManager streamManager;
    @Autowired CheckpointStore checkpointStore;
    @Autowired HistoryLostResumeNowHandler resumeNowHandler;
    @Autowired HistoryLostResumeOplogHandler resumeOplogHandler;
    @Autowired ReconcileNowHandler reconcileNowHandler;
    @Autowired ReconcileOplogHandler reconcileOplogHandler;

    @AfterEach
    void tearDown() {
        try { streamManager.stopStream("hl-fail-test"); } catch (Exception ignored) {}
        try { streamManager.stopStream("hl-resume-now-test"); } catch (Exception ignored) {}
        try { streamManager.stopStream("hl-resume-oplog-test"); } catch (Exception ignored) {}
        try { streamManager.stopStream("hl-reconcile-now-test"); } catch (Exception ignored) {}
        try { streamManager.stopStream("hl-reconcile-oplog-test"); } catch (Exception ignored) {}
        checkpointStore.delete("hl-fail-test");
        checkpointStore.delete("hl-resume-now-test");
        checkpointStore.delete("hl-resume-oplog-test");
        checkpointStore.delete("hl-reconcile-now-test");
        checkpointStore.delete("hl-reconcile-oplog-test");
        resumeNowHandler.clear();
        resumeOplogHandler.clear();
        reconcileNowHandler.clear();
        reconcileOplogHandler.clear();
    }

    // --- @Reconcile ---

    @Test
    void reconcile_runsOnceBeforeResume_withTheLostWindowBound_whileTheCheckpointIsStillDead() {
        Instant before = Instant.now();
        saveExpiredCheckpoint("hl-reconcile-now-test");

        streamManager.startStream("hl-reconcile-now-test");

        await().atMost(Duration.ofSeconds(5)).until(() -> streamManager.isRunning("hl-reconcile-now-test"));
        assertThat(reconcileNowHandler.reconciliations).hasSize(1);
        HistoryLostContext ctx = reconcileNowHandler.reconciliations.get(0);
        assertThat(ctx.streamName()).isEqualTo("hl-reconcile-now-test");
        assertThat(ctx.strategy()).isEqualTo(OnHistoryLost.RESUME_FROM_NOW);
        assertThat(ctx.attempt()).isEqualTo(1);
        assertThat(ctx.lostWindowStart()).isPresent();
        assertThat(ctx.lostWindowStart().get())
                .isBetween(before.minusSeconds(86_400 + 60), before.minusSeconds(86_400 - 60));
        assertThat(ctx.lostWindowEnd()).isAfterOrEqualTo(before.minusSeconds(1));
        // No durable write before the hook returned: the dead pair was still there.
        assertThat(reconcileNowHandler.checkpointDeadAtHook).containsExactly(true);
        io.flowwarden.stream.spi.Checkpoint repaired =
                checkpointStore.findByStreamName("hl-reconcile-now-test").orElseThrow();
        assertThat(repaired.lastProcessedToken()).isNotEqualTo(FAKE_TOKEN);
        assertThat(repaired.lastSeenToken()).isNotNull().isNotEqualTo(FAKE_TOKEN);

        mongoTemplate.insert(new Document("item", "after-reconcile"), "hl_reconcile_now");
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(reconcileNowHandler.items).contains("after-reconcile"));
    }

    @Test
    void reconcile_isABarrier_nothingDeliveredWhileItRuns_eventsWrittenMeanwhileDeliveredAfter() throws Exception {
        saveExpiredCheckpoint("hl-reconcile-now-test");
        reconcileNowHandler.hookEntered = new CountDownLatch(1);
        reconcileNowHandler.hookGate = new CountDownLatch(1);
        Thread starter = new Thread(() -> streamManager.startStream("hl-reconcile-now-test"), "test-starter");
        starter.start();
        try {
            assertThat(reconcileNowHandler.hookEntered.await(10, TimeUnit.SECONDS)).isTrue();

            mongoTemplate.insert(new Document("item", "during-hook"), "hl_reconcile_now");
            await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(3))
                    .until(() -> reconcileNowHandler.items.isEmpty());
            assertThat(streamManager.isRunning("hl-reconcile-now-test")).isFalse();
            assertThat(checkpointStore.findByStreamName("hl-reconcile-now-test").orElseThrow()
                    .lastProcessedToken()).isEqualTo(FAKE_TOKEN);
        } finally {
            reconcileNowHandler.hookGate.countDown();
        }
        starter.join(TimeUnit.SECONDS.toMillis(15));

        await().atMost(Duration.ofSeconds(5)).until(() -> streamManager.isRunning("hl-reconcile-now-test"));
        // Written after the fresh position was captured → after the resume point → delivered.
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(reconcileNowHandler.items).containsExactly("during-hook"));
    }

    @Test
    void reconcile_throws_streamDoesNotStart_deadTokensKept_nextStartReconcilesAgain() {
        saveExpiredCheckpoint("hl-reconcile-now-test");
        reconcileNowHandler.hookFailure = new IllegalStateException("resync failed");

        assertThatThrownBy(() -> streamManager.startStream("hl-reconcile-now-test"))
                .isInstanceOf(ReconciliationFailedException.class)
                .isInstanceOf(HistoryLostException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("resync failed");
        assertThat(streamManager.isRunning("hl-reconcile-now-test")).isFalse();
        assertThat(checkpointStore.findByStreamName("hl-reconcile-now-test").orElseThrow()
                .lastProcessedToken()).isEqualTo(FAKE_TOKEN);

        reconcileNowHandler.hookFailure = null;
        streamManager.startStream("hl-reconcile-now-test");

        await().atMost(Duration.ofSeconds(5)).until(() -> streamManager.isRunning("hl-reconcile-now-test"));
        assertThat(reconcileNowHandler.reconciliations).hasSize(2);
        assertThat(reconcileNowHandler.reconciliations).extracting(HistoryLostContext::attempt).containsExactly(1, 1);
    }

    @Test
    void reconcile_underOplogStart_boundEndsAtTheOldestOplogEntry_andTheStreamResumes() {
        saveExpiredCheckpoint("hl-reconcile-oplog-test");

        streamManager.startStream("hl-reconcile-oplog-test");

        await().atMost(Duration.ofSeconds(5)).until(() -> streamManager.isRunning("hl-reconcile-oplog-test"));
        assertThat(reconcileOplogHandler.reconciliations).hasSize(1);
        HistoryLostContext ctx = reconcileOplogHandler.reconciliations.get(0);
        assertThat(ctx.strategy()).isEqualTo(OnHistoryLost.RESUME_FROM_OPLOG_START);
        assertThat(ctx.attempt()).isEqualTo(1);
        assertThat(ctx.lostWindowEnd())
                .isAfter(ctx.lostWindowStart().orElseThrow())
                .isBeforeOrEqualTo(Instant.now());
        assertThat(reconcileOplogHandler.checkpointDeadAtHook).containsExactly(true);
    }

    @Test
    void failStrategy_throwsHistoryLostException() {
        saveExpiredCheckpoint("hl-fail-test");

        assertThatThrownBy(() -> streamManager.startStream("hl-fail-test"))
                .isInstanceOf(HistoryLostException.class)
                .hasMessageContaining("hl-fail-test")
                .hasMessageContaining("resume token has expired");
    }

    @Test
    void failStrategy_preservesCheckpoint() {
        saveExpiredCheckpoint("hl-fail-test");

        try { streamManager.startStream("hl-fail-test"); } catch (HistoryLostException ignored) {}

        assertThat(checkpointStore.findByStreamName("hl-fail-test")).isPresent();
    }

    @Test
    void resumeFromNow_startsSuccessfully() {
        saveExpiredCheckpoint("hl-resume-now-test");

        streamManager.startStream("hl-resume-now-test");

        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("hl-resume-now-test"));
    }

    @Test
    void resumeFromNow_receivesNewEvents() {
        saveExpiredCheckpoint("hl-resume-now-test");

        streamManager.startStream("hl-resume-now-test");

        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("hl-resume-now-test"));

        mongoTemplate.insert(new Document("item", "after-recovery"), "hl_resume_now");

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(resumeNowHandler.getEvents()).hasSizeGreaterThanOrEqualTo(1));
    }

    @Test
    void resumeFromOplogStart_startsSuccessfully() {
        saveExpiredCheckpoint("hl-resume-oplog-test");

        streamManager.startStream("hl-resume-oplog-test");

        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("hl-resume-oplog-test"));
    }

    private void saveExpiredCheckpoint(String streamName) {
        Instant past = Instant.now().minusSeconds(86400);
        checkpointStore.save(new io.flowwarden.stream.spi.Checkpoint(
                streamName, null, FAKE_TOKEN, past,
                FAKE_TOKEN, past, Collections.emptyMap()));
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import({
            ImperativeHistoryLostIntegrationTest.HistoryLostFailHandler.class,
            ImperativeHistoryLostIntegrationTest.HistoryLostResumeNowHandler.class,
            ImperativeHistoryLostIntegrationTest.HistoryLostResumeOplogHandler.class,
            ImperativeHistoryLostIntegrationTest.ReconcileNowHandler.class,
            ImperativeHistoryLostIntegrationTest.ReconcileOplogHandler.class
    })
    static class TestApp {}

    /** Records every hook call and whether the dead pair was still in the store at that moment. */
    abstract static class RecordingReconcileHandler {
        final List<String> items = new CopyOnWriteArrayList<>();
        final List<HistoryLostContext> reconciliations = new CopyOnWriteArrayList<>();
        final List<Boolean> checkpointDeadAtHook = new CopyOnWriteArrayList<>();
        volatile CountDownLatch hookEntered;
        volatile CountDownLatch hookGate;
        volatile RuntimeException hookFailure;
        private final CheckpointStore store;

        RecordingReconcileHandler(CheckpointStore store) {
            this.store = store;
        }

        // Annotated methods live on the concrete classes: discovery scans
        // declared methods only, never inherited ones.
        void recordEvent(ChangeStreamContext<Document> ctx) {
            ctx.getFullDocument(Document.class).ifPresent(d -> items.add(d.getString("item")));
        }

        void runReconcile(HistoryLostContext ctx) {
            reconciliations.add(ctx);
            checkpointDeadAtHook.add(store.findByStreamName(ctx.streamName())
                    .map(cp -> FAKE_TOKEN.equals(cp.lastProcessedToken())).orElse(false));
            if (hookEntered != null) {
                hookEntered.countDown();
                try {
                    hookGate.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (hookFailure != null) {
                throw hookFailure;
            }
        }

        void clear() {
            items.clear();
            reconciliations.clear();
            checkpointDeadAtHook.clear();
            hookEntered = null;
            hookGate = null;
            hookFailure = null;
        }
    }

    @ChangeStream(name = "hl-reconcile-now-test", collection = "hl_reconcile_now", autoStart = false)
    @Checkpoint(saveEveryN = 1, onHistoryLost = OnHistoryLost.RESUME_FROM_NOW)
    static class ReconcileNowHandler extends RecordingReconcileHandler {
        ReconcileNowHandler(CheckpointStore store) { super(store); }

        @OnInsert
        void handle(ChangeStreamContext<Document> ctx) { recordEvent(ctx); }

        @Reconcile
        void reconcile(HistoryLostContext ctx) { runReconcile(ctx); }
    }

    @ChangeStream(name = "hl-reconcile-oplog-test", collection = "hl_reconcile_oplog", autoStart = false)
    @Checkpoint(saveEveryN = 1, onHistoryLost = OnHistoryLost.RESUME_FROM_OPLOG_START)
    static class ReconcileOplogHandler extends RecordingReconcileHandler {
        ReconcileOplogHandler(CheckpointStore store) { super(store); }

        @OnInsert
        void handle(ChangeStreamContext<Document> ctx) { recordEvent(ctx); }

        @Reconcile
        void reconcile(HistoryLostContext ctx) { runReconcile(ctx); }
    }

    @ChangeStream(name = "hl-fail-test", collection = "hl_fail", autoStart = false)
    @Checkpoint(saveEveryN = 1, onHistoryLost = OnHistoryLost.FAIL)
    static class HistoryLostFailHandler {
        @OnInsert
        void handle(ChangeStreamContext<?> ctx) {}
    }

    @ChangeStream(name = "hl-resume-now-test", collection = "hl_resume_now", autoStart = false)
    @Checkpoint(saveEveryN = 1, onHistoryLost = OnHistoryLost.RESUME_FROM_NOW)
    static class HistoryLostResumeNowHandler {
        private final List<ChangeStreamContext<?>> events = new CopyOnWriteArrayList<>();

        @OnInsert
        void handle(ChangeStreamContext<?> ctx) {
            events.add(ctx);
        }

        List<ChangeStreamContext<?>> getEvents() { return events; }
        void clear() { events.clear(); }
    }

    @ChangeStream(name = "hl-resume-oplog-test", collection = "hl_resume_oplog", autoStart = false)
    @Checkpoint(saveEveryN = 1, onHistoryLost = OnHistoryLost.RESUME_FROM_OPLOG_START)
    static class HistoryLostResumeOplogHandler {
        private final List<ChangeStreamContext<?>> events = new CopyOnWriteArrayList<>();

        @OnInsert
        void handle(ChangeStreamContext<?> ctx) {
            events.add(ctx);
        }

        List<ChangeStreamContext<?>> getEvents() { return events; }
        void clear() { events.clear(); }
    }
}
