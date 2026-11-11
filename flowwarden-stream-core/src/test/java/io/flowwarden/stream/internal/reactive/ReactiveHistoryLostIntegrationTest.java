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
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import io.flowwarden.stream.test.SharedMongoContainer;
import reactor.core.publisher.Mono;

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

@SpringBootTest(classes = ReactiveHistoryLostIntegrationTest.TestApp.class)
@ActiveProfiles("test-webflux")
class ReactiveHistoryLostIntegrationTest {

    private static final BsonDocument FAKE_TOKEN =
            BsonDocument.parse("{\"_data\": \"DEADBEEF\"}");

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired ReactiveMongoTemplate reactiveMongoTemplate;
    @Autowired ReactiveStreamManager streamManager;
    @Autowired CheckpointStore checkpointStore;
    @Autowired HistoryLostResumeNowHandler resumeNowHandler;
    @Autowired HistoryLostResumeOplogHandler resumeOplogHandler;
    @Autowired ReconcileNowHandler reconcileNowHandler;
    @Autowired ReconcileOplogHandler reconcileOplogHandler;

    @AfterEach
    void tearDown() {
        try { streamManager.stopStream("hl-fail-reactive-test"); } catch (Exception ignored) {}
        try { streamManager.stopStream("hl-resume-now-reactive-test"); } catch (Exception ignored) {}
        try { streamManager.stopStream("hl-resume-oplog-reactive-test"); } catch (Exception ignored) {}
        try { streamManager.stopStream("hl-reconcile-now-reactive-test"); } catch (Exception ignored) {}
        try { streamManager.stopStream("hl-reconcile-oplog-reactive-test"); } catch (Exception ignored) {}
        checkpointStore.delete("hl-fail-reactive-test");
        checkpointStore.delete("hl-resume-now-reactive-test");
        checkpointStore.delete("hl-resume-oplog-reactive-test");
        checkpointStore.delete("hl-reconcile-now-reactive-test");
        checkpointStore.delete("hl-reconcile-oplog-reactive-test");
        resumeNowHandler.clear();
        resumeOplogHandler.clear();
        reconcileNowHandler.clear();
        reconcileOplogHandler.clear();
    }

    // --- @Reconcile (Mono<Void>) ---

    @Test
    void reconcile_monoIsAwaitedBeforeResume_withTheLostWindowBound_whileTheCheckpointIsStillDead() {
        Instant before = Instant.now();
        saveExpiredCheckpoint("hl-reconcile-now-reactive-test");

        streamManager.startStream("hl-reconcile-now-reactive-test");

        await().atMost(Duration.ofSeconds(5)).until(() -> streamManager.isRunning("hl-reconcile-now-reactive-test"));
        assertThat(reconcileNowHandler.reconciliations).hasSize(1);
        HistoryLostContext ctx = reconcileNowHandler.reconciliations.get(0);
        assertThat(ctx.strategy()).isEqualTo(OnHistoryLost.RESUME_FROM_NOW);
        assertThat(ctx.attempt()).isEqualTo(1);
        assertThat(ctx.lostWindowStart().orElseThrow())
                .isBetween(before.minusSeconds(86_400 + 60), before.minusSeconds(86_400 - 60));
        assertThat(reconcileNowHandler.checkpointDeadAtHook).containsExactly(true);
        assertThat(checkpointStore.findByStreamName("hl-reconcile-now-reactive-test").orElseThrow()
                .lastProcessedToken()).isNotEqualTo(FAKE_TOKEN);

        reactiveMongoTemplate.insert(new Document("item", "after-reconcile"), "hl_reconcile_now_reactive").block();
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(reconcileNowHandler.items).contains("after-reconcile"));
    }

    @Test
    void reconcile_isABarrier_nothingDeliveredWhileTheMonoRuns() throws Exception {
        saveExpiredCheckpoint("hl-reconcile-now-reactive-test");
        reconcileNowHandler.hookEntered = new CountDownLatch(1);
        reconcileNowHandler.hookGate = new CountDownLatch(1);
        Thread starter = new Thread(() -> streamManager.startStream("hl-reconcile-now-reactive-test"), "test-starter");
        starter.start();
        try {
            assertThat(reconcileNowHandler.hookEntered.await(10, TimeUnit.SECONDS)).isTrue();

            reactiveMongoTemplate.insert(new Document("item", "during-hook"), "hl_reconcile_now_reactive").block();
            await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(3))
                    .until(() -> reconcileNowHandler.items.isEmpty());
            assertThat(streamManager.isRunning("hl-reconcile-now-reactive-test")).isFalse();
        } finally {
            reconcileNowHandler.hookGate.countDown();
        }
        starter.join(TimeUnit.SECONDS.toMillis(15));

        await().atMost(Duration.ofSeconds(5)).until(() -> streamManager.isRunning("hl-reconcile-now-reactive-test"));
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(reconcileNowHandler.items).containsExactly("during-hook"));
    }

    @Test
    void reconcile_monoErrors_streamDoesNotStart_deadTokensKept_nextStartReconcilesAgain() {
        saveExpiredCheckpoint("hl-reconcile-now-reactive-test");
        reconcileNowHandler.hookFailure = new IllegalStateException("resync failed");

        assertThatThrownBy(() -> streamManager.startStream("hl-reconcile-now-reactive-test"))
                .isInstanceOf(ReconciliationFailedException.class)
                .isInstanceOf(HistoryLostException.class)
                .hasMessageContaining("resync failed");
        assertThat(streamManager.isRunning("hl-reconcile-now-reactive-test")).isFalse();
        assertThat(checkpointStore.findByStreamName("hl-reconcile-now-reactive-test").orElseThrow()
                .lastProcessedToken()).isEqualTo(FAKE_TOKEN);

        reconcileNowHandler.hookFailure = null;
        streamManager.startStream("hl-reconcile-now-reactive-test");

        await().atMost(Duration.ofSeconds(5)).until(() -> streamManager.isRunning("hl-reconcile-now-reactive-test"));
        assertThat(reconcileNowHandler.reconciliations).hasSize(2);
    }

    @Test
    void reconcile_underOplogStart_boundEndsAtTheOldestOplogEntry_andTheStreamResumes() {
        saveExpiredCheckpoint("hl-reconcile-oplog-reactive-test");

        streamManager.startStream("hl-reconcile-oplog-reactive-test");

        await().atMost(Duration.ofSeconds(5)).until(() -> streamManager.isRunning("hl-reconcile-oplog-reactive-test"));
        assertThat(reconcileOplogHandler.reconciliations).hasSize(1);
        HistoryLostContext ctx = reconcileOplogHandler.reconciliations.get(0);
        assertThat(ctx.strategy()).isEqualTo(OnHistoryLost.RESUME_FROM_OPLOG_START);
        assertThat(ctx.lostWindowEnd())
                .isAfter(ctx.lostWindowStart().orElseThrow())
                .isBeforeOrEqualTo(Instant.now());
        assertThat(reconcileOplogHandler.checkpointDeadAtHook).containsExactly(true);
    }

    @Test
    void failStrategy_throwsHistoryLostException() {
        saveExpiredCheckpoint("hl-fail-reactive-test");

        assertThatThrownBy(() -> streamManager.startStream("hl-fail-reactive-test"))
                .isInstanceOf(HistoryLostException.class)
                .hasMessageContaining("hl-fail-reactive-test")
                .hasMessageContaining("resume token has expired");
    }

    @Test
    void failStrategy_preservesCheckpoint() {
        saveExpiredCheckpoint("hl-fail-reactive-test");

        try { streamManager.startStream("hl-fail-reactive-test"); } catch (HistoryLostException ignored) {}

        assertThat(checkpointStore.findByStreamName("hl-fail-reactive-test")).isPresent();
    }

    @Test
    void resumeFromNow_startsSuccessfully() {
        saveExpiredCheckpoint("hl-resume-now-reactive-test");

        streamManager.startStream("hl-resume-now-reactive-test");

        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("hl-resume-now-reactive-test"));
    }

    @Test
    void resumeFromNow_receivesNewEvents() {
        saveExpiredCheckpoint("hl-resume-now-reactive-test");

        streamManager.startStream("hl-resume-now-reactive-test");

        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("hl-resume-now-reactive-test"));

        reactiveMongoTemplate.insert(new Document("item", "after-recovery"), "hl_resume_now_reactive").block();

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(resumeNowHandler.getEvents()).hasSizeGreaterThanOrEqualTo(1));
    }

    @Test
    void resumeFromOplogStart_startsSuccessfully() {
        saveExpiredCheckpoint("hl-resume-oplog-reactive-test");

        streamManager.startStream("hl-resume-oplog-reactive-test");

        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("hl-resume-oplog-reactive-test"));
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
            ReactiveHistoryLostIntegrationTest.HistoryLostFailHandler.class,
            ReactiveHistoryLostIntegrationTest.HistoryLostResumeNowHandler.class,
            ReactiveHistoryLostIntegrationTest.HistoryLostResumeOplogHandler.class,
            ReactiveHistoryLostIntegrationTest.ReconcileNowHandler.class,
            ReactiveHistoryLostIntegrationTest.ReconcileOplogHandler.class
    })
    static class TestApp {}

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
        Mono<Void> recordEvent(ChangeStreamContext<Document> ctx) {
            ctx.getFullDocument(Document.class).ifPresent(d -> items.add(d.getString("item")));
            return Mono.empty();
        }

        Mono<Void> runReconcile(HistoryLostContext ctx) {
            return Mono.fromRunnable(() -> {
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
            });
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

    @ChangeStream(name = "hl-reconcile-now-reactive-test", collection = "hl_reconcile_now_reactive", autoStart = false)
    @Checkpoint(saveEveryN = 1, onHistoryLost = OnHistoryLost.RESUME_FROM_NOW)
    static class ReconcileNowHandler extends RecordingReconcileHandler {
        ReconcileNowHandler(CheckpointStore store) { super(store); }

        @OnInsert
        Mono<Void> handle(ChangeStreamContext<Document> ctx) { return recordEvent(ctx); }

        @Reconcile
        Mono<Void> reconcile(HistoryLostContext ctx) { return runReconcile(ctx); }
    }

    @ChangeStream(name = "hl-reconcile-oplog-reactive-test", collection = "hl_reconcile_oplog_reactive", autoStart = false)
    @Checkpoint(saveEveryN = 1, onHistoryLost = OnHistoryLost.RESUME_FROM_OPLOG_START)
    static class ReconcileOplogHandler extends RecordingReconcileHandler {
        ReconcileOplogHandler(CheckpointStore store) { super(store); }

        @OnInsert
        Mono<Void> handle(ChangeStreamContext<Document> ctx) { return recordEvent(ctx); }

        @Reconcile
        Mono<Void> reconcile(HistoryLostContext ctx) { return runReconcile(ctx); }
    }

    @ChangeStream(name = "hl-fail-reactive-test", collection = "hl_fail_reactive", autoStart = false)
    @Checkpoint(saveEveryN = 1, onHistoryLost = OnHistoryLost.FAIL)
    static class HistoryLostFailHandler {
        @OnInsert
        Mono<Void> handle(ChangeStreamContext<?> ctx) {
            return Mono.empty();
        }
    }

    @ChangeStream(name = "hl-resume-now-reactive-test", collection = "hl_resume_now_reactive", autoStart = false)
    @Checkpoint(saveEveryN = 1, onHistoryLost = OnHistoryLost.RESUME_FROM_NOW)
    static class HistoryLostResumeNowHandler {
        private final List<ChangeStreamContext<?>> events = new CopyOnWriteArrayList<>();

        @OnInsert
        Mono<Void> handle(ChangeStreamContext<?> ctx) {
            events.add(ctx);
            return Mono.empty();
        }

        List<ChangeStreamContext<?>> getEvents() { return events; }
        void clear() { events.clear(); }
    }

    @ChangeStream(name = "hl-resume-oplog-reactive-test", collection = "hl_resume_oplog_reactive", autoStart = false)
    @Checkpoint(saveEveryN = 1, onHistoryLost = OnHistoryLost.RESUME_FROM_OPLOG_START)
    static class HistoryLostResumeOplogHandler {
        private final List<ChangeStreamContext<?>> events = new CopyOnWriteArrayList<>();

        @OnInsert
        Mono<Void> handle(ChangeStreamContext<?> ctx) {
            events.add(ctx);
            return Mono.empty();
        }

        List<ChangeStreamContext<?>> getEvents() { return events; }
        void clear() { events.clear(); }
    }
}
