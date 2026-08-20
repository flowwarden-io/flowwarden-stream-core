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
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.spi.CheckpointStore;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Safety tests for the heartbeat probe.
 *
 * <p>First: the heartbeat must never cross an undelivered event. With the
 * handler blocked and matching events pending in the main cursor, the chained
 * probe sees those events and abstains — the persisted {@code lastSeenToken}
 * must stay frozen at the last <em>delivered</em> position until the handler
 * resumes. (This pins the original probe-from-"now" design flaw, which would
 * have certified past the pending events.)</p>
 *
 * <p>Second: a probe chained from a token that has aged out of the oplog is a
 * failure, not a silent re-anchor: {@code onHeartbeatProbeFailed} fires,
 * {@code lastHeartbeatTimestamp} does not move, the stream keeps delivering —
 * and the next real event re-seeds the chain, after which the heartbeat
 * recovers.</p>
 */
@SpringBootTest(classes = ImperativeHeartbeatSafetyIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeHeartbeatSafetyIntegrationTest {

    private static final String LAG_STREAM = "hb-lagging";
    private static final String LAG_COLLECTION = "hb_lagging";
    private static final String EXPIRED_STREAM = "hb-expired";
    private static final String EXPIRED_COLLECTION = "hb_expired";
    // Invalid as a resume point, but lexicographically BELOW any real token
    // ("82…"-prefixed) so the monotonicity guard doesn't pin the checkpoint to
    // it after recovery.
    private static final BsonDocument EXPIRED_TOKEN =
            BsonDocument.parse("{\"_data\": \"0000DEAD\"}");

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired MongoTemplate mongoTemplate;
    @Autowired ImperativeStreamManager streamManager;
    @Autowired CheckpointStore checkpointStore;
    @Autowired LaggingHandler laggingHandler;
    @Autowired ExpiredTokenHandler expiredTokenHandler;
    @Autowired OplogStartHandler oplogStartHandler;
    @Autowired FilteredOplogHandler filteredOplogHandler;
    @Autowired RedeliveryHandler redeliveryHandler;
    @Autowired InterruptedRetryHandler interruptedRetryHandler;

    private RecordingMetrics metrics;

    @BeforeEach
    void setUp() {
        laggingHandler.reset();
        expiredTokenHandler.clear();
        oplogStartHandler.clear();
        filteredOplogHandler.reset();
        redeliveryHandler.reset();
        interruptedRetryHandler.reset();
        mongoTemplate.dropCollection(LAG_COLLECTION);
        mongoTemplate.dropCollection(EXPIRED_COLLECTION);
        mongoTemplate.dropCollection(OPLOG_COLLECTION);
        mongoTemplate.dropCollection(FILTERED_OPLOG_COLLECTION);
        mongoTemplate.dropCollection(REDELIVERY_COLLECTION);
        mongoTemplate.dropCollection(INTERRUPT_COLLECTION);
        checkpointStore.delete(LAG_STREAM);
        checkpointStore.delete(EXPIRED_STREAM);
        checkpointStore.delete(OPLOG_STREAM);
        checkpointStore.delete(FILTERED_OPLOG_STREAM);
        checkpointStore.delete(REDELIVERY_STREAM);
        checkpointStore.delete(INTERRUPT_STREAM);
        metrics = new RecordingMetrics();
        FlowWardenMetrics.setProvider(metrics);
    }

    @AfterEach
    void tearDown() {
        laggingHandler.release();
        filteredOplogHandler.releaseFilter();
        redeliveryHandler.crash();
        try { streamManager.stopStream(LAG_STREAM); } catch (Exception ignored) {}
        try { streamManager.stopStream(EXPIRED_STREAM); } catch (Exception ignored) {}
        try { streamManager.stopStream(OPLOG_STREAM); } catch (Exception ignored) {}
        try { streamManager.stopStream(FILTERED_OPLOG_STREAM); } catch (Exception ignored) {}
        try { streamManager.stopStream(REDELIVERY_STREAM); } catch (Exception ignored) {}
        try { streamManager.stopStream(INTERRUPT_STREAM); } catch (Exception ignored) {}
        checkpointStore.delete(LAG_STREAM);
        checkpointStore.delete(EXPIRED_STREAM);
        checkpointStore.delete(OPLOG_STREAM);
        checkpointStore.delete(FILTERED_OPLOG_STREAM);
        checkpointStore.delete(REDELIVERY_STREAM);
        checkpointStore.delete(INTERRUPT_STREAM);
        FlowWardenMetrics.setProvider(StreamMetricsProvider.noOp());
    }

    @Test
    void heartbeatNeverCrossesUnsettledEvents() throws Exception {
        streamManager.startStream(LAG_STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(LAG_STREAM));

        // Capture the pre-event position (bootstrap / early probe).
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(checkpointStore.findByStreamName(LAG_STREAM)
                        .orElseThrow().lastSeenToken()).isNotNull());
        BsonDocument preEventToken = checkpointStore.findByStreamName(LAG_STREAM)
                .orElseThrow().lastSeenToken();

        // E1 blocks the handler (and thus the listener thread); E2 and E3 are
        // inserted immediately after so they are pending in the main cursor,
        // undelivered, before the next heartbeat tick.
        mongoTemplate.insert(new Document("seq", 1).append("mode", "block"), LAG_COLLECTION);
        assertThat(laggingHandler.awaitBlocked(10)).isTrue();
        mongoTemplate.insert(new Document("seq", 2), LAG_COLLECTION);
        mongoTemplate.insert(new Document("seq", 3), LAG_COLLECTION);

        // Settled semantics: E1 was DELIVERED but its handler is in flight,
        // so its token is not even published to the flush. Several ticks
        // later (interval = 1s), the persisted position must still be the
        // pre-event one: frozen BEFORE the in-flight event, not merely
        // before the pending ones — a crash here redelivers E1.
        Thread.sleep(4_000);
        assertThat(checkpointStore.findByStreamName(LAG_STREAM).orElseThrow().lastSeenToken())
                .as("the heartbeat must never certify past an unsettled event")
                .isEqualTo(preEventToken);

        // Unblock: E1/E2/E3 settle and the position advances normally.
        laggingHandler.release();
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(laggingHandler.count()).isEqualTo(3));
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(checkpointStore.findByStreamName(LAG_STREAM)
                        .orElseThrow().lastSeenToken()).isNotEqualTo(preEventToken));
    }

    @Test
    void expiredCheckpoint_resumeFromNow_selfRepairsImmediately_withoutProbeErrorLoop() throws Exception {
        // A checkpoint whose tokens are unusable, with RESUME_FROM_NOW: the
        // recovery must bootstrap a fresh certified position immediately —
        // the heartbeat never consults the expired token, so no repeated
        // probe errors (the pre-redesign failure loop from the PR review).
        Instant past = Instant.now().minusSeconds(86_400);
        Instant beforeStart = Instant.now();
        checkpointStore.save(new io.flowwarden.stream.spi.Checkpoint(
                EXPIRED_STREAM, null, EXPIRED_TOKEN, past, EXPIRED_TOKEN, past,
                Collections.emptyMap()));

        streamManager.startStream(EXPIRED_STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(EXPIRED_STREAM));

        // Self-repair happened at startup, before any event — clean slate:
        // fresh certified seen, expired processed token CLEARED.
        var repaired = checkpointStore.findByStreamName(EXPIRED_STREAM).orElseThrow();
        assertThat(repaired.lastSeenToken()).isNotEqualTo(EXPIRED_TOKEN);
        assertThat(repaired.lastProcessedToken())
                .as("history-lost recovery must clear the dead processed token")
                .isNull();
        assertThat(repaired.lastHeartbeatTimestamp()).isAfterOrEqualTo(beforeStart);

        // Several probe cycles later (idle interval = 1s): no probe failures —
        // the expired token is never chained from again.
        Thread.sleep(3_000);
        assertThat(metrics.probeFailures)
                .as("the recovery must not leave the heartbeat probing a dead token")
                .isEmpty();

        // Second restart, still without any event: must resume directly from
        // the repaired seen position — no history-lost, no fallback warning
        // (a never-recorded processed token is not a degradation), no 286.
        streamManager.stopStream(EXPIRED_STREAM);
        metrics.reset();
        streamManager.startStream(EXPIRED_STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(EXPIRED_STREAM));
        assertThat(metrics.historyLostCount.get())
                .as("the second restart must not replay the history-lost path")
                .isZero();
        assertThat(metrics.fallbackToSeenCount.get())
                .as("resuming from seen with no processed recorded is not a fallback")
                .isZero();
        assertThat(metrics.probeFailures).isEmpty();

        // The stream delivers, and the checkpoint keeps advancing normally.
        mongoTemplate.insert(new Document("type", "recovery"), EXPIRED_COLLECTION);
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(expiredTokenHandler.count()).isEqualTo(1));
    }

    @Test
    void expiredCheckpoint_oplogStart_selfRepairs_secondRestartIsClean() {
        // OPLOG_START nominal path (#58 blocker): dead tokens cleared at
        // recovery, position re-established by the transient chain probing
        // from the recovery operation time (never "now"), second restart
        // resumes directly.
        Instant past = Instant.now().minusSeconds(86_400);
        checkpointStore.save(new io.flowwarden.stream.spi.Checkpoint(
                OPLOG_STREAM, null, EXPIRED_TOKEN, past, EXPIRED_TOKEN, past,
                Collections.emptyMap()));

        streamManager.startStream(OPLOG_STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(OPLOG_STREAM));

        // Dead tokens cleared immediately; the establishment chain then
        // certifies a fresh position once the (empty) replay is exhausted.
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var cp = checkpointStore.findByStreamName(OPLOG_STREAM).orElseThrow();
            assertThat(cp.lastProcessedToken()).isNull();
            assertThat(cp.lastSeenToken())
                    .as("the establishment chain must produce a durable fresh position")
                    .isNotNull()
                    .isNotEqualTo(EXPIRED_TOKEN);
        });

        // Second restart without events: direct resume, no history-lost replay.
        streamManager.stopStream(OPLOG_STREAM);
        metrics.reset();
        streamManager.startStream(OPLOG_STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(OPLOG_STREAM));
        assertThat(metrics.historyLostCount.get()).isZero();
        assertThat(metrics.probeFailures).isEmpty();

        mongoTemplate.insert(new Document("type", "after-repair"), OPLOG_COLLECTION);
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(oplogStartHandler.count()).isEqualTo(1));
    }

    @Test
    void nullProcessed_expiredSeen_takesHistoryLostPathOnce() {
        // Cascade truth-table row: primary absent + secondary expired — the
        // single-validation cascade escalates to onHistoryLost (one probe,
        // one signal), and RESUME_FROM_NOW self-repairs as usual.
        Instant past = Instant.now().minusSeconds(86_400);
        checkpointStore.save(new io.flowwarden.stream.spi.Checkpoint(
                EXPIRED_STREAM, null, EXPIRED_TOKEN, past, null, null,
                Collections.emptyMap()));

        streamManager.startStream(EXPIRED_STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(EXPIRED_STREAM));

        assertThat(metrics.historyLostCount.get()).isEqualTo(1);
        var repaired = checkpointStore.findByStreamName(EXPIRED_STREAM).orElseThrow();
        assertThat(repaired.lastSeenToken()).isNotEqualTo(EXPIRED_TOKEN);

        mongoTemplate.insert(new Document("type", "recovery"), EXPIRED_COLLECTION);
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(expiredTokenHandler.count()).isEqualTo(1));
    }

    @Test
    void oplogStartRecovery_filteredEvent_settlesOnRejection_establishmentChainThenWrites()
            throws Exception {
        // Review blocker's IT, rebuilt to force the scenario: the event is
        // preloaded INTO the replay window before the stream starts, and the
        // @Filter is latch-instrumented. While the filter decision is
        // pending, the event is not settled: the establishment chain (both
        // periodic policies are opted out — it is the only writer) must keep
        // abstaining and the durable recovery marker must stay intact. The
        // rejection settles the event; only then may the chain certify and
        // perform the deferred cleanup (no processed was reacquired).
        Instant past = Instant.now().minusSeconds(86_400);
        checkpointStore.save(new io.flowwarden.stream.spi.Checkpoint(
                FILTERED_OPLOG_STREAM, null, EXPIRED_TOKEN, past, EXPIRED_TOKEN, past,
                Collections.emptyMap()));
        mongoTemplate.insert(new Document("type", "rejected"), FILTERED_OPLOG_COLLECTION);

        streamManager.startStream(FILTERED_OPLOG_STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(FILTERED_OPLOG_STREAM));
        assertThat(filteredOplogHandler.awaitFilterEntered(10)).isTrue();

        // Longer than one chain retry (5s): at least two establishment
        // attempts ran against the undecided event — zero writes allowed.
        Thread.sleep(7_000);
        var beforeDecision = checkpointStore.findByStreamName(FILTERED_OPLOG_STREAM).orElseThrow();
        assertThat(beforeDecision.lastSeenToken())
                .as("no establishment write may precede the filter decision")
                .isEqualTo(EXPIRED_TOKEN);
        assertThat(beforeDecision.lastProcessedToken())
                .as("the durable recovery marker must survive until the event settles")
                .isEqualTo(EXPIRED_TOKEN);

        filteredOplogHandler.releaseFilter();

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var cp = checkpointStore.findByStreamName(FILTERED_OPLOG_STREAM).orElseThrow();
            assertThat(cp.lastSeenToken())
                    .as("the rejected event settled: the establishment chain chains from "
                            + "its token and produces the durable write")
                    .isNotNull()
                    .isNotEqualTo(EXPIRED_TOKEN);
            assertThat(cp.lastProcessedToken())
                    .as("no processed was reacquired: the deferred cleanup applies")
                    .isNull();
        });
    }

    @Test
    void retryBackoffInterrupted_eventNotSettled_redeliveredAutomatically() throws Exception {
        // An interrupted retry backoff exits the processing loop with the
        // outcome undecided: it must NOT settle the event. If it published
        // its token, a probe chained from it could certify past a delivery
        // that was never handled — this locks the at-least-once contract of
        // that exit path. Since the managed-restart work, the crash is
        // followed by an automatic resubscription: the cascade resumes from
        // the persisted position (which, the event being unsettled, never
        // certified past it) and REdelivers the event without any operator
        // action — a strictly stronger version of the old manual-restart
        // assertion.
        streamManager.startStream(INTERRUPT_STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(INTERRUPT_STREAM));

        mongoTemplate.insert(new Document("seq", 1), INTERRUPT_COLLECTION);

        // First delivery fails with the listener thread self-interrupted:
        // the backoff sleep aborts immediately, the listener then dies on
        // the restored interrupt flag — a runtime cursor death.
        await().atMost(Duration.ofSeconds(10))
                .until(() -> interruptedRetryHandler.invocationCount() >= 1);
        assertThat(interruptedRetryHandler.completedCount()).isZero();

        // The managed restart resubscribes and the unsettled event is
        // redelivered; the second invocation completes normally.
        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThat(interruptedRetryHandler.completedCount()).isEqualTo(1));
        assertThat(interruptedRetryHandler.invocationCount())
                .as("the same event was delivered twice: aborted, then settled")
                .isEqualTo(2);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(INTERRUPT_STREAM));
    }

    @Test
    void oplogStartRecovery_inFlightHandlerAtCrash_eventIsRedeliveredOnRestart() throws Exception {
        // The at-least-once race behind the settled-token model: recovery
        // must never certify past an event whose handler is still in flight.
        // If it did, the reset would replace the durable marker and a crash
        // would silently lose the delivery.
        Instant past = Instant.now().minusSeconds(86_400);
        checkpointStore.save(new io.flowwarden.stream.spi.Checkpoint(
                REDELIVERY_STREAM, null, EXPIRED_TOKEN, past, EXPIRED_TOKEN, past,
                Collections.emptyMap()));
        // The event belongs to the replay the recovery promises to deliver.
        mongoTemplate.insert(new Document("seq", 1), REDELIVERY_COLLECTION);

        streamManager.startStream(REDELIVERY_STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(REDELIVERY_STREAM));
        assertThat(redeliveryHandler.awaitEntered(10)).isTrue();

        // Handler in flight ⇒ the event is not settled. Longer than one
        // chain retry (5s): several establishment attempts abstained, the
        // durable marker is intact at "crash" time.
        Thread.sleep(7_000);
        var atCrash = checkpointStore.findByStreamName(REDELIVERY_STREAM).orElseThrow();
        assertThat(atCrash.lastSeenToken()).isEqualTo(EXPIRED_TOKEN);
        assertThat(atCrash.lastProcessedToken()).isEqualTo(EXPIRED_TOKEN);
        assertThat(redeliveryHandler.completedCount()).isZero();

        // Crash mid-handler: the stream dies with the invocation still in
        // flight. The orphaned invocation stays parked until teardown, where
        // it aborts without any store write — like the thread a real crash
        // would simply never resume.
        streamManager.stopStream(REDELIVERY_STREAM);

        // Restart: the intact marker re-enters the recovery and the event is
        // REDELIVERED; this time the handler completes and saveProcessed
        // writes a fresh anchor BEFORE the token settles.
        streamManager.startStream(REDELIVERY_STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(REDELIVERY_STREAM));
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(redeliveryHandler.completedCount()).isEqualTo(1));

        // The establishment write arrives after the fresh processed anchor:
        // the conditional cleanup must preserve it (guard mismatch → seen-only).
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var cp = checkpointStore.findByStreamName(REDELIVERY_STREAM).orElseThrow();
            assertThat(cp.lastSeenToken()).isNotNull().isNotEqualTo(EXPIRED_TOKEN);
            assertThat(cp.lastProcessedToken())
                    .as("a processed anchor reacquired during the replay must never be unset")
                    .isNotNull()
                    .isNotEqualTo(EXPIRED_TOKEN);
        });
    }

    private static final String OPLOG_STREAM = "hb-oplog-start";
    private static final String OPLOG_COLLECTION = "hb_oplog_start";
    private static final String FILTERED_OPLOG_STREAM = "hb-oplog-filtered";
    private static final String FILTERED_OPLOG_COLLECTION = "hb_oplog_filtered";
    private static final String REDELIVERY_STREAM = "hb-oplog-redelivery";
    private static final String REDELIVERY_COLLECTION = "hb_oplog_redelivery";
    private static final String INTERRUPT_STREAM = "hb-interrupted-retry";
    private static final String INTERRUPT_COLLECTION = "hb_interrupted_retry";

    @SpringBootApplication
    @EnableFlowWarden
    @Import({LaggingHandler.class, ExpiredTokenHandler.class, OplogStartHandler.class,
            FilteredOplogHandler.class, RedeliveryHandler.class, InterruptedRetryHandler.class})
    static class TestApp {}

    @ChangeStream(name = INTERRUPT_STREAM, collection = INTERRUPT_COLLECTION,
            documentType = Document.class, autoStart = false)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1, idleHeartbeatIntervalSeconds = 1)
    @io.flowwarden.stream.annotation.RetryPolicy(maxAttempts = 3, initialDelay = "10s")
    static class InterruptedRetryHandler {

        private final java.util.concurrent.atomic.AtomicInteger invocations =
                new java.util.concurrent.atomic.AtomicInteger();
        private final List<Document> completed = new CopyOnWriteArrayList<>();

        @OnInsert
        void handle(ChangeStreamContext<Document> ctx) {
            if (invocations.incrementAndGet() == 1) {
                // Fail AND self-interrupt: the retry backoff sleep throws
                // immediately — the interruption exit is deterministic.
                Thread.currentThread().interrupt();
                throw new IllegalStateException("failure with interrupted backoff");
            }
            ctx.getFullDocument(Document.class).ifPresent(completed::add);
        }

        int invocationCount() {
            return invocations.get();
        }

        int completedCount() {
            return completed.size();
        }

        void reset() {
            invocations.set(0);
            completed.clear();
        }
    }

    @ChangeStream(name = FILTERED_OPLOG_STREAM, collection = FILTERED_OPLOG_COLLECTION,
            documentType = Document.class, autoStart = false)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 0, idleHeartbeatIntervalSeconds = 0,
            onHistoryLost = OnHistoryLost.RESUME_FROM_OPLOG_START)
    static class FilteredOplogHandler {

        private volatile CountDownLatch filterGate = new CountDownLatch(1);
        private volatile CountDownLatch filterEntered = new CountDownLatch(1);

        @OnInsert
        void handle(ChangeStreamContext<Document> ctx) {
        }

        @io.flowwarden.stream.annotation.Filter
        boolean rejectEverything(ChangeStreamContext<Document> ctx) {
            filterEntered.countDown();
            try {
                filterGate.await(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return false;
        }

        boolean awaitFilterEntered(int seconds) throws InterruptedException {
            return filterEntered.await(seconds, TimeUnit.SECONDS);
        }

        void releaseFilter() {
            filterGate.countDown();
        }

        void reset() {
            filterGate.countDown();
            filterGate = new CountDownLatch(1);
            filterEntered = new CountDownLatch(1);
        }
    }

    @ChangeStream(name = REDELIVERY_STREAM, collection = REDELIVERY_COLLECTION,
            documentType = Document.class, autoStart = false)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1, idleHeartbeatIntervalSeconds = 1,
            onHistoryLost = OnHistoryLost.RESUME_FROM_OPLOG_START)
    static class RedeliveryHandler {

        private final java.util.concurrent.atomic.AtomicInteger invocations =
                new java.util.concurrent.atomic.AtomicInteger();
        private final java.util.concurrent.atomic.AtomicBoolean crashed =
                new java.util.concurrent.atomic.AtomicBoolean(false);
        private volatile CountDownLatch gate = new CountDownLatch(1);
        private volatile CountDownLatch entered = new CountDownLatch(1);
        private final List<Document> completed = new CopyOnWriteArrayList<>();

        @OnInsert
        void handle(ChangeStreamContext<Document> ctx) throws InterruptedException {
            if (invocations.incrementAndGet() == 1) {
                entered.countDown();
                gate.await(60, TimeUnit.SECONDS);
                if (crashed.get()) {
                    // The JVM died while this handler was in flight: no store
                    // write of any kind may result from this invocation.
                    throw new IllegalStateException("simulated crash mid-handler");
                }
            }
            ctx.getFullDocument(Document.class).ifPresent(completed::add);
        }

        boolean awaitEntered(int seconds) throws InterruptedException {
            return entered.await(seconds, TimeUnit.SECONDS);
        }

        void crash() {
            crashed.set(true);
            gate.countDown();
        }

        int completedCount() {
            return completed.size();
        }

        void reset() {
            gate.countDown();
            gate = new CountDownLatch(1);
            entered = new CountDownLatch(1);
            crashed.set(false);
            invocations.set(0);
            completed.clear();
        }
    }


    @ChangeStream(name = OPLOG_STREAM, collection = OPLOG_COLLECTION,
            documentType = Document.class, autoStart = false)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1, idleHeartbeatIntervalSeconds = 1,
            onHistoryLost = OnHistoryLost.RESUME_FROM_OPLOG_START)
    static class OplogStartHandler {

        private final List<Document> events = new CopyOnWriteArrayList<>();

        @OnInsert
        void handle(ChangeStreamContext<Document> ctx) {
            ctx.getFullDocument(Document.class).ifPresent(events::add);
        }

        int count() { return events.size(); }
        void clear() { events.clear(); }
    }

    @ChangeStream(name = LAG_STREAM, collection = LAG_COLLECTION,
            documentType = Document.class, autoStart = false)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1, idleHeartbeatIntervalSeconds = 1)
    static class LaggingHandler {

        private volatile CountDownLatch blockGate = new CountDownLatch(1);
        private final CountDownLatch blockedSignal = new CountDownLatch(1);
        private final List<Document> events = new CopyOnWriteArrayList<>();

        @OnInsert
        void handle(ChangeStreamContext<Document> ctx) throws InterruptedException {
            Document doc = ctx.getFullDocument(Document.class).orElse(new Document());
            events.add(doc);
            if ("block".equals(doc.getString("mode"))) {
                blockedSignal.countDown();
                blockGate.await(30, TimeUnit.SECONDS);
            }
        }

        boolean awaitBlocked(int seconds) throws InterruptedException {
            return blockedSignal.await(seconds, TimeUnit.SECONDS);
        }

        void release() { blockGate.countDown(); }
        int count() { return events.size(); }

        void reset() {
            blockGate.countDown();
            blockGate = new CountDownLatch(1);
            events.clear();
        }
    }

    @ChangeStream(name = EXPIRED_STREAM, collection = EXPIRED_COLLECTION,
            documentType = Document.class, autoStart = false)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1, idleHeartbeatIntervalSeconds = 1,
            onHistoryLost = OnHistoryLost.RESUME_FROM_NOW)
    static class ExpiredTokenHandler {

        private final List<Document> events = new CopyOnWriteArrayList<>();

        @OnInsert
        void handle(ChangeStreamContext<Document> ctx) {
            ctx.getFullDocument(Document.class).ifPresent(events::add);
        }

        int count() { return events.size(); }
        void clear() { events.clear(); }
    }

    private static final class RecordingMetrics implements StreamMetricsProvider {
        final List<Throwable> probeFailures = new CopyOnWriteArrayList<>();
        final java.util.concurrent.atomic.AtomicInteger historyLostCount =
                new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.atomic.AtomicInteger fallbackToSeenCount =
                new java.util.concurrent.atomic.AtomicInteger();

        void reset() {
            probeFailures.clear();
            historyLostCount.set(0);
            fallbackToSeenCount.set(0);
        }

        @Override
        public void onHeartbeatProbeFailed(String streamName, Throwable cause) {
            probeFailures.add(cause);
        }

        @Override
        public void onResumeHistoryLost(String streamName) {
            historyLostCount.incrementAndGet();
        }

        @Override
        public void onResumeFallbackToSeen(String streamName) {
            fallbackToSeenCount.incrementAndGet();
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
