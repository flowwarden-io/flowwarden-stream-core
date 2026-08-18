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
    private static final BsonDocument EXPIRED_TOKEN =
            BsonDocument.parse("{\"_data\": \"DEADBEEF\"}");

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired MongoTemplate mongoTemplate;
    @Autowired ImperativeStreamManager streamManager;
    @Autowired CheckpointStore checkpointStore;
    @Autowired LaggingHandler laggingHandler;
    @Autowired ExpiredTokenHandler expiredTokenHandler;

    private RecordingMetrics metrics;

    @BeforeEach
    void setUp() {
        laggingHandler.reset();
        expiredTokenHandler.clear();
        mongoTemplate.dropCollection(LAG_COLLECTION);
        mongoTemplate.dropCollection(EXPIRED_COLLECTION);
        checkpointStore.delete(LAG_STREAM);
        checkpointStore.delete(EXPIRED_STREAM);
        metrics = new RecordingMetrics();
        FlowWardenMetrics.setProvider(metrics);
    }

    @AfterEach
    void tearDown() {
        laggingHandler.release();
        try { streamManager.stopStream(LAG_STREAM); } catch (Exception ignored) {}
        try { streamManager.stopStream(EXPIRED_STREAM); } catch (Exception ignored) {}
        checkpointStore.delete(LAG_STREAM);
        checkpointStore.delete(EXPIRED_STREAM);
        FlowWardenMetrics.setProvider(StreamMetricsProvider.noOp());
    }

    @Test
    void heartbeatNeverCrossesUndeliveredEvents() throws Exception {
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

        // The timer persists E1 (the last DELIVERED position) on its next tick.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(checkpointStore.findByStreamName(LAG_STREAM)
                        .orElseThrow().lastSeenToken()).isNotEqualTo(preEventToken));
        BsonDocument frozenAt = checkpointStore.findByStreamName(LAG_STREAM)
                .orElseThrow().lastSeenToken();

        // Several ticks later (interval = 1s), the probe must have abstained
        // every time: the persisted position may not move past E1 while E2/E3
        // are undelivered.
        Thread.sleep(4_000);
        assertThat(checkpointStore.findByStreamName(LAG_STREAM).orElseThrow().lastSeenToken())
                .as("the heartbeat must never certify past undelivered events")
                .isEqualTo(frozenAt);

        // Unblock: E2/E3 are delivered and the position advances normally.
        laggingHandler.release();
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(laggingHandler.count()).isEqualTo(3));
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(checkpointStore.findByStreamName(LAG_STREAM)
                        .orElseThrow().lastSeenToken()).isNotEqualTo(frozenAt));
    }

    @Test
    void expiredChainToken_probeFailsLoudly_streamKeepsDelivering_nextEventRecovers() {
        // A checkpoint whose seen token is unusable, with RESUME_FROM_NOW so
        // the stream starts anyway. The stale token stays in the checkpoint
        // (recovery self-repair is a separate issue), so the heartbeat's
        // persisted-fallback chains from it and fails.
        Instant past = Instant.now().minusSeconds(86_400);
        checkpointStore.save(new io.flowwarden.stream.spi.Checkpoint(
                EXPIRED_STREAM, null, EXPIRED_TOKEN, past, EXPIRED_TOKEN, past,
                Collections.emptyMap()));

        streamManager.startStream(EXPIRED_STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(EXPIRED_STREAM));

        // The probe fails, loudly and without writing.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(metrics.probeFailures).isNotEmpty());
        assertThat(checkpointStore.findByStreamName(EXPIRED_STREAM)
                .orElseThrow().lastHeartbeatTimestamp())
                .as("a failed probe must not confirm anything")
                .isNull();

        // The stream itself is unaffected: events are delivered...
        Instant beforeRecovery = Instant.now();
        mongoTemplate.insert(new Document("type", "recovery"), EXPIRED_COLLECTION);
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(expiredTokenHandler.count()).isEqualTo(1));

        // ...and the delivered event re-seeds the chain: the heartbeat recovers.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var cp = checkpointStore.findByStreamName(EXPIRED_STREAM).orElseThrow();
            assertThat(cp.lastSeenToken()).isNotEqualTo(EXPIRED_TOKEN);
            assertThat(cp.lastHeartbeatTimestamp()).isAfterOrEqualTo(beforeRecovery);
        });
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import({LaggingHandler.class, ExpiredTokenHandler.class})
    static class TestApp {}

    @ChangeStream(name = LAG_STREAM, collection = LAG_COLLECTION,
            documentType = Document.class, autoStart = false)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1)
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
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1,
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

        @Override
        public void onHeartbeatProbeFailed(String streamName, Throwable cause) {
            probeFailures.add(cause);
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
