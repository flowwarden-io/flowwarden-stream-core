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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end coverage of the fixed resume cascade:
 * {@code lastProcessedToken} → {@code lastSeenToken} → {@code onHistoryLost}.
 *
 * <ul>
 *   <li>Happy path — the processed anchor is valid, no fallback metric.</li>
 *   <li>Fallback-to-seen — the processed anchor is unusable, the cascade
 *       falls back to the certified seen position;
 *       {@code onResumeFallbackToSeen} fires.</li>
 *   <li>Processed never recorded — resuming from the seen position is not a
 *       degradation (typical after a history-lost self-repair): no fallback
 *       metric.</li>
 * </ul>
 */
@SpringBootTest(classes = ImperativeResumeCascadeIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeResumeCascadeIntegrationTest {

    private static final String STREAM = "rs-cascade";
    private static final String COLLECTION = "rs_cascade";
    private static final BsonDocument FAKE_TOKEN =
            BsonDocument.parse("{\"_data\": \"DEADBEEF\"}");

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired MongoTemplate mongoTemplate;
    @Autowired ImperativeStreamManager streamManager;
    @Autowired CheckpointStore checkpointStore;
    @Autowired CascadeHandler handler;

    private final RecordingMetricsProvider metrics = new RecordingMetricsProvider();

    @BeforeEach
    void setUp() {
        FlowWardenMetrics.setProvider(metrics);
        metrics.reset();
        handler.clear();
    }

    @AfterEach
    void tearDown() {
        try { streamManager.stopStream(STREAM); } catch (Exception ignored) {}
        checkpointStore.delete(STREAM);
        FlowWardenMetrics.setProvider(StreamMetricsProvider.noOp());
    }

    @Test
    void happyPath_resumesFromProcessed_withoutFallback() {
        primeCheckpointFromLiveStream();

        streamManager.startStream(STREAM);
        await().atMost(Duration.ofSeconds(5)).until(() -> streamManager.isRunning(STREAM));

        // Drive a new event through to prove the cascade picked level 1.
        mongoTemplate.insert(new Document("item", "post-resume"), COLLECTION);
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(handler.size()).isPositive());

        assertThat(metrics.fallbackToSeen.get()).isZero();
        assertThat(metrics.historyLost.get()).isZero();
    }

    @Test
    void agedOutProcessed_fallsBackToTheCertifiedSeen_emitsFallbackMetric() {
        BsonDocument certifiedSeen = primeCheckpointFromLiveStream();

        // Replace lastProcessedToken with a token guaranteed not to be in the
        // oplog. lastSeenToken stays the real certified PBRT → level 2.
        writeCheckpoint(certifiedSeen, FAKE_TOKEN);

        streamManager.startStream(STREAM);

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(metrics.fallbackToSeen.get()).isEqualTo(1));
        assertThat(metrics.historyLost.get()).isZero();

        // The stream genuinely runs from the certified position.
        await().atMost(Duration.ofSeconds(5)).until(() -> streamManager.isRunning(STREAM));
        mongoTemplate.insert(new Document("item", "post-fallback"), COLLECTION);
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(handler.size()).isPositive());
    }

    @Test
    void processedNeverRecorded_resumesFromSeen_withoutDegradationMetric() {
        BsonDocument certifiedSeen = primeCheckpointFromLiveStream();

        // Typical post-self-repair shape: a certified seen, no processed pair.
        writeCheckpoint(certifiedSeen, null);

        streamManager.startStream(STREAM);

        await().atMost(Duration.ofSeconds(5)).until(() -> streamManager.isRunning(STREAM));
        mongoTemplate.insert(new Document("item", "post-repair"), COLLECTION);
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(handler.size()).isPositive());

        assertThat(metrics.fallbackToSeen.get())
                .as("a missing processed anchor is not a degradation")
                .isZero();
        assertThat(metrics.historyLost.get()).isZero();
    }

    @Test
    void pathologicalCheckpoint_seenFarBehindProcessed_healsAtFirstCertification() {
        // The shape left behind by the pre-redesign regression (#74): a seen
        // position frozen far BEHIND a current processed anchor. The fixed
        // cascade resumes from the processed anchor regardless, and the
        // first idle certification simply replaces the stale seen — no
        // catch-up state, no comparison, no operator action.
        BsonDocument staleSeen = primeCheckpointFromLiveStream();

        // Advance the stream well past the stale certification.
        streamManager.startStream(STREAM);
        await().atMost(Duration.ofSeconds(5)).until(() -> streamManager.isRunning(STREAM));
        for (int i = 0; i < 3; i++) {
            mongoTemplate.insert(new Document("item", "advance-" + i), COLLECTION);
        }
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(handler.size()).isGreaterThanOrEqualTo(3));
        streamManager.stopStream(STREAM);
        BsonDocument freshProcessed = checkpointStore.findByStreamName(STREAM)
                .orElseThrow().lastProcessedToken();

        writeCheckpoint(staleSeen, freshProcessed);
        metrics.reset();
        handler.clear();

        streamManager.startStream(STREAM);
        await().atMost(Duration.ofSeconds(5)).until(() -> streamManager.isRunning(STREAM));

        // Resumed at level 1, and the stale seen heals at the first idle
        // certification (idleHeartbeatIntervalSeconds = 1).
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            var cp = checkpointStore.findByStreamName(STREAM).orElseThrow();
            assertThat(cp.lastSeenToken())
                    .as("the stale seen must be replaced by a fresh certification")
                    .isNotEqualTo(staleSeen);
            assertThat(cp.lastHeartbeatTimestamp()).isNotNull();
        });
        assertThat(metrics.fallbackToSeen.get()).isZero();
        assertThat(metrics.historyLost.get()).isZero();
    }

    /**
     * Boots the stream long enough to anchor a settlement
     * ({@code lastProcessedToken}, {@code saveEveryN=1}) and let the idle
     * probe certify a seen position ({@code idleHeartbeatIntervalSeconds=1}),
     * then stops it. Returns the certified seen token.
     */
    private BsonDocument primeCheckpointFromLiveStream() {
        streamManager.startStream(STREAM);
        await().atMost(Duration.ofSeconds(5)).until(() -> streamManager.isRunning(STREAM));

        mongoTemplate.insert(new Document("item", "seed"), COLLECTION);
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(handler.size()).isPositive());

        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> {
                    var cp = checkpointStore.findByStreamName(STREAM);
                    assertThat(cp).isPresent();
                    assertThat(cp.get().lastProcessedToken()).isNotNull();
                    assertThat(cp.get().lastSeenToken())
                            .as("the idle probe certifies the seen position")
                            .isNotNull();
                });

        streamManager.stopStream(STREAM);
        handler.clear();
        metrics.reset();

        return checkpointStore.findByStreamName(STREAM).orElseThrow().lastSeenToken();
    }

    private void writeCheckpoint(BsonDocument seenToken, BsonDocument processedToken) {
        var now = java.time.Instant.now();
        checkpointStore.save(new io.flowwarden.stream.spi.Checkpoint(
                STREAM, null,
                seenToken, now,
                processedToken, processedToken != null ? now : null,
                java.util.Collections.emptyMap()));
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import(ImperativeResumeCascadeIntegrationTest.CascadeHandler.class)
    static class TestApp {}

    @ChangeStream(name = STREAM, collection = COLLECTION, autoStart = false)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1, idleHeartbeatIntervalSeconds = 1)
    static class CascadeHandler {
        private final List<ChangeStreamContext<?>> events = new CopyOnWriteArrayList<>();

        @OnInsert
        void handle(ChangeStreamContext<?> ctx) { events.add(ctx); }

        int size() { return events.size(); }
        void clear() { events.clear(); }
    }

    /**
     * Minimal recording provider. Only tracks the metrics relevant to this test;
     * every other method is a no-op so the real call sites remain unaffected.
     */
    private static final class RecordingMetricsProvider implements StreamMetricsProvider {
        final AtomicInteger fallbackToSeen = new AtomicInteger();
        final AtomicInteger historyLost = new AtomicInteger();

        void reset() {
            fallbackToSeen.set(0);
            historyLost.set(0);
        }

        @Override public void onStreamStarted(String streamName,
                                              io.flowwarden.stream.spi.StreamConfiguration config) {}
        @Override public void onEventReceived(String streamName,
                                              io.flowwarden.stream.spi.ChangeEventMetadata metadata) {}
        @Override public void onEventProcessed(String streamName, long durationNanos, boolean success) {}
        @Override public void onEventError(String streamName, Throwable error, boolean willRetry,
                                           int attemptNumber,
                                           io.flowwarden.stream.spi.ChangeEventMetadata metadata) {}
        @Override public void onCheckpoint(String streamName, String resumeToken) {}
        @Override public void onBufferStatus(String streamName, int currentSize, int maxSize) {}
        @Override public void onBackpressure(String streamName,
                                             io.flowwarden.stream.spi.BackpressureAction action) {}
        @Override public void onEventSentToDlq(String streamName) {}
        @Override public void onOplogStats(double logLengthHours, String status) {}

        @Override public void onResumeFallbackToSeen(String streamName) {
            fallbackToSeen.incrementAndGet();
        }
        @Override public void onResumeHistoryLost(String streamName) {
            historyLost.incrementAndGet();
        }
    }
}
