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
import io.flowwarden.stream.ResumeStrategy;
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
 * End-to-end coverage of {@link ResumeStrategy}. Verifies that the resume cascade
 * picks the right primary token (and emits the matching fallback metric) under
 * both {@code PROCESSED_FIRST} (default) and {@code SEEN_FIRST}.
 *
 * <p>Four scenarios:</p>
 * <ul>
 *   <li>{@code PROCESSED_FIRST} happy path — primary {@code lastProcessedToken} is
 *       valid, no fallback metric fires.</li>
 *   <li>{@code PROCESSED_FIRST} fallback-to-seen — {@code lastProcessedToken} aged
 *       out, cascade falls back to {@code lastSeenToken}; {@code onResumeFallbackToSeen}
 *       fires.</li>
 *   <li>{@code SEEN_FIRST} happy path — primary {@code lastSeenToken} is valid, no
 *       fallback metric fires.</li>
 *   <li>{@code SEEN_FIRST} fallback-to-processed — {@code lastSeenToken} aged out,
 *       cascade falls back to {@code lastProcessedToken}; {@code onResumeFallbackToProcessed}
 *       fires.</li>
 * </ul>
 */
@SpringBootTest(classes = ImperativeResumeStrategyIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeResumeStrategyIntegrationTest {

    private static final BsonDocument FAKE_TOKEN =
            BsonDocument.parse("{\"_data\": \"DEADBEEF\"}");

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired MongoTemplate mongoTemplate;
    @Autowired ImperativeStreamManager streamManager;
    @Autowired CheckpointStore checkpointStore;
    @Autowired ProcessedFirstHandler processedFirstHandler;
    @Autowired SeenFirstHandler seenFirstHandler;

    private final RecordingMetricsProvider metrics = new RecordingMetricsProvider();

    @BeforeEach
    void setUp() {
        FlowWardenMetrics.setProvider(metrics);
        metrics.reset();
        processedFirstHandler.clear();
        seenFirstHandler.clear();
    }

    @AfterEach
    void tearDown() {
        for (String name : List.of("rs-processed-first", "rs-seen-first")) {
            try { streamManager.stopStream(name); } catch (Exception ignored) {}
            checkpointStore.delete(name);
        }
        FlowWardenMetrics.setProvider(StreamMetricsProvider.noOp());
    }

    @Test
    void processedFirst_happyPath_resumesWithoutFallback() {
        primeCheckpointFromLiveStream("rs-processed-first", "rs_processed_first",
                processedFirstHandler);

        // Both tokens are real and fresh — primary (processed) must succeed.
        streamManager.startStream("rs-processed-first");

        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("rs-processed-first"));

        // Drive a new event through to prove the cascade picked level 1.
        mongoTemplate.insert(new Document("item", "post-resume"), "rs_processed_first");
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(processedFirstHandler.size()).isPositive());

        assertThat(metrics.fallbackToSeen.get()).isZero();
        assertThat(metrics.fallbackToProcessed.get()).isZero();
        assertThat(metrics.historyLost.get()).isZero();
    }

    @Test
    void processedFirst_fallbackToSeen_emitsFallbackMetric() {
        var seenToken = primeCheckpointFromLiveStream("rs-processed-first",
                "rs_processed_first", processedFirstHandler);

        // Replace lastProcessedToken with a token guaranteed not to be in the oplog.
        // lastSeenToken stays real → cascade should fall back to seen.
        writeCheckpoint("rs-processed-first", seenToken, FAKE_TOKEN);

        streamManager.startStream("rs-processed-first");

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(metrics.fallbackToSeen.get()).isEqualTo(1));
        assertThat(metrics.fallbackToProcessed.get()).isZero();
        assertThat(metrics.historyLost.get()).isZero();
    }

    @Test
    void seenFirst_happyPath_resumesWithoutFallback() {
        primeCheckpointFromLiveStream("rs-seen-first", "rs_seen_first", seenFirstHandler);

        streamManager.startStream("rs-seen-first");

        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("rs-seen-first"));

        mongoTemplate.insert(new Document("item", "post-resume"), "rs_seen_first");
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(seenFirstHandler.size()).isPositive());

        assertThat(metrics.fallbackToSeen.get()).isZero();
        assertThat(metrics.fallbackToProcessed.get()).isZero();
        assertThat(metrics.historyLost.get()).isZero();
    }

    @Test
    void seenFirst_fallbackToProcessed_emitsFallbackMetric() {
        var processedToken = primeCheckpointFromLiveStream("rs-seen-first",
                "rs_seen_first", seenFirstHandler);

        // Replace lastSeenToken with a fake token. lastProcessedToken stays real →
        // SEEN_FIRST cascade should fall back to processed.
        writeCheckpoint("rs-seen-first", FAKE_TOKEN, processedToken);

        streamManager.startStream("rs-seen-first");

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(metrics.fallbackToProcessed.get()).isEqualTo(1));
        assertThat(metrics.fallbackToSeen.get()).isZero();
        assertThat(metrics.historyLost.get()).isZero();
    }

    /**
     * Boots the stream long enough to capture a real resume token, then stops it.
     * Returns the captured token (both {@code lastSeenToken} and {@code lastProcessedToken}
     * end up at this value because {@code saveEveryN=1}).
     */
    private BsonDocument primeCheckpointFromLiveStream(String streamName,
                                                       String collection,
                                                       EventRecorder recorder) {
        streamManager.startStream(streamName);
        await().atMost(Duration.ofSeconds(5)).until(() -> streamManager.isRunning(streamName));

        mongoTemplate.insert(new Document("item", "seed"), collection);

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(recorder.size()).isPositive());

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> {
                    var cp = checkpointStore.findByStreamName(streamName);
                    assertThat(cp).isPresent();
                    assertThat(cp.get().lastProcessedToken()).isNotNull();
                    assertThat(cp.get().lastSeenToken()).isNotNull();
                });

        streamManager.stopStream(streamName);
        recorder.clear();
        metrics.reset();

        return checkpointStore.findByStreamName(streamName).orElseThrow().lastSeenToken();
    }

    private void writeCheckpoint(String streamName, BsonDocument seenToken, BsonDocument processedToken) {
        var now = java.time.Instant.now();
        checkpointStore.save(new io.flowwarden.stream.spi.Checkpoint(
                streamName, null,
                seenToken, now,
                processedToken, now,
                java.util.Collections.emptyMap()));
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import({
            ImperativeResumeStrategyIntegrationTest.ProcessedFirstHandler.class,
            ImperativeResumeStrategyIntegrationTest.SeenFirstHandler.class
    })
    static class TestApp {}

    interface EventRecorder {
        int size();
        void clear();
    }

    @ChangeStream(name = "rs-processed-first", collection = "rs_processed_first", autoStart = false)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1,
            resumeStrategy = ResumeStrategy.PROCESSED_FIRST)
    static class ProcessedFirstHandler implements EventRecorder {
        private final List<ChangeStreamContext<?>> events = new CopyOnWriteArrayList<>();

        @OnInsert
        void handle(ChangeStreamContext<?> ctx) { events.add(ctx); }

        @Override public int size() { return events.size(); }
        @Override public void clear() { events.clear(); }
    }

    @ChangeStream(name = "rs-seen-first", collection = "rs_seen_first", autoStart = false)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1,
            resumeStrategy = ResumeStrategy.SEEN_FIRST)
    static class SeenFirstHandler implements EventRecorder {
        private final List<ChangeStreamContext<?>> events = new CopyOnWriteArrayList<>();

        @OnInsert
        void handle(ChangeStreamContext<?> ctx) { events.add(ctx); }

        @Override public int size() { return events.size(); }
        @Override public void clear() { events.clear(); }
    }

    /**
     * Minimal recording provider. Only tracks the metrics relevant to this test;
     * every other method is a no-op so the real call sites remain unaffected.
     */
    private static final class RecordingMetricsProvider implements StreamMetricsProvider {
        final AtomicInteger fallbackToSeen = new AtomicInteger();
        final AtomicInteger fallbackToProcessed = new AtomicInteger();
        final AtomicInteger historyLost = new AtomicInteger();

        void reset() {
            fallbackToSeen.set(0);
            fallbackToProcessed.set(0);
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
        @Override public void onResumeFallbackToProcessed(String streamName) {
            fallbackToProcessed.incrementAndGet();
        }
        @Override public void onResumeHistoryLost(String streamName) {
            historyLost.incrementAndGet();
        }
    }
}
