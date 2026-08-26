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

import com.mongodb.MongoWriteConcernException;
import com.mongodb.ServerAddress;
import com.mongodb.bulk.WriteConcernError;
import io.flowwarden.stream.ChangeStreamContext;
import io.flowwarden.stream.FlowWardenMetrics;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.spi.BackpressureAction;
import io.flowwarden.stream.spi.ChangeEventMetadata;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.StreamConfiguration;
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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = ReactiveCheckpointFailureIntegrationTest.TestApp.class)
@ActiveProfiles("test-webflux")
class ReactiveCheckpointFailureIntegrationTest {

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired
    ReactiveMongoTemplate reactiveMongoTemplate;

    @Autowired
    FailingCheckpointStore failingStore;

    @Autowired
    PostHandlerStream postHandlerStream;

    @Autowired
    ManualSaveStream manualSaveStream;

    @Autowired
    TimerStream timerStream;

    @Autowired
    ReactiveStreamManager streamManager;

    private RecordingMetricsProvider metrics;

    @BeforeEach
    void setUp() {
        metrics = new RecordingMetricsProvider();
        FlowWardenMetrics.setProvider(metrics);
        failingStore.reset();
        postHandlerStream.clear();
        manualSaveStream.clear();
        timerStream.clear();
    }

    @AfterEach
    void tearDown() {
        FlowWardenMetrics.setProvider(StreamMetricsProvider.noOp());
    }

    @Test
    void streamContinuesWhenPostHandlerCheckpointWriteFails() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("rea-cp-fail-post-handler"));

        failingStore.failNextNCalls("saveProcessed:rea-cp-fail-post-handler", 2);

        reactiveMongoTemplate.insert(new Document("item", "A"), "rea_cp_fail_post_handler").block();
        reactiveMongoTemplate.insert(new Document("item", "B"), "rea_cp_fail_post_handler").block();
        reactiveMongoTemplate.insert(new Document("item", "C"), "rea_cp_fail_post_handler").block();

        // Critical: the reactive pipeline must NOT terminate on a checkpoint
        // failure escaping the doOnSuccess callback.
        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() ->
                        assertThat(postHandlerStream.getEvents()).hasSize(3));

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    List<Throwable> failures = metrics.checkpointFailures("rea-cp-fail-post-handler");
                    assertThat(failures).hasSize(2);
                    assertThat(failures).allMatch(t -> t instanceof MongoWriteConcernException);
                });

        // Store-confirmed oracle, exact with or without restarts/replays:
        // every write the store actually confirmed emits exactly one
        // onCheckpoint. An optimistic-emit regression makes the emissions
        // exceed the confirmed writes (failed writes reporting success) —
        // that inequality can never be produced by legitimate replays,
        // which increment both sides together.
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(
                        metrics.checkpointSuccesses("rea-cp-fail-post-handler"))
                        .isGreaterThanOrEqualTo(1));
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(
                        metrics.checkpointSuccesses("rea-cp-fail-post-handler"))
                        .as("failed checkpoint writes must not report success")
                        .isEqualTo(failingStore.confirmedSaveProcessed("rea-cp-fail-post-handler")));
    }

    @Test
    void failedManualSave_isNotDeclaredDurable_theFlushRetriesTheToken() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("rea-cp-fail-manual"));

        // saveCheckpointNow() routes through the targeted saveProcessed
        // write. A SINGLE event whose manual save fails: the failure must
        // not be recorded as a manual save — the anchor stays dirty and a
        // later flush genuinely retries the token.
        failingStore.failNextNCalls("saveProcessed:rea-cp-fail-manual", 1);

        reactiveMongoTemplate.insert(new Document("item", "M1"), "rea_cp_fail_manual").block();

        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() ->
                        assertThat(manualSaveStream.getEvents()).hasSize(1));
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    List<Throwable> failures = metrics.checkpointFailures("rea-cp-fail-manual");
                    assertThat(failures).hasSize(1);
                    assertThat(failures.get(0)).isInstanceOf(MongoWriteConcernException.class);
                });

        // The count threshold is out of reach (saveEveryN=100): only the
        // time threshold can repair the anchor — and it must.
        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> {
                    var cp = failingStore.findByStreamName("rea-cp-fail-manual");
                    assertThat(cp).isPresent();
                    assertThat(cp.get().lastProcessedToken())
                            .as("the flush must retry the token the manual save failed to persist")
                            .isNotNull();
                });
    }

    @Test
    void timerKeepsSchedulingWhenSaveProcessedFails() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("rea-cp-fail-timer"));

        // The timer anchors the processed token (the count threshold is out
        // of reach): every write throws for a long stretch — the scheduler
        // must not give up on the first error.
        failingStore.failNextNCalls("saveProcessed:rea-cp-fail-timer", 100);

        reactiveMongoTemplate.insert(new Document("item", "T1"), "rea_cp_fail_timer").block();

        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() ->
                        assertThat(timerStream.getEvents()).hasSizeGreaterThanOrEqualTo(1));

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(metrics.checkpointFailures("rea-cp-fail-timer"))
                                .hasSizeGreaterThanOrEqualTo(2));
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import({PostHandlerStream.class, ManualSaveStream.class, TimerStream.class,
            ReactiveCheckpointFailureIntegrationTest.FailingStoreConfig.class})
    static class TestApp {
    }

    @Configuration
    static class FailingStoreConfig {
        @Bean
        @Primary
        FailingCheckpointStore failingCheckpointStore(MongoTemplate template) {
            return new FailingCheckpointStore(
                    new io.flowwarden.stream.internal.checkpoint.MongoCheckpointStore(template));
        }
    }

    // Both periodic policies opted out: the interval flush AND the probe
    // certifications (diagnostic probe shortly after start, idle probes)
    // all emit onCheckpoint when they persist a position — through saveSeen,
    // which the store-confirmed counter does not track. Disabling them
    // isolates the saveProcessed emissions so they can be compared exactly
    // to the store-confirmed write count.
    @ChangeStream(name = "rea-cp-fail-post-handler", collection = "rea_cp_fail_post_handler")
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 0, idleHeartbeatIntervalSeconds = 0)
    static class PostHandlerStream {
        private final List<ChangeStreamContext<?>> events = new CopyOnWriteArrayList<>();

        @OnInsert
        Mono<Void> handle(ChangeStreamContext<?> ctx) {
            events.add(ctx);
            return Mono.empty();
        }

        List<ChangeStreamContext<?>> getEvents() { return events; }

        void clear() { events.clear(); }
    }

    @ChangeStream(name = "rea-cp-fail-manual", collection = "rea_cp_fail_manual")
    @Checkpoint(saveEveryN = 100)
    static class ManualSaveStream {
        private final List<ChangeStreamContext<?>> events = new CopyOnWriteArrayList<>();

        @OnInsert
        Mono<Void> handle(ChangeStreamContext<?> ctx) {
            events.add(ctx);
            ctx.saveCheckpointNow();
            return Mono.empty();
        }

        List<ChangeStreamContext<?>> getEvents() { return events; }

        void clear() { events.clear(); }
    }

    @ChangeStream(name = "rea-cp-fail-timer", collection = "rea_cp_fail_timer")
    // saveEveryN high so only the timer path triggers; idle probing opted
    // out so no startup diagnostic probe can hold the heartbeat lock under
    // full-suite load (the flush would tryLock-skip for its whole duration).
    @Checkpoint(saveEveryN = 100, saveIntervalSeconds = 1, idleHeartbeatIntervalSeconds = 0)
    static class TimerStream {
        private final List<ChangeStreamContext<?>> events = new CopyOnWriteArrayList<>();

        @OnInsert
        Mono<Void> handle(ChangeStreamContext<?> ctx) {
            events.add(ctx);
            return Mono.empty();
        }

        List<ChangeStreamContext<?>> getEvents() { return events; }

        void clear() { events.clear(); }
    }

    static final class FailingCheckpointStore implements CheckpointStore {
        private final CheckpointStore delegate;
        private final Map<String, AtomicInteger> remainingFailures = new ConcurrentHashMap<>();

        FailingCheckpointStore(CheckpointStore delegate) {
            this.delegate = delegate;
        }

        void failNextNCalls(String methodAndStream, int count) {
            remainingFailures
                    .computeIfAbsent(methodAndStream, k -> new AtomicInteger())
                    .set(count);
        }

        void reset() {
            remainingFailures.clear();
            confirmedSaveProcessed.clear();
        }

        private void maybeThrow(String key) {
            AtomicInteger counter = remainingFailures.get(key);
            if (counter != null && counter.get() > 0 && counter.getAndDecrement() > 0) {
                throw wtimeoutException();
            }
        }

        @Override
        public void save(io.flowwarden.stream.spi.Checkpoint checkpoint) {
            maybeThrow("save:" + checkpoint.streamName());
            delegate.save(checkpoint);
        }

        @Override
        public void saveProcessed(String streamName, BsonDocument token, Instant timestamp) {
            maybeThrow("saveProcessed:" + streamName);
            delegate.saveProcessed(streamName, token, timestamp);
            confirmedSaveProcessed.computeIfAbsent(streamName, k -> new AtomicInteger())
                    .incrementAndGet();
        }

        @Override
        public void saveProcessed(String streamName, BsonDocument token, Instant timestamp,
                                  Instant heartbeatTimestamp) {
            maybeThrow("saveProcessed:" + streamName);
            delegate.saveProcessed(streamName, token, timestamp, heartbeatTimestamp);
            confirmedSaveProcessed.computeIfAbsent(streamName, k -> new AtomicInteger())
                    .incrementAndGet();
        }

        private final Map<String, AtomicInteger> confirmedSaveProcessed = new ConcurrentHashMap<>();

        int confirmedSaveProcessed(String streamName) {
            AtomicInteger c = confirmedSaveProcessed.get(streamName);
            return c == null ? 0 : c.get();
        }

        @Override
        public void saveSeen(String streamName, BsonDocument token, Instant timestamp) {
            maybeThrow("saveSeen:" + streamName);
            delegate.saveSeen(streamName, token, timestamp);
        }

        @Override
        public Optional<io.flowwarden.stream.spi.Checkpoint> findByStreamName(String streamName) {
            return delegate.findByStreamName(streamName);
        }

        @Override
        public void delete(String streamName) {
            delegate.delete(streamName);
        }

        private static MongoWriteConcernException wtimeoutException() {
            return new MongoWriteConcernException(
                    new WriteConcernError(64, "WriteConcernFailed",
                            "waiting for replication timed out", new BsonDocument()),
                    new ServerAddress("localhost", 27017));
        }
    }

    static final class RecordingMetricsProvider implements StreamMetricsProvider {
        private final Map<String, AtomicInteger> successes = new ConcurrentHashMap<>();
        private final Map<String, List<Throwable>> failures = new ConcurrentHashMap<>();

        int checkpointSuccesses(String streamName) {
            AtomicInteger c = successes.get(streamName);
            return c == null ? 0 : c.get();
        }

        List<Throwable> checkpointFailures(String streamName) {
            return failures.getOrDefault(streamName, List.of());
        }

        @Override
        public void onCheckpoint(String streamName, String resumeToken) {
            successes.computeIfAbsent(streamName, k -> new AtomicInteger()).incrementAndGet();
        }

        @Override
        public void onCheckpointFailed(String streamName, Throwable cause) {
            failures.computeIfAbsent(streamName, k -> new CopyOnWriteArrayList<>()).add(cause);
        }

        @Override public void onStreamStarted(String streamName, StreamConfiguration config) {}
        @Override public void onEventReceived(String streamName, ChangeEventMetadata metadata) {}
        @Override public void onEventProcessed(String streamName, long durationNanos, boolean success) {}
        @Override public void onEventError(String streamName, Throwable error, boolean willRetry,
                                           int attemptNumber, ChangeEventMetadata metadata) {}
        @Override public void onBufferStatus(String streamName, int currentSize, int maxSize) {}
        @Override public void onBackpressure(String streamName, BackpressureAction action) {}
        @Override public void onEventSentToDlq(String streamName) {}
        @Override public void onOplogStats(double logLengthHours, String status) {}
    }
}
