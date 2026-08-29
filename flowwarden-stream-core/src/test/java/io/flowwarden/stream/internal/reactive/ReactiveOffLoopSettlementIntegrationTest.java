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
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.Filter;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.annotation.RetryPolicy;
import io.flowwarden.stream.internal.checkpoint.MongoCheckpointStore;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.bson.BsonDocument;
import org.bson.Document;
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
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Regression harness for the event-loop settlement deadlock: the whole
 * synchronous dispatch of an event — {@code @Filter} evaluation, handler
 * invocation (including {@code ctx.saveCheckpointNow()}), and the
 * settlements of events that never reach a handler — can trigger a
 * BLOCKING checkpoint write through the processed-anchor policy. Running
 * any of it on a driver delivery thread wedges the stream permanently
 * when the write's own reply needs the blocked thread
 * (thread-dump-confirmed, load-dependent). The dispatch is therefore
 * fenced behind a single off-loop boundary; this test pins the thread
 * discipline for the three persisting paths.
 */
@SpringBootTest(classes = ReactiveOffLoopSettlementIntegrationTest.TestApp.class)
@ActiveProfiles("test-webflux")
class ReactiveOffLoopSettlementIntegrationTest {

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired
    ReactiveMongoTemplate reactiveMongoTemplate;

    @Autowired
    ThreadRecordingStore recordingStore;

    @Autowired
    RetryManualSaveStream retryStream;

    @Autowired
    ReactiveStreamManager streamManager;

    @BeforeEach
    void setUp() {
        recordingStore.clear();
    }

    @Test
    void manualSave_neverPersistsOnAnEventLoopThread() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("rea-offloop-manual"));

        // The handler calls ctx.saveCheckpointNow() before returning: the
        // targeted saveProcessed write happens during handler invocation,
        // inside the dispatch — not on a terminal signal.
        reactiveMongoTemplate.insert(new Document("item", "M1"), "rea_offloop_manual").block();

        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThat(recordingStore.saveProcessedThreads())
                        .hasSizeGreaterThanOrEqualTo(1));

        assertThat(recordingStore.saveProcessedThreads())
                .as("a manual save from inside a handler must run on the "
                        + "blocking-safe scheduler, never on the driver thread "
                        + "that delivered the event")
                .allMatch(name -> name.startsWith("boundedElastic"));
    }

    @Test
    void retriedAttemptManualSave_runsOnTheBlockingSafeScheduler() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("rea-offloop-retry"));

        // First invocation fails; retryWhen re-subscribes the attempt source
        // after the backoff, from the parallel timer thread. The second
        // invocation calls ctx.saveCheckpointNow() and succeeds — the write
        // must have hopped back to the blocking-safe scheduler, not run on
        // the re-subscribing thread.
        reactiveMongoTemplate.insert(new Document("item", "R1"), "rea_offloop_retry").block();

        await().atMost(Duration.ofSeconds(15))
                .until(() -> retryStream.succeeded());
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(recordingStore.saveProcessedThreads())
                        .hasSizeGreaterThanOrEqualTo(1));

        assertThat(retryStream.attempts())
                .as("the manual save must come from a RETRIED attempt")
                .isGreaterThanOrEqualTo(2);
        assertThat(recordingStore.saveProcessedThreads())
                .as("a manual save from a retried handler attempt must run on "
                        + "the blocking-safe scheduler — retryWhen re-subscribes "
                        + "from the parallel timer thread, which must never carry "
                        + "a blocking store write")
                .allMatch(name -> name.startsWith("boundedElastic"));
    }

    @Test
    void offPipelineSettlements_neverPersistOnAnEventLoopThread() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("rea-offloop"));

        // @Filter rejects everything: each insert is a terminal settlement
        // taken on the filter path, off the delivery pipeline.
        reactiveMongoTemplate.insert(new Document("item", "F1"), "rea_offloop").block();
        reactiveMongoTemplate.insert(new Document("item", "F2"), "rea_offloop").block();

        // No @OnUpdate handler: each update settles on the no-handler path.
        reactiveMongoTemplate.updateFirst(
                Query.query(Criteria.where("item").is("F1")),
                Update.update("touched", true), "rea_offloop").block();

        // saveEveryN = 1: every settlement must reach the store.
        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThat(recordingStore.saveProcessedThreads())
                        .hasSizeGreaterThanOrEqualTo(3));

        // The driver's delivery threads carry no stable name across
        // transports (Netty event loops in production, JDK-async
        // InnocuousThreads under the test harness), so the pin is on the
        // destination instead: every settlement persistence must have hopped
        // to the blocking-safe scheduler. Any regression to an inline
        // settlement surfaces as a driver-owned thread name here.
        assertThat(recordingStore.saveProcessedThreads())
                .as("a settlement persistence must never run on the driver "
                        + "thread that delivered the event — a block() there can "
                        + "wait on a reply that needs the very thread it is "
                        + "blocking; it must hop to the blocking-safe scheduler")
                .allMatch(name -> name.startsWith("boundedElastic"));
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import({RejectAllStream.class, ManualSaveStream.class, RetryManualSaveStream.class,
            RecordingStoreConfig.class})
    static class TestApp {
    }

    @Configuration
    static class RecordingStoreConfig {
        @Bean
        @Primary
        ThreadRecordingStore threadRecordingStore(MongoTemplate template) {
            return new ThreadRecordingStore(template);
        }
    }

    /**
     * Records the thread name of every processed-anchor persistence. The
     * writes stay real (delegated to the Mongo store) so the run exercises
     * the exact blocking I/O the event loop must never carry.
     */
    static class ThreadRecordingStore extends MongoCheckpointStore {
        private final List<String> saveProcessedThreads = new CopyOnWriteArrayList<>();

        ThreadRecordingStore(MongoTemplate template) {
            super(template);
        }

        @Override
        public void saveProcessed(String streamName, BsonDocument token, Instant timestamp) {
            saveProcessedThreads.add(Thread.currentThread().getName());
            super.saveProcessed(streamName, token, timestamp);
        }

        @Override
        public void saveProcessed(String streamName, BsonDocument token, Instant timestamp,
                                  Instant heartbeatTimestamp) {
            saveProcessedThreads.add(Thread.currentThread().getName());
            super.saveProcessed(streamName, token, timestamp, heartbeatTimestamp);
        }

        List<String> saveProcessedThreads() {
            return saveProcessedThreads;
        }

        void clear() {
            saveProcessedThreads.clear();
        }
    }

    // Count threshold out of reach: the only persisting path is the manual
    // save issued from the second (retried) invocation.
    @ChangeStream(name = "rea-offloop-retry", collection = "rea_offloop_retry")
    @Checkpoint(saveEveryN = 100, saveIntervalSeconds = 0, idleHeartbeatIntervalSeconds = 0)
    @RetryPolicy(maxAttempts = 3, initialDelay = "100ms")
    static class RetryManualSaveStream {
        private final AtomicInteger attempts = new AtomicInteger();
        private final AtomicBoolean succeeded = new AtomicBoolean();

        @OnInsert
        Mono<Void> handle(ChangeStreamContext<?> ctx) {
            if (attempts.incrementAndGet() == 1) {
                return Mono.error(new IllegalStateException("first attempt fails"));
            }
            ctx.saveCheckpointNow();
            succeeded.set(true);
            return Mono.empty();
        }

        int attempts() { return attempts.get(); }

        boolean succeeded() { return succeeded.get(); }
    }

    // Count threshold out of reach: the only persisting path is the manual
    // save issued from inside the handler.
    @ChangeStream(name = "rea-offloop-manual", collection = "rea_offloop_manual")
    @Checkpoint(saveEveryN = 100, saveIntervalSeconds = 0, idleHeartbeatIntervalSeconds = 0)
    static class ManualSaveStream {

        @OnInsert
        Mono<Void> handle(ChangeStreamContext<?> ctx) {
            ctx.saveCheckpointNow();
            return Mono.empty();
        }
    }

    // Interval flush and idle probes opted out: the recorded saveProcessed
    // calls must all originate from event settlements, nothing periodic.
    @ChangeStream(name = "rea-offloop", collection = "rea_offloop")
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 0, idleHeartbeatIntervalSeconds = 0)
    static class RejectAllStream {

        @Filter
        boolean rejectAll(ChangeStreamContext<?> ctx) {
            return false;
        }

        @OnInsert
        Mono<Void> handle(ChangeStreamContext<?> ctx) {
            return Mono.empty();
        }
    }
}
