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
import io.flowwarden.stream.annotation.DeadLetterQueue;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.internal.dlq.MongoDlqProperties;
import io.flowwarden.stream.internal.dlq.MongoDlqStore;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.DlqPolicy;
import io.flowwarden.stream.spi.DlqStore;
import io.flowwarden.stream.spi.FailedEvent;
import io.flowwarden.stream.spi.StreamMetricsProvider;
import io.flowwarden.stream.test.RecordingBacklogMetrics;
import io.flowwarden.stream.test.SharedMongoContainer;
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
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The DLQ backlog gauge must never block anything but its own stats thread:
 * with {@code DlqStore.count()} fully blocked, event processing (including
 * DLQ writes) and checkpoint flushes keep advancing, and the gauge arrives
 * once the count unblocks.
 */
@SpringBootTest(classes = ImperativeDlqBacklogIsolationIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeDlqBacklogIsolationIntegrationTest {

    private static final String STREAM = "imp-dlq-backlog-iso";
    private static final String COLLECTION = "impDlqBacklogIso";

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired MongoTemplate mongoTemplate;
    @Autowired ImperativeStreamManager streamManager;
    @Autowired CheckpointStore checkpointStore;
    @Autowired BlockingCountDlqStore blockingStore;
    @Autowired IsoHandler handler;

    private final RecordingBacklogMetrics metrics = new RecordingBacklogMetrics();

    @BeforeEach
    void setUp() {
        FlowWardenMetrics.setProvider(metrics);
        handler.clear();
    }

    @AfterEach
    void tearDown() {
        blockingStore.gate.countDown();
        try { streamManager.stopStream(STREAM); } catch (Exception ignored) {}
        FlowWardenMetrics.setProvider(StreamMetricsProvider.noOp());
    }

    @Test
    void blockedCount_neverBlocksProcessing_flushesAdvance_freshEmitsCoalesce() throws Exception {
        streamManager.startStream(STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(STREAM));
        // Bootstrap already persisted a position — the flush oracle below
        // must observe PROGRESS from it, not mere presence.
        org.bson.BsonDocument bootstrapToken = checkpointStore.findByStreamName(STREAM)
                .map(cp -> cp.lastSeenToken()).orElse(null);

        // A failing event: durable DLQ write, then the fresh gauge emit is
        // queued behind a count that will NOT return.
        mongoTemplate.insert(new Document("fail", true).append("tag", "boom"), COLLECTION);
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(blockingStore.saved).hasSize(1));
        assertThat(blockingStore.countEntered.await(10, TimeUnit.SECONDS))
                .as("the stats thread must be inside the blocked count")
                .isTrue();

        // A flood of DLQ writes while the count is blocked: every write is
        // durable (processing advances), but the fresh emits must coalesce —
        // not queue one task per write.
        for (int i = 0; i < 4; i++) {
            mongoTemplate.insert(new Document("fail", true).append("seq", i), COLLECTION);
        }
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(blockingStore.saved).hasSize(5));

        // Processing keeps advancing while the count is blocked.
        mongoTemplate.insert(new Document("tag", "after-block"), COLLECTION);
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(handler.tags()).contains("after-block"));

        // Checkpoint flushes keep advancing too: saveEveryN is out of reach,
        // so progress past the bootstrap position can only come from the
        // interval flush.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(checkpointStore.findByStreamName(STREAM)
                        .map(cp -> cp.lastSeenToken()).orElse(null))
                        .as("interval flush must progress past bootstrap while count is blocked")
                        .isNotNull()
                        .isNotEqualTo(bootstrapToken));

        // No gauge was emitted while blocked; once unblocked, the LAST state
        // (all 5 entries) is published.
        assertThat(metrics.backlogs).isEmpty();
        blockingStore.gate.countDown();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(metrics.backlogs)
                        .anyMatch(b -> b.streamName().equals(STREAM) && b.pending() == 5));

        // Coalescing bound: the blocked pass, one coalesced catch-up pass,
        // and at most one more from the racing hand-off — NOT one per write.
        assertThat(blockingStore.countCalls.get())
                .as("fresh emits must coalesce per stream, not queue per write")
                .isLessThanOrEqualTo(3);
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import({IsoHandler.class, BlockingStoreConfig.class})
    static class TestApp {}

    @Configuration
    static class BlockingStoreConfig {
        @Bean
        @Primary
        BlockingCountDlqStore blockingCountDlqStore(MongoTemplate template) {
            return new BlockingCountDlqStore(
                    new MongoDlqStore(template, new MongoDlqProperties()));
        }
    }

    /** Delegates everything; {@code count} blocks until the gate opens. */
    static class BlockingCountDlqStore implements DlqStore {
        final MongoDlqStore delegate;
        final CountDownLatch gate = new CountDownLatch(1);
        final CountDownLatch countEntered = new CountDownLatch(1);
        final List<FailedEvent> saved = new CopyOnWriteArrayList<>();

        BlockingCountDlqStore(MongoDlqStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public void save(FailedEvent event, DlqPolicy policy) {
            delegate.save(event, policy);
            saved.add(event);
        }

        @Override
        public Optional<FailedEvent> findById(String id) {
            return delegate.findById(id);
        }

        @Override
        public List<FailedEvent> findByStreamName(String streamName) {
            return delegate.findByStreamName(streamName);
        }

        final java.util.concurrent.atomic.AtomicInteger countCalls =
                new java.util.concurrent.atomic.AtomicInteger();

        @Override
        public long count(String streamName) {
            countCalls.incrementAndGet();
            countEntered.countDown();
            try {
                // Bounded so a failing assertion can never hang the fork.
                gate.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return delegate.count(streamName);
        }
    }

    @ChangeStream(name = STREAM, collection = COLLECTION, autoStart = false)
    @Checkpoint(saveEveryN = 1000, saveIntervalSeconds = 1, idleHeartbeatIntervalSeconds = 0)
    @DeadLetterQueue
    static class IsoHandler {
        private final List<String> tags = new CopyOnWriteArrayList<>();

        @OnInsert
        void handle(ChangeStreamContext<Document> ctx) {
            Document doc = ctx.getFullDocument(Document.class).orElse(new Document());
            if (Boolean.TRUE.equals(doc.getBoolean("fail"))) {
                throw new RuntimeException("DLQ me");
            }
            tags.add(doc.getString("tag"));
        }

        List<String> tags() { return tags; }

        void clear() { tags.clear(); }
    }
}
