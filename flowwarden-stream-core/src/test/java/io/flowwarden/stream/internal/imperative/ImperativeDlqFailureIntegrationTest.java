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

import com.mongodb.MongoWriteConcernException;
import com.mongodb.ServerAddress;
import com.mongodb.bulk.WriteConcernError;
import io.flowwarden.stream.ChangeStreamContext;
import io.flowwarden.stream.FlowWardenMetrics;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.annotation.DeadLetterQueue;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.spi.DlqPolicy;
import io.flowwarden.stream.spi.DlqStore;
import io.flowwarden.stream.spi.FailedEvent;
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
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@SpringBootTest(classes = ImperativeDlqFailureIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeDlqFailureIntegrationTest {

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired
    MongoTemplate mongoTemplate;

    @Autowired
    FailingDlqStore failingStore;

    @Autowired
    AutoDlqHandler autoHandler;

    @Autowired
    ManualDlqHandler manualHandler;

    @Autowired
    ImperativeStreamManager streamManager;

    private StreamMetricsProvider metrics;

    @BeforeEach
    void setUp() {
        metrics = mock(StreamMetricsProvider.class);
        FlowWardenMetrics.setProvider(metrics);
        failingStore.reset();
        autoHandler.reset();
        manualHandler.reset();
    }

    @AfterEach
    void tearDown() {
        FlowWardenMetrics.setProvider(StreamMetricsProvider.noOp());
    }

    @Test
    void autoDlqFailureEmitsOnEventDlqFailedAndStreamContinues() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("imp-auto-dlq-fail"));

        // First insert: handler throws → auto DLQ path → store throws → onEventDlqFailed
        failingStore.failNextNSaves("imp-auto-dlq-fail", 1);
        mongoTemplate.insert(new Document("seq", 1), "impAutoDlqFailOrders");

        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> verify(metrics)
                        .onEventDlqFailed(eq("imp-auto-dlq-fail"), any(MongoWriteConcernException.class)));
        verify(metrics, never()).onEventSentToDlq("imp-auto-dlq-fail");

        // Second insert: handler still throws, but store now succeeds → stream proceeds
        mongoTemplate.insert(new Document("seq", 2), "impAutoDlqFailOrders");

        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> verify(metrics).onEventSentToDlq("imp-auto-dlq-fail"));

        // Confirms the stream survived the DLQ outage instead of dying
        assertThat(autoHandler.getInvocationCount()).isGreaterThanOrEqualTo(2);
    }

    @Test
    void manualDlqFailurePropagatesAndEmitsOnEventDlqFailed() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("imp-manual-dlq-fail"));

        failingStore.failNextNSaves("imp-manual-dlq-fail", 1);
        mongoTemplate.insert(new Document("seq", 1), "impManualDlqFailOrders");

        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> {
                    assertThat(manualHandler.getCapturedDlqErrors())
                            .hasSizeGreaterThanOrEqualTo(1);
                    assertThat(manualHandler.getCapturedDlqErrors().get(0))
                            .isInstanceOf(MongoWriteConcernException.class);
                });

        verify(metrics).onEventDlqFailed(
                eq("imp-manual-dlq-fail"), any(MongoWriteConcernException.class));
        verify(metrics, never()).onEventSentToDlq("imp-manual-dlq-fail");
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import({AutoDlqHandler.class, ManualDlqHandler.class, FailingStoreConfig.class})
    static class TestApp {
    }

    @Configuration
    static class FailingStoreConfig {
        @Bean
        @Primary
        FailingDlqStore failingDlqStore() {
            return new FailingDlqStore();
        }
    }

    @ChangeStream(name = "imp-auto-dlq-fail", collection = "impAutoDlqFailOrders")
    @DeadLetterQueue
    static class AutoDlqHandler {
        private final AtomicInteger invocationCount = new AtomicInteger();

        @OnInsert
        void handle(ChangeStreamContext<?> ctx) {
            invocationCount.incrementAndGet();
            throw new RuntimeException("forced handler failure → DLQ");
        }

        void reset() { invocationCount.set(0); }

        int getInvocationCount() { return invocationCount.get(); }
    }

    @ChangeStream(name = "imp-manual-dlq-fail", collection = "impManualDlqFailOrders")
    @Checkpoint(saveEveryN = 1000)  // required so the manager wires ctx.sendToDlq
    @DeadLetterQueue
    static class ManualDlqHandler {
        private final List<RuntimeException> capturedDlqErrors = new CopyOnWriteArrayList<>();

        @OnInsert
        void handle(ChangeStreamContext<?> ctx) {
            try {
                ctx.sendToDlq("manual dlq from test");
            } catch (RuntimeException e) {
                capturedDlqErrors.add(e);
            }
        }

        void reset() { capturedDlqErrors.clear(); }

        List<RuntimeException> getCapturedDlqErrors() { return capturedDlqErrors; }
    }

    /**
     * Decorates a no-op {@link DlqStore} so a configurable number of upcoming
     * {@link #save} calls per stream throw a {@link MongoWriteConcernException}.
     */
    static final class FailingDlqStore implements DlqStore {
        private final Map<String, AtomicInteger> remainingFailures = new ConcurrentHashMap<>();

        void failNextNSaves(String streamName, int count) {
            remainingFailures
                    .computeIfAbsent(streamName, k -> new AtomicInteger())
                    .set(count);
        }

        void reset() {
            remainingFailures.clear();
        }

        @Override
        public void save(FailedEvent event, DlqPolicy policy) {
            AtomicInteger counter = remainingFailures.get(event.streamName());
            if (counter != null && counter.get() > 0 && counter.getAndDecrement() > 0) {
                throw wtimeoutException();
            }
            // delegate to no-op (we only care about metrics signals in this test)
        }

        @Override
        public Optional<FailedEvent> findById(String id) {
            return Optional.empty();
        }

        @Override
        public List<FailedEvent> findByStreamName(String streamName) {
            return List.of();
        }

        private static MongoWriteConcernException wtimeoutException() {
            return new MongoWriteConcernException(
                    new WriteConcernError(64, "WriteConcernFailed",
                            "waiting for replication timed out", new BsonDocument()),
                    new ServerAddress("localhost", 27017));
        }
    }
}
