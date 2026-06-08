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
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import reactor.core.publisher.Mono;

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

@SpringBootTest(classes = ReactiveDlqFailureIntegrationTest.TestApp.class)
@ActiveProfiles("test-webflux")
class ReactiveDlqFailureIntegrationTest {

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired
    ReactiveMongoTemplate reactiveMongoTemplate;

    @Autowired
    FailingDlqStore failingStore;

    @Autowired
    AutoDlqHandler autoHandler;

    @Autowired
    ManualDlqHandler manualHandler;

    @Autowired
    ReactiveStreamManager streamManager;

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
                .until(() -> streamManager.isRunning("rea-auto-dlq-fail"));

        failingStore.failNextNSaves("rea-auto-dlq-fail", 1);
        reactiveMongoTemplate.insert(new Document("seq", 1), "reaAutoDlqFailOrders").block();

        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> verify(metrics)
                        .onEventDlqFailed(eq("rea-auto-dlq-fail"), any(MongoWriteConcernException.class)));
        verify(metrics, never()).onEventSentToDlq("rea-auto-dlq-fail");

        reactiveMongoTemplate.insert(new Document("seq", 2), "reaAutoDlqFailOrders").block();

        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> verify(metrics).onEventSentToDlq("rea-auto-dlq-fail"));

        assertThat(autoHandler.getInvocationCount()).isGreaterThanOrEqualTo(2);
    }

    @Test
    void manualDlqFailurePropagatesAndEmitsOnEventDlqFailed() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("rea-manual-dlq-fail"));

        failingStore.failNextNSaves("rea-manual-dlq-fail", 1);
        reactiveMongoTemplate.insert(new Document("seq", 1), "reaManualDlqFailOrders").block();

        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> {
                    assertThat(manualHandler.getCapturedDlqErrors())
                            .hasSizeGreaterThanOrEqualTo(1);
                    assertThat(manualHandler.getCapturedDlqErrors().get(0))
                            .isInstanceOf(MongoWriteConcernException.class);
                });

        verify(metrics).onEventDlqFailed(
                eq("rea-manual-dlq-fail"), any(MongoWriteConcernException.class));
        verify(metrics, never()).onEventSentToDlq("rea-manual-dlq-fail");
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

    @ChangeStream(name = "rea-auto-dlq-fail", collection = "reaAutoDlqFailOrders")
    @DeadLetterQueue
    static class AutoDlqHandler {
        private final AtomicInteger invocationCount = new AtomicInteger();

        @OnInsert
        Mono<Void> handle(ChangeStreamContext<?> ctx) {
            invocationCount.incrementAndGet();
            return Mono.error(new RuntimeException("forced handler failure → DLQ"));
        }

        void reset() { invocationCount.set(0); }

        int getInvocationCount() { return invocationCount.get(); }
    }

    @ChangeStream(name = "rea-manual-dlq-fail", collection = "reaManualDlqFailOrders")
    @Checkpoint(saveEveryN = 1000)  // required so the manager wires ctx.sendToDlq
    @DeadLetterQueue
    static class ManualDlqHandler {
        private final List<RuntimeException> capturedDlqErrors = new CopyOnWriteArrayList<>();

        @OnInsert
        Mono<Void> handle(ChangeStreamContext<?> ctx) {
            try {
                ctx.sendToDlq("manual dlq from test");
            } catch (RuntimeException e) {
                capturedDlqErrors.add(e);
            }
            return Mono.empty();
        }

        void reset() { capturedDlqErrors.clear(); }

        List<RuntimeException> getCapturedDlqErrors() { return capturedDlqErrors; }
    }

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
