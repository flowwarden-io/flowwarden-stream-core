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
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.DeadLetterQueue;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.annotation.RetryPolicy;
import io.flowwarden.stream.spi.DlqStore;
import io.flowwarden.stream.spi.FailedEvent;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.bson.Document;
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

@SpringBootTest(classes = ImperativeDlqAutoSendIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeDlqAutoSendIntegrationTest {

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired
    MongoTemplate mongoTemplate;

    @Autowired
    RetryThenDlqHandler retryThenDlqHandler;

    @Autowired
    NoDlqWithoutAnnotationHandler noDlqHandler;

    @Autowired
    DirectDlqHandler directDlqHandler;

    @Autowired
    ImperativeStreamManager streamManager;

    @Autowired
    DlqStore dlqStore;

    @Test
    void eventSentToDlqAfterRetryExhaustion() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("imp-dlq-auto-retry"));

        retryThenDlqHandler.reset();
        mongoTemplate.insert(new Document("test", "dlq-auto"), "impDlqAutoRetryOrders");

        // Wait for all 3 retries to be exhausted
        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() ->
                        assertThat(retryThenDlqHandler.getInvocationCount()).isEqualTo(3));

        // Verify the event was persisted in DLQ
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    List<FailedEvent> events = dlqStore.findByStreamName("imp-dlq-auto-retry");
                    assertThat(events).hasSize(1);

                    FailedEvent event = events.get(0);
                    assertThat(event.streamName()).isEqualTo("imp-dlq-auto-retry");
                    assertThat(event.error().type()).isEqualTo("java.lang.RuntimeException");
                    assertThat(event.error().message()).contains("Permanent failure");
                    assertThat(event.error().stackTrace()).isNotNull();
                    assertThat(event.attempts()).isEqualTo(3);
                    assertThat(event.status()).isEqualTo(FailedEvent.STATUS_PENDING);
                    assertThat(event.expiresAt()).isNotNull();
                });

        // Verify stream continues processing: insert a second document
        retryThenDlqHandler.setShouldSucceed(true);
        mongoTemplate.insert(new Document("test", "success-after-dlq"), "impDlqAutoRetryOrders");

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(retryThenDlqHandler.getSuccessEvents()).hasSize(1));
    }

    @Test
    void eventSentToDlqDirectlyWithoutRetryPolicy() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("imp-dlq-direct"));

        directDlqHandler.reset();
        mongoTemplate.insert(new Document("test", "direct-dlq"), "impDlqDirectOrders");

        // Should fail once and go straight to DLQ
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(directDlqHandler.getInvocationCount()).isGreaterThanOrEqualTo(1));

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    List<FailedEvent> events = dlqStore.findByStreamName("imp-dlq-direct");
                    assertThat(events).hasSize(1);

                    FailedEvent event = events.get(0);
                    assertThat(event.attempts()).isEqualTo(1);
                    assertThat(event.error().type()).isEqualTo("java.lang.RuntimeException");
                    assertThat(event.status()).isEqualTo(FailedEvent.STATUS_PENDING);
                });
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import({
            ImperativeDlqAutoSendIntegrationTest.RetryThenDlqHandler.class,
            ImperativeDlqAutoSendIntegrationTest.NoDlqWithoutAnnotationHandler.class,
            ImperativeDlqAutoSendIntegrationTest.DirectDlqHandler.class
    })
    static class TestApp {
    }

    @ChangeStream(name = "imp-dlq-auto-retry", collection = "impDlqAutoRetryOrders")
    @RetryPolicy(maxAttempts = 3, initialDelay = "100ms", maxDelay = "500ms", jitter = false)
    @DeadLetterQueue
    static class RetryThenDlqHandler {
        private final AtomicInteger invocationCount = new AtomicInteger(0);
        private final List<ChangeStreamContext<?>> successEvents = new CopyOnWriteArrayList<>();
        private volatile boolean shouldSucceed = false;

        @OnInsert
        void handle(ChangeStreamContext<?> ctx) {
            invocationCount.incrementAndGet();
            if (!shouldSucceed) {
                throw new RuntimeException("Permanent failure #" + invocationCount.get());
            }
            successEvents.add(ctx);
        }

        void reset() {
            invocationCount.set(0);
            successEvents.clear();
            shouldSucceed = false;
        }

        void setShouldSucceed(boolean value) { shouldSucceed = value; }
        int getInvocationCount() { return invocationCount.get(); }
        List<ChangeStreamContext<?>> getSuccessEvents() { return successEvents; }
    }

    @ChangeStream(name = "imp-no-dlq-annotation", collection = "impNoDlqAnnotationOrders")
    @RetryPolicy(maxAttempts = 3, initialDelay = "100ms", maxDelay = "500ms", jitter = false)
    static class NoDlqWithoutAnnotationHandler {
        private final AtomicInteger invocationCount = new AtomicInteger(0);

        @OnInsert
        void handle(ChangeStreamContext<?> ctx) {
            invocationCount.incrementAndGet();
            throw new RuntimeException("Failure without DLQ");
        }

        void reset() { invocationCount.set(0); }
        int getInvocationCount() { return invocationCount.get(); }
    }

    @ChangeStream(name = "imp-dlq-direct", collection = "impDlqDirectOrders")
    @DeadLetterQueue
    static class DirectDlqHandler {
        private final AtomicInteger invocationCount = new AtomicInteger(0);

        @OnInsert
        void handle(ChangeStreamContext<?> ctx) {
            invocationCount.incrementAndGet();
            throw new RuntimeException("Direct DLQ failure");
        }

        void reset() { invocationCount.set(0); }
        int getInvocationCount() { return invocationCount.get(); }
    }
}
