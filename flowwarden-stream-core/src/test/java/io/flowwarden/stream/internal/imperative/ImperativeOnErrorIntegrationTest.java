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
import io.flowwarden.stream.ErrorAction;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.DeadLetterQueue;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnChange;
import io.flowwarden.stream.annotation.OnError;
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

@SpringBootTest(classes = ImperativeOnErrorIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeOnErrorIntegrationTest {

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired
    MongoTemplate mongoTemplate;

    @Autowired
    SkipOnNpeHandler skipHandler;

    @Autowired
    DlqOnRuntimeHandler dlqHandler;

    @Autowired
    RethrowWithRetryHandler rethrowHandler;

    @Autowired
    UnmatchedExceptionHandler unmatchedHandler;

    @Autowired
    ImperativeStreamManager streamManager;

    @Autowired
    DlqStore dlqStore;

    /**
     * Test A: NPE → @OnError returns SKIP → no retry, no DLQ, stream continues.
     */
    @Test
    void onErrorSkipIgnoresEventAndContinues() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("imp-onerror-skip"));

        skipHandler.reset();
        mongoTemplate.insert(new Document("test", "skip-npe"), "impOnErrorSkipOrders");

        // Wait for handler to be invoked
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(skipHandler.getHandlerInvocations()).isGreaterThanOrEqualTo(1));

        // Error handler should have been called
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() ->
                        assertThat(skipHandler.getErrorHandlerInvocations()).isEqualTo(1));

        // No retry — only 1 handler invocation
        assertThat(skipHandler.getHandlerInvocations()).isEqualTo(1);

        // No DLQ
        List<FailedEvent> events = dlqStore.findByStreamName("imp-onerror-skip");
        assertThat(events).isEmpty();

        // Verify stream continues: insert a second document
        skipHandler.setThrowNpe(false);
        mongoTemplate.insert(new Document("test", "after-skip"), "impOnErrorSkipOrders");

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(skipHandler.getSuccessEvents()).hasSize(1));
    }

    /**
     * Test B: RuntimeException → @OnError returns DLQ → immediate DLQ, no retry.
     */
    @Test
    void onErrorDlqSendsDirectlyToDlq() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("imp-onerror-dlq"));

        dlqHandler.reset();
        mongoTemplate.insert(new Document("test", "dlq-runtime"), "impOnErrorDlqOrders");

        // Wait for error handler
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(dlqHandler.getErrorHandlerInvocations()).isEqualTo(1));

        // Only 1 handler invocation (no retry)
        assertThat(dlqHandler.getHandlerInvocations()).isEqualTo(1);

        // Event in DLQ
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    List<FailedEvent> events = dlqStore.findByStreamName("imp-onerror-dlq");
                    assertThat(events).hasSize(1);
                    assertThat(events.get(0).error().type()).isEqualTo("java.lang.RuntimeException");
                });
    }

    /**
     * Test C: @OnError returns RETHROW + @RetryPolicy(maxAttempts=3) → normal retry.
     */
    @Test
    void onErrorRethrowFallsBackToRetryPolicy() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("imp-onerror-rethrow"));

        rethrowHandler.reset();
        mongoTemplate.insert(new Document("test", "rethrow"), "impOnErrorRethrowOrders");

        // Should retry 3 times total
        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() ->
                        assertThat(rethrowHandler.getHandlerInvocations()).isEqualTo(3));

        assertThat(rethrowHandler.getAttemptNumbers()).containsExactly(1, 2, 3);
    }

    /**
     * Test D: Exception not matched by @OnError → fall through to retry standard.
     */
    @Test
    void unmatchedExceptionFallsThroughToStandardRetry() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("imp-onerror-unmatched"));

        unmatchedHandler.reset();
        mongoTemplate.insert(new Document("test", "unmatched"), "impOnErrorUnmatchedOrders");

        // Should retry 3 times (standard retry)
        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() ->
                        assertThat(unmatchedHandler.getHandlerInvocations()).isEqualTo(3));

        // Error handler should NOT have been called (no match for IOException)
        assertThat(unmatchedHandler.getErrorHandlerInvocations()).isEqualTo(0);
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import({
            ImperativeOnErrorIntegrationTest.SkipOnNpeHandler.class,
            ImperativeOnErrorIntegrationTest.DlqOnRuntimeHandler.class,
            ImperativeOnErrorIntegrationTest.RethrowWithRetryHandler.class,
            ImperativeOnErrorIntegrationTest.UnmatchedExceptionHandler.class
    })
    static class TestApp {
    }

    // --- Test A: SKIP on NPE ---

    @ChangeStream(name = "imp-onerror-skip", collection = "impOnErrorSkipOrders")
    @DeadLetterQueue
    static class SkipOnNpeHandler {
        private final AtomicInteger handlerInvocations = new AtomicInteger(0);
        private final AtomicInteger errorHandlerInvocations = new AtomicInteger(0);
        private final List<ChangeStreamContext<?>> successEvents = new CopyOnWriteArrayList<>();
        private volatile boolean throwNpe = true;

        @OnChange
        void handle(ChangeStreamContext<?> ctx) {
            handlerInvocations.incrementAndGet();
            if (throwNpe) {
                throw new NullPointerException("test NPE");
            }
            successEvents.add(ctx);
        }

        @OnError(NullPointerException.class)
        ErrorAction onNpe(Throwable ex, ChangeStreamContext<?> ctx) {
            errorHandlerInvocations.incrementAndGet();
            return ErrorAction.SKIP;
        }

        void reset() {
            handlerInvocations.set(0);
            errorHandlerInvocations.set(0);
            successEvents.clear();
            throwNpe = true;
        }

        void setThrowNpe(boolean value) { throwNpe = value; }
        int getHandlerInvocations() { return handlerInvocations.get(); }
        int getErrorHandlerInvocations() { return errorHandlerInvocations.get(); }
        List<ChangeStreamContext<?>> getSuccessEvents() { return successEvents; }
    }

    // --- Test B: DLQ on RuntimeException ---

    @ChangeStream(name = "imp-onerror-dlq", collection = "impOnErrorDlqOrders")
    @RetryPolicy(maxAttempts = 3, initialDelay = "100ms", maxDelay = "500ms", jitter = false)
    @DeadLetterQueue
    static class DlqOnRuntimeHandler {
        private final AtomicInteger handlerInvocations = new AtomicInteger(0);
        private final AtomicInteger errorHandlerInvocations = new AtomicInteger(0);

        @OnChange
        void handle(ChangeStreamContext<?> ctx) {
            handlerInvocations.incrementAndGet();
            throw new RuntimeException("test runtime");
        }

        @OnError(RuntimeException.class)
        ErrorAction onRuntime(Throwable ex, ChangeStreamContext<?> ctx) {
            errorHandlerInvocations.incrementAndGet();
            return ErrorAction.DLQ;
        }

        void reset() {
            handlerInvocations.set(0);
            errorHandlerInvocations.set(0);
        }

        int getHandlerInvocations() { return handlerInvocations.get(); }
        int getErrorHandlerInvocations() { return errorHandlerInvocations.get(); }
    }

    // --- Test C: RETHROW with RetryPolicy ---

    @ChangeStream(name = "imp-onerror-rethrow", collection = "impOnErrorRethrowOrders")
    @RetryPolicy(maxAttempts = 3, initialDelay = "100ms", maxDelay = "500ms", jitter = false)
    @DeadLetterQueue
    static class RethrowWithRetryHandler {
        private final AtomicInteger handlerInvocations = new AtomicInteger(0);
        private final List<Integer> attemptNumbers = new CopyOnWriteArrayList<>();

        @OnChange
        void handle(ChangeStreamContext<?> ctx) {
            handlerInvocations.incrementAndGet();
            attemptNumbers.add(ctx.getAttemptNumber());
            throw new RuntimeException("always fail");
        }

        @OnError(RuntimeException.class)
        ErrorAction onRuntime(Throwable ex, ChangeStreamContext<?> ctx) {
            return ErrorAction.RETHROW;
        }

        void reset() {
            handlerInvocations.set(0);
            attemptNumbers.clear();
        }

        int getHandlerInvocations() { return handlerInvocations.get(); }
        List<Integer> getAttemptNumbers() { return attemptNumbers; }
    }

    // --- Test D: Unmatched exception → standard retry ---

    @ChangeStream(name = "imp-onerror-unmatched", collection = "impOnErrorUnmatchedOrders")
    @RetryPolicy(maxAttempts = 3, initialDelay = "100ms", maxDelay = "500ms", jitter = false)
    @DeadLetterQueue
    static class UnmatchedExceptionHandler {
        private final AtomicInteger handlerInvocations = new AtomicInteger(0);
        private final AtomicInteger errorHandlerInvocations = new AtomicInteger(0);

        @OnChange
        void handle(ChangeStreamContext<?> ctx) {
            handlerInvocations.incrementAndGet();
            throw new RuntimeException("unmatched exception");
        }

        // Only handles NullPointerException, but handler throws RuntimeException
        @OnError(NullPointerException.class)
        ErrorAction onNpe(Throwable ex, ChangeStreamContext<?> ctx) {
            errorHandlerInvocations.incrementAndGet();
            return ErrorAction.SKIP;
        }

        void reset() {
            handlerInvocations.set(0);
            errorHandlerInvocations.set(0);
        }

        int getHandlerInvocations() { return handlerInvocations.get(); }
        int getErrorHandlerInvocations() { return errorHandlerInvocations.get(); }
    }
}
