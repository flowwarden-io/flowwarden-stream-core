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
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnChange;
import io.flowwarden.stream.annotation.RetryPolicy;
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

@SpringBootTest(classes = ImperativeRetryIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeRetryIntegrationTest {

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired
    MongoTemplate mongoTemplate;

    @Autowired
    RetryTwiceThenSucceedHandler retryHandler;

    @Autowired
    ExhaustedRetryHandler exhaustedHandler;

    @Autowired
    NoRetryOnIllegalArgHandler noRetryHandler;

    @Autowired
    ImperativeStreamManager streamManager;

    @Test
    void handlerFailsTwiceThenSucceeds() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("retry-twice-stream"));

        retryHandler.reset();
        mongoTemplate.insert(new Document("test", "retry-twice"), "retryTwiceOrders");

        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() ->
                        assertThat(retryHandler.getSuccessEvents()).hasSize(1));

        assertThat(retryHandler.getInvocationCount()).isEqualTo(3); // 2 failures + 1 success
        assertThat(retryHandler.getAttemptNumbers()).containsExactly(1, 2, 3);
    }

    @Test
    void handlerExhaustsAllRetries() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("exhausted-retry-stream"));

        exhaustedHandler.reset();
        mongoTemplate.insert(new Document("test", "exhaust"), "exhaustedRetryOrders");

        // Wait for all retries to be exhausted (3 attempts with short delays)
        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() ->
                        assertThat(exhaustedHandler.getInvocationCount()).isEqualTo(3));

        // Should have attempted exactly maxAttempts times
        assertThat(exhaustedHandler.getSuccessEvents()).isEmpty();
        assertThat(exhaustedHandler.getAttemptNumbers()).containsExactly(1, 2, 3);
    }

    @Test
    void noRetryOnIllegalArgumentException() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("no-retry-illegal-arg-stream"));

        noRetryHandler.reset();
        mongoTemplate.insert(new Document("test", "no-retry"), "noRetryIllegalArgOrders");

        // Should fail immediately without retrying
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(noRetryHandler.getInvocationCount()).isGreaterThanOrEqualTo(1));

        // Give a brief window to make sure no additional invocations happen
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(3))
                .untilAsserted(() ->
                        assertThat(noRetryHandler.getInvocationCount()).isEqualTo(1));

        assertThat(noRetryHandler.getAttemptNumbers()).containsExactly(1);
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import({
            ImperativeRetryIntegrationTest.RetryTwiceThenSucceedHandler.class,
            ImperativeRetryIntegrationTest.ExhaustedRetryHandler.class,
            ImperativeRetryIntegrationTest.NoRetryOnIllegalArgHandler.class
    })
    static class TestApp {
    }

    @ChangeStream(name = "retry-twice-stream", collection = "retryTwiceOrders")
    @RetryPolicy(maxAttempts = 5, initialDelay = "100ms", maxDelay = "500ms", jitter = false)
    static class RetryTwiceThenSucceedHandler {
        private final AtomicInteger invocationCount = new AtomicInteger(0);
        private final List<ChangeStreamContext<?>> successEvents = new CopyOnWriteArrayList<>();
        private final List<Integer> attemptNumbers = new CopyOnWriteArrayList<>();

        @OnChange
        void handle(ChangeStreamContext<?> ctx) {
            int count = invocationCount.incrementAndGet();
            attemptNumbers.add(ctx.getAttemptNumber());
            if (count <= 2) {
                throw new RuntimeException("Transient failure #" + count);
            }
            successEvents.add(ctx);
        }

        void reset() {
            invocationCount.set(0);
            successEvents.clear();
            attemptNumbers.clear();
        }

        int getInvocationCount() { return invocationCount.get(); }
        List<ChangeStreamContext<?>> getSuccessEvents() { return successEvents; }
        List<Integer> getAttemptNumbers() { return attemptNumbers; }
    }

    @ChangeStream(name = "exhausted-retry-stream", collection = "exhaustedRetryOrders")
    @RetryPolicy(maxAttempts = 3, initialDelay = "100ms", maxDelay = "500ms", jitter = false)
    static class ExhaustedRetryHandler {
        private final AtomicInteger invocationCount = new AtomicInteger(0);
        private final List<ChangeStreamContext<?>> successEvents = new CopyOnWriteArrayList<>();
        private final List<Integer> attemptNumbers = new CopyOnWriteArrayList<>();

        @OnChange
        void handle(ChangeStreamContext<?> ctx) {
            int count = invocationCount.incrementAndGet();
            attemptNumbers.add(ctx.getAttemptNumber());
            throw new RuntimeException("Permanent failure #" + count);
        }

        void reset() {
            invocationCount.set(0);
            successEvents.clear();
            attemptNumbers.clear();
        }

        int getInvocationCount() { return invocationCount.get(); }
        List<ChangeStreamContext<?>> getSuccessEvents() { return successEvents; }
        List<Integer> getAttemptNumbers() { return attemptNumbers; }
    }

    @ChangeStream(name = "no-retry-illegal-arg-stream", collection = "noRetryIllegalArgOrders")
    @RetryPolicy(maxAttempts = 5, initialDelay = "100ms", maxDelay = "500ms", jitter = false)
    static class NoRetryOnIllegalArgHandler {
        private final AtomicInteger invocationCount = new AtomicInteger(0);
        private final List<Integer> attemptNumbers = new CopyOnWriteArrayList<>();

        @OnChange
        void handle(ChangeStreamContext<?> ctx) {
            invocationCount.incrementAndGet();
            attemptNumbers.add(ctx.getAttemptNumber());
            throw new IllegalArgumentException("Bad argument - no retry");
        }

        void reset() {
            invocationCount.set(0);
            attemptNumbers.clear();
        }

        int getInvocationCount() { return invocationCount.get(); }
        List<Integer> getAttemptNumbers() { return attemptNumbers; }
    }
}
