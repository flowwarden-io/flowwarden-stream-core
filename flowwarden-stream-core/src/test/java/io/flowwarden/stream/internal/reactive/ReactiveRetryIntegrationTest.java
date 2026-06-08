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
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = ReactiveRetryIntegrationTest.TestApp.class)
@ActiveProfiles("test-webflux")
class ReactiveRetryIntegrationTest {

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired
    ReactiveMongoTemplate reactiveMongoTemplate;

    @Autowired
    RetryTwiceThenSucceedHandler retryHandler;

    @Autowired
    ExhaustedRetryHandler exhaustedHandler;

    @Autowired
    NoRetryOnIllegalArgHandler noRetryHandler;

    @Autowired
    ReactiveStreamManager streamManager;

    @Test
    void handlerFailsTwiceThenSucceeds() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("reactive-retry-twice-stream"));

        retryHandler.reset();
        reactiveMongoTemplate.insert(new Document("test", "retry-twice"), "reactiveRetryTwiceOrders").block();

        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() ->
                        assertThat(retryHandler.getSuccessEvents()).hasSize(1));

        assertThat(retryHandler.getInvocationCount()).isEqualTo(3);
        assertThat(retryHandler.getAttemptNumbers()).containsExactly(1, 2, 3);
    }

    @Test
    void handlerExhaustsAllRetries() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("reactive-exhausted-retry-stream"));

        exhaustedHandler.reset();
        reactiveMongoTemplate.insert(new Document("test", "exhaust"), "reactiveExhaustedRetryOrders").block();

        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() ->
                        assertThat(exhaustedHandler.getInvocationCount()).isEqualTo(3));

        assertThat(exhaustedHandler.getSuccessEvents()).isEmpty();
        assertThat(exhaustedHandler.getAttemptNumbers()).containsExactly(1, 2, 3);
    }

    @Test
    void noRetryOnIllegalArgumentException() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("reactive-no-retry-illegal-arg-stream"));

        noRetryHandler.reset();
        reactiveMongoTemplate.insert(new Document("test", "no-retry"), "reactiveNoRetryIllegalArgOrders").block();

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(noRetryHandler.getInvocationCount()).isGreaterThanOrEqualTo(1));

        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(3))
                .untilAsserted(() ->
                        assertThat(noRetryHandler.getInvocationCount()).isEqualTo(1));

        assertThat(noRetryHandler.getAttemptNumbers()).containsExactly(1);
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import({
            ReactiveRetryIntegrationTest.RetryTwiceThenSucceedHandler.class,
            ReactiveRetryIntegrationTest.ExhaustedRetryHandler.class,
            ReactiveRetryIntegrationTest.NoRetryOnIllegalArgHandler.class
    })
    static class TestApp {
    }

    @ChangeStream(name = "reactive-retry-twice-stream", collection = "reactiveRetryTwiceOrders")
    @RetryPolicy(maxAttempts = 5, initialDelay = "100ms", maxDelay = "500ms", jitter = false)
    static class RetryTwiceThenSucceedHandler {
        private final AtomicInteger invocationCount = new AtomicInteger(0);
        private final List<ChangeStreamContext<?>> successEvents = new CopyOnWriteArrayList<>();
        private final List<Integer> attemptNumbers = new CopyOnWriteArrayList<>();

        @OnChange
        Mono<Void> handle(ChangeStreamContext<?> ctx) {
            int count = invocationCount.incrementAndGet();
            attemptNumbers.add(ctx.getAttemptNumber());
            if (count <= 2) {
                return Mono.error(new RuntimeException("Transient failure #" + count));
            }
            successEvents.add(ctx);
            return Mono.empty();
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

    @ChangeStream(name = "reactive-exhausted-retry-stream", collection = "reactiveExhaustedRetryOrders")
    @RetryPolicy(maxAttempts = 3, initialDelay = "100ms", maxDelay = "500ms", jitter = false)
    static class ExhaustedRetryHandler {
        private final AtomicInteger invocationCount = new AtomicInteger(0);
        private final List<ChangeStreamContext<?>> successEvents = new CopyOnWriteArrayList<>();
        private final List<Integer> attemptNumbers = new CopyOnWriteArrayList<>();

        @OnChange
        Mono<Void> handle(ChangeStreamContext<?> ctx) {
            int count = invocationCount.incrementAndGet();
            attemptNumbers.add(ctx.getAttemptNumber());
            return Mono.error(new RuntimeException("Permanent failure #" + count));
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

    @ChangeStream(name = "reactive-no-retry-illegal-arg-stream", collection = "reactiveNoRetryIllegalArgOrders")
    @RetryPolicy(maxAttempts = 5, initialDelay = "100ms", maxDelay = "500ms", jitter = false)
    static class NoRetryOnIllegalArgHandler {
        private final AtomicInteger invocationCount = new AtomicInteger(0);
        private final List<Integer> attemptNumbers = new CopyOnWriteArrayList<>();

        @OnChange
        Mono<Void> handle(ChangeStreamContext<?> ctx) {
            invocationCount.incrementAndGet();
            attemptNumbers.add(ctx.getAttemptNumber());
            return Mono.error(new IllegalArgumentException("Bad argument - no retry"));
        }

        void reset() {
            invocationCount.set(0);
            attemptNumbers.clear();
        }

        int getInvocationCount() { return invocationCount.get(); }
        List<Integer> getAttemptNumbers() { return attemptNumbers; }
    }
}
