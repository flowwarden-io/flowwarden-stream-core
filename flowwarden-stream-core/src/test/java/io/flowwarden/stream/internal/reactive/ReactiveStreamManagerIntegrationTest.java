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
import io.flowwarden.stream.OperationType;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnChange;
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
import io.flowwarden.stream.test.SharedMongoContainer;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = ReactiveStreamManagerIntegrationTest.TestApp.class)
@ActiveProfiles("test-webflux")
class ReactiveStreamManagerIntegrationTest {

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired
    ReactiveMongoTemplate reactiveMongoTemplate;

    @Autowired
    TestOrderHandler testHandler;

    @Autowired
    ReactiveStreamManager streamManager;

    @Test
    void insertTriggersOnChangeHandler() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("order-watcher"));

        int before = testHandler.getReceivedEvents().size();

        reactiveMongoTemplate.insert(new Document("status", "NEW").append("amount", 42), "orders")
                .block();

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(testHandler.getReceivedEvents()).hasSizeGreaterThan(before));

        ChangeStreamContext<?> ctx = testHandler.getReceivedEvents().get(testHandler.getReceivedEvents().size() - 1);
        assertThat(ctx.getOperationType()).isEqualTo(OperationType.INSERT);
        assertThat(ctx.getCollectionName()).isEqualTo("orders");
        assertThat(ctx.getEventId()).isNotEmpty();
        assertThat(ctx.getResumeToken()).isNotNull();
        assertThat(ctx.getStreamName()).isEqualTo("order-watcher");
    }

    @Test
    void multipleInsertsAreAllReceived() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("order-watcher"));

        int before = testHandler.getReceivedEvents().size();

        reactiveMongoTemplate.insert(new Document("status", "A"), "orders").block();
        reactiveMongoTemplate.insert(new Document("status", "B"), "orders").block();
        reactiveMongoTemplate.insert(new Document("status", "C"), "orders").block();

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(testHandler.getReceivedEvents()).hasSizeGreaterThanOrEqualTo(before + 3));
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import(ReactiveStreamManagerIntegrationTest.TestOrderHandler.class)
    static class TestApp {
    }

    @ChangeStream(name = "order-watcher", collection = "orders")
    static class TestOrderHandler {
        private final List<ChangeStreamContext<?>> events = new CopyOnWriteArrayList<>();

        @OnChange
        Mono<Void> handle(ChangeStreamContext<?> ctx) {
            events.add(ctx);
            return Mono.empty();
        }

        List<ChangeStreamContext<?>> getReceivedEvents() {
            return events;
        }
    }
}
