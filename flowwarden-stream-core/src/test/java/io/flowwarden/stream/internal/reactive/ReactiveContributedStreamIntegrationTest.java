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
import io.flowwarden.stream.ErrorAction;
import io.flowwarden.stream.OperationType;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.registration.StreamDefinitionContributor;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * REACTIVE-mode counterpart of {@link io.flowwarden.stream.internal.imperative.ImperativeContributedStreamIntegrationTest}
 * — same end-to-end proof, consuming a {@link StreamDefinitionContributor}-declared stream
 * via reactive handlers.
 */
@SpringBootTest(classes = ReactiveContributedStreamIntegrationTest.TestApp.class)
@ActiveProfiles("test-webflux")
class ReactiveContributedStreamIntegrationTest {

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired
    ReactiveMongoTemplate reactiveMongoTemplate;

    @Autowired
    ContributedOrderHandler testHandler;

    @Autowired
    ReactiveStreamManager streamManager;

    @Test
    void contributedStreamAutoStartsAndProcessesInsert() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("reactive-contributed-order-watcher"));

        int beforeInsert = testHandler.insertEvents.size();

        reactiveMongoTemplate.insert(new Document("status", "NEW").append("amount", 10),
                "reactive_contributed_orders").block();

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(testHandler.insertEvents).hasSizeGreaterThan(beforeInsert));

        ChangeStreamContext<?> ctx = testHandler.insertEvents.get(testHandler.insertEvents.size() - 1);
        assertThat(ctx.getOperationType()).isEqualTo(OperationType.INSERT);
    }

    @Test
    void contributedPipelineRestrictsToInsertsAndFilterRestrictsToHighAmounts() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("reactive-contributed-pipeline-filter-watcher"));

        reactiveMongoTemplate.insert(new Document("amount", 50), "reactive_contributed_pf_orders").block();

        int beforeHighAmount = testHandler.pipelineFilteredEvents.size();
        reactiveMongoTemplate.insert(new Document("amount", 200), "reactive_contributed_pf_orders").block();

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(testHandler.pipelineFilteredEvents).hasSizeGreaterThan(beforeHighAmount));

        assertThat(testHandler.pipelineFilteredEvents).allSatisfy(ctx ->
                assertThat(fullDocumentAmount(ctx)).hasValue(200));
        assertThat(testHandler.pipelineFilteredEvents)
                .allSatisfy(ctx -> assertThat(ctx.getOperationType()).isEqualTo(OperationType.INSERT));
    }

    @Test
    void contributedOnErrorHandlerCatchesHandlerExceptionAndStreamSurvives() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("reactive-contributed-onerror-watcher"));

        int beforeErrors = testHandler.caughtErrors.size();
        reactiveMongoTemplate.insert(new Document("amount", 999), "reactive_contributed_error_orders").block();

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(testHandler.caughtErrors).hasSizeGreaterThan(beforeErrors));

        int beforeProcessed = testHandler.processedEvents.size();
        reactiveMongoTemplate.insert(new Document("amount", 10), "reactive_contributed_error_orders").block();

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(testHandler.processedEvents).hasSizeGreaterThan(beforeProcessed));
    }

    @SuppressWarnings("unchecked")
    private static java.util.Optional<Integer> fullDocumentAmount(ChangeStreamContext<?> ctx) {
        return ((ChangeStreamContext<Document>) ctx).getFullDocument(Document.class)
                .map(d -> d.getInteger("amount"));
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import(ReactiveContributedStreamIntegrationTest.ContributedOrderHandler.class)
    static class TestApp {

        @Bean
        StreamDefinitionContributor orderStreamContributor(ContributedOrderHandler handler) {
            return registration -> registration.stream("reactive-contributed-order-watcher", Document.class)
                    .collection("reactive_contributed_orders")
                    .onInsertReactive((order, ctx) -> {
                        handler.insertEvents.add(ctx);
                        return Mono.empty();
                    });
        }

        @Bean
        StreamDefinitionContributor pipelineFilterContributor(ContributedOrderHandler handler) {
            return registration -> registration.stream("reactive-contributed-pipeline-filter-watcher", Document.class)
                    .collection("reactive_contributed_pf_orders")
                    .pipeline(() -> List.of(new Document("$match",
                            new Document("operationType", "insert"))))
                    .filter(ctx -> ctx.getFullDocument(Document.class)
                            .map(d -> d.getInteger("amount", 0) > 100)
                            .orElse(false))
                    .onInsertReactive((order, ctx) -> {
                        handler.pipelineFilteredEvents.add(ctx);
                        return Mono.empty();
                    });
        }

        @Bean
        StreamDefinitionContributor onErrorContributor(ContributedOrderHandler handler) {
            return registration -> registration.stream("reactive-contributed-onerror-watcher", Document.class)
                    .collection("reactive_contributed_error_orders")
                    .onInsertReactive((order, ctx) -> {
                        if (order.getInteger("amount", 0) == 999) {
                            return Mono.error(new IllegalStateException("poison amount"));
                        }
                        handler.processedEvents.add(ctx);
                        return Mono.empty();
                    })
                    .onError((ex, ctx) -> {
                        handler.caughtErrors.add(ex);
                        return ErrorAction.SKIP;
                    }, IllegalStateException.class);
        }
    }

    static class ContributedOrderHandler {
        final List<ChangeStreamContext<?>> insertEvents = new CopyOnWriteArrayList<>();
        final List<ChangeStreamContext<?>> pipelineFilteredEvents = new CopyOnWriteArrayList<>();
        final List<ChangeStreamContext<?>> processedEvents = new CopyOnWriteArrayList<>();
        final List<Throwable> caughtErrors = new CopyOnWriteArrayList<>();
    }
}
