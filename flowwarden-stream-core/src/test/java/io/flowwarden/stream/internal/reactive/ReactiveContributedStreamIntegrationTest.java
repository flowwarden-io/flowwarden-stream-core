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
    }

    static class ContributedOrderHandler {
        final List<ChangeStreamContext<?>> insertEvents = new CopyOnWriteArrayList<>();
    }
}
