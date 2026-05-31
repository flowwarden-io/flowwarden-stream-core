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

import com.mongodb.reactivestreams.client.MongoClients;
import io.flowwarden.stream.ChangeStreamContext;
import io.flowwarden.stream.OperationType;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnChange;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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
 * Reactive integration test verifying that {@code @ChangeStream(mongoTemplateRef = "...")}
 * uses the correct ReactiveMongoTemplate bean to subscribe to change streams.
 */
@SpringBootTest(classes = ReactiveMongoTemplateRefIntegrationTest.TestApp.class)
@ActiveProfiles("test-webflux")
class ReactiveMongoTemplateRefIntegrationTest {

    private static final String SECONDARY_DB = "fw_ref_test_reactive";

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired
    @Qualifier("secondaryReactiveMongoTemplate")
    ReactiveMongoTemplate secondaryReactiveMongoTemplate;

    @Autowired
    SecondaryDbHandler secondaryHandler;

    @Autowired
    ReactiveStreamManager streamManager;

    @Test
    void insertInSecondaryDbTriggersHandlerWithCustomTemplateRef() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("reactive-ref-test-secondary"));

        int before = secondaryHandler.getReceivedEvents().size();

        // Insert into the secondary database's collection
        secondaryReactiveMongoTemplate.insert(
                new Document("source", "secondary").append("ts", System.currentTimeMillis()),
                "ref_test_events").block();

        // The secondary handler should receive the event
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(secondaryHandler.getReceivedEvents())
                                .hasSizeGreaterThan(before));

        ChangeStreamContext<?> ctx = secondaryHandler.getReceivedEvents()
                .get(secondaryHandler.getReceivedEvents().size() - 1);
        assertThat(ctx.getOperationType()).isEqualTo(OperationType.INSERT);
        assertThat(ctx.getStreamName()).isEqualTo("reactive-ref-test-secondary");
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import({
            ReactiveMongoTemplateRefIntegrationTest.SecondaryMongoConfig.class,
            ReactiveMongoTemplateRefIntegrationTest.SecondaryDbHandler.class
    })
    static class TestApp {
    }

    @Configuration
    static class SecondaryMongoConfig {
        @Bean
        ReactiveMongoTemplate secondaryReactiveMongoTemplate() {
            return new ReactiveMongoTemplate(
                    MongoClients.create(SharedMongoContainer.MONGO.getReplicaSetUrl()),
                    SECONDARY_DB);
        }
    }

    @ChangeStream(name = "reactive-ref-test-secondary", collection = "ref_test_events",
            mongoTemplateRef = "secondaryReactiveMongoTemplate")
    static class SecondaryDbHandler {
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
