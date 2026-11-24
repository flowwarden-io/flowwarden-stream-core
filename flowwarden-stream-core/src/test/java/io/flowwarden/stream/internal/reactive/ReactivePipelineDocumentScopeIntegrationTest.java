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

import io.flowwarden.stream.FullDocumentMode;
import io.flowwarden.stream.PipelineScope;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.registration.StreamDefinitionContributor;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.match;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.newAggregation;
import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * REACTIVE-mode counterpart of
 * {@link io.flowwarden.stream.internal.imperative.ImperativePipelineDocumentScopeIntegrationTest},
 * through the programmatic registration: a contributed {@code Aggregation}
 * pipeline with the {@code DOCUMENT} scope receives the events its filter,
 * written against the document, selects.
 */
@SpringBootTest(classes = ReactivePipelineDocumentScopeIntegrationTest.TestApp.class)
@ActiveProfiles("test-webflux")
class ReactivePipelineDocumentScopeIntegrationTest {

    private static final String COLLECTION = "reactive_pipeline_scope_orders";
    private static final List<String> DOCUMENT_SCOPE = new CopyOnWriteArrayList<>();
    private static final List<String> EVENT_SCOPE = new CopyOnWriteArrayList<>();

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired ReactiveMongoTemplate reactiveMongoTemplate;
    @Autowired ReactiveStreamManager streamManager;

    @Test
    void contributedDocumentScopedFilterReceivesMatchingEvents_eventScopedSameFilterReceivesNothing() {
        await().atMost(Duration.ofSeconds(10)).until(() ->
                streamManager.isRunning("reactive-pipeline-scope-document")
                        && streamManager.isRunning("reactive-pipeline-scope-event"));

        reactiveMongoTemplate.insert(new Document("_id", "closed-1").append("status", "CLOSED"), COLLECTION).block();
        reactiveMongoTemplate.insert(new Document("_id", "open-1").append("status", "OPEN"), COLLECTION).block();
        reactiveMongoTemplate.updateFirst(Query.query(Criteria.where("_id").is("closed-1")),
                Update.update("status", "OPEN"), COLLECTION).block();

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(DOCUMENT_SCOPE).containsExactly("insert:open-1", "update:closed-1"));

        assertThat(EVENT_SCOPE).isEmpty();
        assertThat(streamManager.isRunning("reactive-pipeline-scope-event")).isTrue();
    }

    @SpringBootApplication
    @EnableFlowWarden
    static class TestApp {

        @Bean
        StreamDefinitionContributor pipelineScopeContributor() {
            return registration -> {
                registration.stream("reactive-pipeline-scope-document", Document.class)
                        .collection(COLLECTION)
                        .fullDocument(FullDocumentMode.UPDATE_LOOKUP)
                        .aggregationPipeline(() -> newAggregation(match(where("status").is("OPEN"))))
                        .pipelineScope(PipelineScope.DOCUMENT)
                        .onChangeReactive(ctx -> {
                            DOCUMENT_SCOPE.add(ctx.getOperationType().name().toLowerCase() + ":"
                                    + ctx.getDocumentKey().asDocument().getString("_id").getValue());
                            return Mono.empty();
                        });
                registration.stream("reactive-pipeline-scope-event", Document.class)
                        .collection(COLLECTION)
                        .fullDocument(FullDocumentMode.UPDATE_LOOKUP)
                        .aggregationPipeline(() -> newAggregation(match(where("status").is("OPEN"))))
                        .onChangeReactive(ctx -> {
                            EVENT_SCOPE.add(ctx.getOperationType().name().toLowerCase() + ":"
                                    + ctx.getDocumentKey().asDocument().getString("_id").getValue());
                            return Mono.empty();
                        });
            };
        }
    }
}
