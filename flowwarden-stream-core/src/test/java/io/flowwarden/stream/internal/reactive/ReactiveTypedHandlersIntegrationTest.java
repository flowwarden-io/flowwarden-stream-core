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
import io.flowwarden.stream.annotation.OnDelete;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.annotation.OnUpdate;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
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

@SpringBootTest(classes = ReactiveTypedHandlersIntegrationTest.TestApp.class)
@ActiveProfiles("test-webflux")
class ReactiveTypedHandlersIntegrationTest {

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired
    ReactiveMongoTemplate reactiveMongoTemplate;

    @Autowired
    TypedOrderHandler testHandler;

    @Autowired
    ReactiveStreamManager streamManager;

    @Test
    void insertTriggersOnInsertHandler() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("reactive-typed-watcher"));

        int beforeInsert = testHandler.insertEvents.size();
        int beforeChange = testHandler.changeEvents.size();

        reactiveMongoTemplate.insert(new Document("status", "NEW").append("amount", 10), "reactive_typed_orders")
                .block();

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(testHandler.insertEvents).hasSizeGreaterThan(beforeInsert));

        assertThat(testHandler.insertEvents.size()).isGreaterThan(beforeInsert);
        assertThat(testHandler.changeEvents.size()).isEqualTo(beforeChange);

        ChangeStreamContext<?> ctx = testHandler.insertEvents.get(testHandler.insertEvents.size() - 1);
        assertThat(ctx.getOperationType()).isEqualTo(OperationType.INSERT);
    }

    @Test
    void updateTriggersOnUpdateHandler() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("reactive-typed-watcher"));

        Document doc = new Document("status", "PENDING").append("amount", 20);
        reactiveMongoTemplate.insert(doc, "reactive_typed_orders").block();

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(testHandler.insertEvents).isNotEmpty());

        int beforeUpdate = testHandler.updateEvents.size();

        reactiveMongoTemplate.updateFirst(
                Query.query(Criteria.where("_id").is(doc.get("_id"))),
                Update.update("status", "SHIPPED"),
                "reactive_typed_orders").block();

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(testHandler.updateEvents).hasSizeGreaterThan(beforeUpdate));

        ChangeStreamContext<?> ctx = testHandler.updateEvents.get(testHandler.updateEvents.size() - 1);
        assertThat(ctx.getOperationType()).isEqualTo(OperationType.UPDATE);
    }

    @Test
    void deleteTriggersOnDeleteHandler() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("reactive-typed-watcher"));

        Document doc = new Document("status", "TO_DELETE").append("amount", 30);
        reactiveMongoTemplate.insert(doc, "reactive_typed_orders").block();

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(testHandler.insertEvents).isNotEmpty());

        int beforeDelete = testHandler.deleteEvents.size();

        reactiveMongoTemplate.remove(
                Query.query(Criteria.where("_id").is(doc.get("_id"))),
                "reactive_typed_orders").block();

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(testHandler.deleteEvents).hasSizeGreaterThan(beforeDelete));

        ChangeStreamContext<?> ctx = testHandler.deleteEvents.get(testHandler.deleteEvents.size() - 1);
        assertThat(ctx.getOperationType()).isEqualTo(OperationType.DELETE);
    }

    @Test
    void replaceWithNoOnReplaceFallsBackToOnChange() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("reactive-typed-watcher"));

        Document doc = new Document("status", "ORIGINAL").append("amount", 40);
        reactiveMongoTemplate.insert(doc, "reactive_typed_orders").block();

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(testHandler.insertEvents).isNotEmpty());

        int beforeChange = testHandler.changeEvents.size();

        Document replacement = new Document("_id", doc.get("_id"))
                .append("status", "REPLACED").append("amount", 99);
        reactiveMongoTemplate.save(replacement, "reactive_typed_orders").block();

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(testHandler.changeEvents).hasSizeGreaterThan(beforeChange));

        ChangeStreamContext<?> ctx = testHandler.changeEvents.get(testHandler.changeEvents.size() - 1);
        assertThat(ctx.getOperationType()).isEqualTo(OperationType.REPLACE);
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import(ReactiveTypedHandlersIntegrationTest.TypedOrderHandler.class)
    static class TestApp {
    }

    @ChangeStream(name = "reactive-typed-watcher", collection = "reactive_typed_orders")
    static class TypedOrderHandler {
        final List<ChangeStreamContext<?>> insertEvents = new CopyOnWriteArrayList<>();
        final List<ChangeStreamContext<?>> updateEvents = new CopyOnWriteArrayList<>();
        final List<ChangeStreamContext<?>> deleteEvents = new CopyOnWriteArrayList<>();
        final List<ChangeStreamContext<?>> changeEvents = new CopyOnWriteArrayList<>();

        @OnInsert
        Mono<Void> onInsert(ChangeStreamContext<?> ctx) {
            insertEvents.add(ctx);
            return Mono.empty();
        }

        @OnUpdate
        Mono<Void> onUpdate(ChangeStreamContext<?> ctx) {
            updateEvents.add(ctx);
            return Mono.empty();
        }

        @OnDelete
        Mono<Void> onDelete(ChangeStreamContext<?> ctx) {
            deleteEvents.add(ctx);
            return Mono.empty();
        }

        @OnChange
        Mono<Void> onChange(ChangeStreamContext<?> ctx) {
            changeEvents.add(ctx);
            return Mono.empty();
        }
    }
}
