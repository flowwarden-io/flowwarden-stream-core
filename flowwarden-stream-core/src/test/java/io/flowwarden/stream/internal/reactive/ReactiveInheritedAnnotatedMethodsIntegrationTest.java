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

import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.annotation.Pipeline;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
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
 * {@link io.flowwarden.stream.internal.imperative.ImperativeInheritedAnnotatedMethodsIntegrationTest}.
 */
@SpringBootTest(classes = ReactiveInheritedAnnotatedMethodsIntegrationTest.TestApp.class)
@ActiveProfiles("test-webflux")
class ReactiveInheritedAnnotatedMethodsIntegrationTest {

    private static final String ORDERS = "reactive_inherited_orders";
    private static final String INVOICES = "reactive_inherited_invoices";

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired ReactiveMongoTemplate reactiveMongoTemplate;
    @Autowired ReactiveStreamManager streamManager;
    @Autowired OrdersStream ordersStream;
    @Autowired InvoicesStream invoicesStream;

    @Test
    void inheritedPipelineFiltersAndInheritedHandlerReceivesTheTypedDocument() {
        await().atMost(Duration.ofSeconds(10)).until(() ->
                streamManager.isRunning("reactive-inherited-orders") && streamManager.isRunning("reactive-inherited-invoices"));

        reactiveMongoTemplate.insert(new Document("_id", "order-closed").append("status", "CLOSED"), ORDERS).block();
        reactiveMongoTemplate.insert(new Document("_id", "order-open").append("status", "OPEN"), ORDERS).block();
        reactiveMongoTemplate.insert(new Document("_id", "invoice-closed").append("status", "CLOSED"), INVOICES).block();
        reactiveMongoTemplate.insert(new Document("_id", "invoice-open").append("status", "OPEN"), INVOICES).block();
        // Sentinels: once they are delivered, the closed documents inserted before them
        // would have been delivered too if the inherited pipeline were not applied.
        reactiveMongoTemplate.insert(new Document("_id", "order-last").append("status", "OPEN"), ORDERS).block();
        reactiveMongoTemplate.insert(new Document("_id", "invoice-last").append("status", "OPEN"), INVOICES).block();

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(ordersStream.received).extracting(Object::getClass).containsOnly((Class) Order.class);
            assertThat(ordersStream.received).extracting(o -> o.id).containsExactly("order-open", "order-last");
            assertThat(invoicesStream.received).extracting(Object::getClass).containsOnly((Class) Invoice.class);
            assertThat(invoicesStream.received).extracting(i -> i.id).containsExactly("invoice-open", "invoice-last");
        });
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import({OrdersStream.class, InvoicesStream.class})
    static class TestApp {}

    static class Order {
        String id;
        String status;
    }

    static class Invoice {
        String id;
        String status;
    }

    abstract static class BaseStream<T> {

        final List<T> received = new CopyOnWriteArrayList<>();

        @Pipeline
        Aggregation pipeline() {
            return newAggregation(match(where("fullDocument.status").is("OPEN")));
        }

        @OnInsert
        Mono<Void> onInsert(T document) {
            received.add(document);
            return Mono.empty();
        }
    }

    @ChangeStream(name = "reactive-inherited-orders", collection = ORDERS, documentType = Order.class)
    static class OrdersStream extends BaseStream<Order> {
    }

    @ChangeStream(name = "reactive-inherited-invoices", collection = INVOICES, documentType = Invoice.class)
    static class InvoicesStream extends BaseStream<Invoice> {
    }
}
