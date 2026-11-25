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
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.match;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.newAggregation;
import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * Two streams that only differ by their document type share their {@code @Pipeline}
 * and their {@code @OnInsert} handler through a generic superclass: the inherited
 * pipeline filters on the server and the inherited handler receives the document
 * converted to the type each subclass binds.
 */
@SpringBootTest(classes = ImperativeInheritedAnnotatedMethodsIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeInheritedAnnotatedMethodsIntegrationTest {

    private static final String ORDERS = "inherited_orders";
    private static final String INVOICES = "inherited_invoices";

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired MongoTemplate mongoTemplate;
    @Autowired ImperativeStreamManager streamManager;
    @Autowired OrdersStream ordersStream;
    @Autowired InvoicesStream invoicesStream;

    @Test
    void inheritedPipelineFiltersAndInheritedHandlerReceivesTheTypedDocument() {
        await().atMost(Duration.ofSeconds(10)).until(() ->
                streamManager.isRunning("inherited-orders") && streamManager.isRunning("inherited-invoices"));

        mongoTemplate.insert(new Document("_id", "order-closed").append("status", "CLOSED"), ORDERS);
        mongoTemplate.insert(new Document("_id", "order-open").append("status", "OPEN"), ORDERS);
        mongoTemplate.insert(new Document("_id", "invoice-closed").append("status", "CLOSED"), INVOICES);
        mongoTemplate.insert(new Document("_id", "invoice-open").append("status", "OPEN"), INVOICES);
        // Sentinels: once they are delivered, the closed documents inserted before them
        // would have been delivered too if the inherited pipeline were not applied.
        mongoTemplate.insert(new Document("_id", "order-last").append("status", "OPEN"), ORDERS);
        mongoTemplate.insert(new Document("_id", "invoice-last").append("status", "OPEN"), INVOICES);

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
        void onInsert(T document) {
            received.add(document);
        }
    }

    @ChangeStream(name = "inherited-orders", collection = ORDERS, documentType = Order.class)
    static class OrdersStream extends BaseStream<Order> {
    }

    @ChangeStream(name = "inherited-invoices", collection = INVOICES, documentType = Invoice.class)
    static class InvoicesStream extends BaseStream<Invoice> {
    }
}
