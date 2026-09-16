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
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end proof (real MongoDB, real application startup) for the properties #87
 * requires of {@link StreamDefinitionContributor}: the contributed stream actually
 * autostarts and processes real events in IMPERATIVE mode — {@code streamManager.isRunning}
 * becoming true after {@code ApplicationReadyEvent} is the black-box evidence that the
 * contribution phase completed before the managers started reading the catalog; a
 * contribution completing too late would leave this stream permanently absent and
 * {@code isRunning} would never turn {@code true}.
 */
@SpringBootTest(classes = ImperativeContributedStreamIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeContributedStreamIntegrationTest {

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired
    MongoTemplate mongoTemplate;

    @Autowired
    ContributedOrderHandler testHandler;

    @Autowired
    ImperativeStreamManager streamManager;

    @Test
    void contributedStreamAutoStartsAndProcessesInsert() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("contributed-order-watcher"));

        int beforeInsert = testHandler.insertEvents.size();

        mongoTemplate.insert(new Document("status", "NEW").append("amount", 10), "contributed_orders");

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(testHandler.insertEvents).hasSizeGreaterThan(beforeInsert));

        ChangeStreamContext<?> ctx = testHandler.insertEvents.get(testHandler.insertEvents.size() - 1);
        assertThat(ctx.getOperationType()).isEqualTo(OperationType.INSERT);
    }

    @Test
    void contributedStreamProcessesUpdate() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("contributed-order-watcher"));

        Document doc = new Document("status", "PENDING").append("amount", 20);
        mongoTemplate.insert(doc, "contributed_orders");

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(testHandler.insertEvents).isNotEmpty());

        int beforeUpdate = testHandler.updateEvents.size();

        mongoTemplate.updateFirst(
                Query.query(Criteria.where("_id").is(doc.get("_id"))),
                Update.update("status", "SHIPPED"),
                "contributed_orders");

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(testHandler.updateEvents).hasSizeGreaterThan(beforeUpdate));

        ChangeStreamContext<?> ctx = testHandler.updateEvents.get(testHandler.updateEvents.size() - 1);
        assertThat(ctx.getOperationType()).isEqualTo(OperationType.UPDATE);
    }

    @Test
    void contributedPipelineRestrictsToInsertsAndFilterRestrictsToHighAmounts() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("contributed-pipeline-filter-watcher"));

        mongoTemplate.insert(new Document("amount", 50), "contributed_pf_orders");

        int beforeHighAmount = testHandler.pipelineFilteredEvents.size();
        Document highAmountDoc = new Document("amount", 200);
        mongoTemplate.insert(highAmountDoc, "contributed_pf_orders");

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(testHandler.pipelineFilteredEvents).hasSizeGreaterThan(beforeHighAmount));

        // The low-amount insert never reached the handler (client-side @Filter equivalent).
        assertThat(testHandler.pipelineFilteredEvents).allSatisfy(ctx ->
                assertThat(fullDocumentAmount(ctx)).hasValue(200));

        // An update on the high-amount doc never reaches the handler either — the
        // server-side pipeline equivalent restricts this stream to insert events only.
        mongoTemplate.updateFirst(
                Query.query(Criteria.where("_id").is(highAmountDoc.get("_id"))),
                Update.update("amount", 300),
                "contributed_pf_orders");

        // Insert a second high-amount doc to get a synchronization point after the update.
        mongoTemplate.insert(new Document("amount", 250), "contributed_pf_orders");
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(testHandler.pipelineFilteredEvents).hasSizeGreaterThan(beforeHighAmount + 1));

        assertThat(testHandler.pipelineFilteredEvents)
                .allSatisfy(ctx -> assertThat(ctx.getOperationType()).isEqualTo(OperationType.INSERT));
    }

    @Test
    void contributedOnErrorHandlerCatchesHandlerExceptionAndStreamSurvives() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("contributed-onerror-watcher"));

        int beforeErrors = testHandler.caughtErrors.size();
        mongoTemplate.insert(new Document("amount", 999), "contributed_error_orders");

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(testHandler.caughtErrors).hasSizeGreaterThan(beforeErrors));

        // The stream survived the handler exception: a subsequent normal event still processes.
        int beforeProcessed = testHandler.processedEvents.size();
        mongoTemplate.insert(new Document("amount", 10), "contributed_error_orders");

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
    @Import(ImperativeContributedStreamIntegrationTest.ContributedOrderHandler.class)
    static class TestApp {

        @Bean
        StreamDefinitionContributor orderStreamContributor(ContributedOrderHandler handler) {
            return registration -> registration.stream("contributed-order-watcher", Document.class)
                    .collection("contributed_orders")
                    .onInsert((order, ctx) -> handler.insertEvents.add(ctx))
                    .onUpdate((order, ctx) -> handler.updateEvents.add(ctx));
        }

        @Bean
        StreamDefinitionContributor pipelineFilterContributor(ContributedOrderHandler handler) {
            return registration -> registration.stream("contributed-pipeline-filter-watcher", Document.class)
                    .collection("contributed_pf_orders")
                    .pipeline(() -> List.of(new Document("$match",
                            new Document("operationType", "insert"))))
                    .filter(ctx -> ctx.getFullDocument(Document.class)
                            .map(d -> d.getInteger("amount", 0) > 100)
                            .orElse(false))
                    .onInsert((order, ctx) -> handler.pipelineFilteredEvents.add(ctx));
        }

        @Bean
        StreamDefinitionContributor onErrorContributor(ContributedOrderHandler handler) {
            return registration -> registration.stream("contributed-onerror-watcher", Document.class)
                    .collection("contributed_error_orders")
                    .onInsert((order, ctx) -> {
                        if (order.getInteger("amount", 0) == 999) {
                            throw new IllegalStateException("poison amount");
                        }
                        handler.processedEvents.add(ctx);
                    })
                    .onError((ex, ctx) -> {
                        handler.caughtErrors.add(ex);
                        return ErrorAction.SKIP;
                    }, IllegalStateException.class);
        }
    }

    static class ContributedOrderHandler {
        final List<ChangeStreamContext<?>> insertEvents = new CopyOnWriteArrayList<>();
        final List<ChangeStreamContext<?>> updateEvents = new CopyOnWriteArrayList<>();
        final List<ChangeStreamContext<?>> pipelineFilteredEvents = new CopyOnWriteArrayList<>();
        final List<ChangeStreamContext<?>> processedEvents = new CopyOnWriteArrayList<>();
        final List<Throwable> caughtErrors = new CopyOnWriteArrayList<>();
    }
}
