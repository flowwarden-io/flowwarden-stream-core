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
import io.flowwarden.stream.FullDocumentMode;
import io.flowwarden.stream.PipelineScope;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnChange;
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
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
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
import static org.springframework.data.mongodb.core.aggregation.Aggregation.match;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.newAggregation;
import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * The same filter, written against the document, on the same collection with
 * the two scopes: {@code DOCUMENT} receives the matching events, {@code EVENT}
 * sends the stage as written and receives none — the silent failure the scope
 * exists to remove.
 */
@SpringBootTest(classes = ImperativePipelineDocumentScopeIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativePipelineDocumentScopeIntegrationTest {

    private static final String COLLECTION = "pipeline_scope_orders";

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired MongoTemplate mongoTemplate;
    @Autowired ImperativeStreamManager streamManager;
    @Autowired DocumentScopeHandler documentScope;
    @Autowired OperationListDocumentScopeHandler operationListScope;
    @Autowired EventScopeHandler eventScope;

    @Test
    void documentScopedFilterReceivesMatchingInsertsAndUpdates_eventScopedSameFilterReceivesNothing() {
        await().atMost(Duration.ofSeconds(10)).until(() ->
                streamManager.isRunning("pipeline-scope-document")
                        && streamManager.isRunning("pipeline-scope-document-ops")
                        && streamManager.isRunning("pipeline-scope-event"));

        mongoTemplate.insert(new Document("_id", "closed-1").append("status", "CLOSED"), COLLECTION);
        mongoTemplate.insert(new Document("_id", "open-1").append("status", "OPEN"), COLLECTION);
        // An update is selected on the looked-up document, like an insert.
        mongoTemplate.updateFirst(Query.query(Criteria.where("_id").is("closed-1")),
                Update.update("status", "OPEN"), COLLECTION);
        // Deletes carry no fullDocument: only the mixed event/document filter lets one through.
        mongoTemplate.remove(Query.query(Criteria.where("_id").is("open-1")), COLLECTION);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(documentScope.seen).containsExactly("insert:open-1", "update:closed-1");
            assertThat(operationListScope.seen)
                    .containsExactly("insert:open-1", "update:closed-1", "delete:open-1");
        });

        // The event-scoped stream saw the same four writes pass and matched none of them.
        assertThat(eventScope.seen).isEmpty();
        assertThat(streamManager.isRunning("pipeline-scope-event")).isTrue();
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import({DocumentScopeHandler.class, OperationListDocumentScopeHandler.class, EventScopeHandler.class})
    static class TestApp {}

    abstract static class Recorder {
        final List<String> seen = new CopyOnWriteArrayList<>();

        void record(ChangeStreamContext<Document> ctx) {
            seen.add(ctx.getOperationType().name().toLowerCase() + ":"
                    + ctx.getDocumentKey().asDocument().getString("_id").getValue());
        }
    }

    @ChangeStream(name = "pipeline-scope-document", collection = COLLECTION, documentType = Document.class,
            fullDocument = FullDocumentMode.UPDATE_LOOKUP)
    static class DocumentScopeHandler extends Recorder {

        @Pipeline(scope = PipelineScope.DOCUMENT)
        Aggregation pipeline() {
            return newAggregation(match(where("status").is("OPEN")));
        }

        @OnChange
        void handle(ChangeStreamContext<Document> ctx) {
            record(ctx);
        }
    }

    @ChangeStream(name = "pipeline-scope-document-ops", collection = COLLECTION, documentType = Document.class,
            fullDocument = FullDocumentMode.UPDATE_LOOKUP)
    static class OperationListDocumentScopeHandler extends Recorder {

        /** Event fields and document fields in one filter: only the document ones are prefixed. */
        @Pipeline(scope = PipelineScope.DOCUMENT)
        List<AggregationOperation> pipeline() {
            return List.of(match(new Criteria().orOperator(
                    where("operationType").is("delete"),
                    where("status").is("OPEN"))));
        }

        @OnChange
        void handle(ChangeStreamContext<Document> ctx) {
            record(ctx);
        }
    }

    @ChangeStream(name = "pipeline-scope-event", collection = COLLECTION, documentType = Document.class,
            fullDocument = FullDocumentMode.UPDATE_LOOKUP)
    static class EventScopeHandler extends Recorder {

        @Pipeline
        Aggregation pipeline() {
            return newAggregation(match(where("status").is("OPEN")));
        }

        @OnChange
        void handle(ChangeStreamContext<Document> ctx) {
            record(ctx);
        }
    }
}
