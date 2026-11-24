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
package io.flowwarden.stream.internal.discovery;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.flowwarden.stream.DeploymentMode;
import io.flowwarden.stream.FullDocumentBeforeChangeMode;
import io.flowwarden.stream.FullDocumentMode;
import io.flowwarden.stream.OperationType;
import io.flowwarden.stream.PipelineScope;
import io.flowwarden.stream.core.ContextHandler;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.match;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.newAggregation;
import static org.springframework.data.mongodb.core.query.Criteria.where;

class PipelineInspectorTest {

    private final Logger logger = (Logger) LoggerFactory.getLogger(PipelineInspector.class);
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

    @BeforeEach
    void attach() {
        appender.start();
        logger.addAppender(appender);
    }

    @AfterEach
    void detach() {
        logger.detachAppender(appender);
    }

    private static Document matchStage(Document query) {
        return new Document("$match", query);
    }

    private List<String> warnings() {
        return appender.list.stream()
                .filter(e -> e.getLevel() == Level.WARN)
                .map(ILoggingEvent::getFormattedMessage)
                .toList();
    }

    private static ChangeStreamDefinition definition(PipelineMethod pipeline, FullDocumentMode fullDocument,
                                                     boolean onUpdate) {
        Map<OperationType, HandlerMethod> typed = new EnumMap<>(OperationType.class);
        if (onUpdate) {
            ContextHandler<Document> handler = ctx -> { };
            typed.put(OperationType.UPDATE, HandlerMethod.fromContextHandler(handler));
        }
        StreamConfig config = new StreamConfig(true, true, Document.class, "", fullDocument,
                FullDocumentBeforeChangeMode.OFF, DeploymentMode.ALL_INSTANCES);
        return new ChangeStreamDefinition("orders-stream", "orders", "", "", new Object(), null, typed, config,
                pipeline, null, null, null, null, null, null,
                new ErrorHandlerResolver(List.of()), Collections.emptyMap());
    }

    private static PipelineMethod rawPipeline(Document... stages) {
        return PipelineMethod.fromSupplier(() -> List.of(stages));
    }

    // --- analyze ---

    @Test
    void reportsATopLevelFieldThatIsNotAnEventField() {
        var findings = PipelineInspector.analyze(List.of(matchStage(new Document("status", "OPEN"))));

        assertThat(findings.unknownFields()).containsExactly("status");
    }

    @Test
    void acceptsEveryEventFieldAndItsSubPaths() {
        Document query = new Document();
        ChangeEventFields.TOP_LEVEL.forEach(f -> query.append(f, 1).append(f + ".sub", 1));

        var findings = PipelineInspector.analyze(List.of(matchStage(query)));

        assertThat(findings.unknownFields()).isEmpty();
        assertThat(findings.referencesFullDocument()).isTrue();
        assertThat(findings.matchesOnEventId()).isTrue();
    }

    @Test
    void descendsIntoLogicalOperatorsAndReportsEachFieldOnce() {
        Document query = new Document("$or", List.of(
                new Document("operationType", "delete"),
                new Document("$and", List.of(
                        new Document("status", "OPEN"),
                        new Document("$nor", List.of(new Document("archived", true), new Document("status", 1)))))));

        var findings = PipelineInspector.analyze(List.of(matchStage(query), matchStage(new Document("status", 2))));

        assertThat(findings.unknownFields()).containsExactly("status", "archived");
    }

    @Test
    void ignoresExprAndOtherOperators() {
        Document query = new Document("$expr", new Document("$eq", List.of("$status", "OPEN")))
                .append("$comment", "status")
                .append("$jsonSchema", new Document("required", List.of("status")));

        assertThat(PipelineInspector.analyze(List.of(matchStage(query))).unknownFields()).isEmpty();
    }

    @Test
    void doesNotLookInsideTheOperandOfAField() {
        Document query = new Document("fullDocument.lines",
                new Document("$elemMatch", new Document("sku", "A")))
                .append("documentKey", new Document("_id", 1).append("tenant", "t"));

        assertThat(PipelineInspector.analyze(List.of(matchStage(query))).unknownFields()).isEmpty();
    }

    @Test
    void stopsAtTheFirstReshapingStage() {
        for (String stage : ChangeEventFields.RESHAPING_STAGES) {
            var findings = PipelineInspector.analyze(List.of(
                    matchStage(new Document("operationType", "insert")),
                    new Document(stage, new Document("status", "$fullDocument.status")),
                    matchStage(new Document("status", "OPEN"))));

            assertThat(findings.unknownFields()).as(stage).isEmpty();
        }
    }

    @Test
    void stopsAtAStageItDoesNotKnow() {
        var findings = PipelineInspector.analyze(List.of(
                new Document("$someFutureStage", new Document()),
                matchStage(new Document("status", "OPEN"))));

        assertThat(findings.unknownFields()).isEmpty();
    }

    @Test
    void continuesPastStagesThatKeepTheShape() {
        var findings = PipelineInspector.analyze(List.of(
                new Document("$redact", "$$KEEP"),
                new Document("$changeStreamSplitLargeEvent", new Document()),
                matchStage(new Document("status", "OPEN"))));

        assertThat(findings.unknownFields()).containsExactly("status");
    }

    @Test
    void readsAPipelineRenderedFromRawBsonTypes() {
        // A List<Bson> built with the driver's Filters is parsed back into Documents by PipelineMethod.
        List<Document> resolved = PipelineMethod.fromSupplier(() -> List.of(
                com.mongodb.client.model.Aggregates.match(com.mongodb.client.model.Filters.or(
                        com.mongodb.client.model.Filters.eq("status", "OPEN"),
                        com.mongodb.client.model.Filters.eq("operationType", "delete")))))
                .resolve(new Object());

        assertThat(PipelineInspector.analyze(resolved).unknownFields()).containsExactly("status");
    }

    // --- inspect (logging) ---

    @Test
    void warnsOnceNamingTheStreamTheFieldsAndTheFix() {
        Document stage = matchStage(new Document("status", "OPEN").append("amount", 3));
        ChangeStreamDefinition def = definition(rawPipeline(stage), FullDocumentMode.DEFAULT, false);

        PipelineInspector.inspect(def, List.of(stage));

        assertThat(warnings()).singleElement().satisfies(message -> assertThat(message)
                .contains("orders-stream")
                .contains("[status, amount]")
                .contains("fullDocument.status")
                .contains("PipelineScope.DOCUMENT"));
    }

    @Test
    void staysSilentOnAValidPipeline() {
        Document stage = matchStage(new Document("operationType", "insert").append("fullDocument.status", "OPEN"));
        ChangeStreamDefinition def = definition(rawPipeline(stage), FullDocumentMode.DEFAULT, false);

        PipelineInspector.inspect(def, List.of(stage));

        assertThat(warnings()).isEmpty();
    }

    @Test
    void aDocumentScopedPipelineIsInspectedAfterTheRewriteAndStaysSilent() {
        PipelineMethod pipeline = PipelineMethod.fromAggregationSupplier(
                () -> newAggregation(match(where("status").is("OPEN"))), PipelineScope.DOCUMENT);
        ChangeStreamDefinition def = definition(pipeline, FullDocumentMode.UPDATE_LOOKUP, true);

        List<Document> resolved = def.resolvePipeline();

        assertThat(resolved).containsExactly(matchStage(new Document("fullDocument.status", "OPEN")));
        assertThat(warnings()).isEmpty();
    }

    @Test
    void resolvingThroughTheDefinitionRunsTheInspection() {
        ChangeStreamDefinition def = definition(
                PipelineMethod.fromAggregationSupplier(
                        () -> newAggregation(match(where("status").is("OPEN"))), PipelineScope.EVENT),
                FullDocumentMode.DEFAULT, false);

        List<Document> resolved = def.resolvePipeline();

        assertThat(resolved).containsExactly(matchStage(new Document("status", "OPEN")));
        assertThat(warnings()).singleElement().asString().contains("[status]");
    }

    @Test
    void warnsWhenADocumentScopedPipelineMatchesOnId() {
        PipelineMethod pipeline = PipelineMethod.fromAggregationSupplier(
                () -> newAggregation(match(where("_id").is("order-1"))), PipelineScope.DOCUMENT);
        ChangeStreamDefinition def = definition(pipeline, FullDocumentMode.DEFAULT, false);

        def.resolvePipeline();

        assertThat(warnings()).singleElement().asString()
                .contains("orders-stream").contains("documentKey._id");
    }

    @Test
    void anEventScopedMatchOnIdIsNotReported() {
        Document stage = matchStage(new Document("_id", new Document("_data", "82")));
        ChangeStreamDefinition def = definition(rawPipeline(stage), FullDocumentMode.DEFAULT, false);

        PipelineInspector.inspect(def, List.of(stage));

        assertThat(warnings()).isEmpty();
    }

    @Test
    void warnsWhenThePipelineReadsFullDocumentInDefaultModeWithAnUpdateHandler() {
        Document stage = matchStage(new Document("fullDocument.status", "OPEN"));
        ChangeStreamDefinition def = definition(rawPipeline(stage), FullDocumentMode.DEFAULT, true);

        PipelineInspector.inspect(def, List.of(stage));

        assertThat(warnings()).singleElement().asString()
                .contains("orders-stream").contains("UPDATE_LOOKUP");
    }

    @Test
    void noFullDocumentWarningWithUpdateLookupOrWithoutAnUpdateHandler() {
        Document stage = matchStage(new Document("fullDocument.status", "OPEN"));

        PipelineInspector.inspect(definition(rawPipeline(stage), FullDocumentMode.UPDATE_LOOKUP, true),
                List.of(stage));
        PipelineInspector.inspect(definition(rawPipeline(stage), FullDocumentMode.DEFAULT, false),
                List.of(stage));

        assertThat(warnings()).isEmpty();
    }

    @Test
    void neverThrowsOnAMalformedPipeline() {
        ChangeStreamDefinition def = definition(null, FullDocumentMode.DEFAULT, false);
        List<Document> malformed = new java.util.ArrayList<>();
        malformed.add(null);
        malformed.add(new Document("$match", "not-a-document"));

        assertThatCode(() -> PipelineInspector.inspect(def, malformed)).doesNotThrowAnyException();
        assertThatCode(() -> PipelineInspector.inspect(def, null)).doesNotThrowAnyException();
        assertThatCode(() -> PipelineInspector.inspect(def, List.of(
                new Document("$match", new Document("$or", "oops"))))).doesNotThrowAnyException();
        assertThat(warnings()).isEmpty();
    }
}
