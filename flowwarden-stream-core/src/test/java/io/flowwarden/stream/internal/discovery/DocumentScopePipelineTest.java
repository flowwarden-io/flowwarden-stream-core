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

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.query.Criteria;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.match;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.newAggregation;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.project;
import static org.springframework.data.mongodb.core.query.Criteria.where;

class DocumentScopePipelineTest {

    private static List<Document> rewrite(Aggregation aggregation) {
        return DocumentScopePipeline.toEventScope(aggregation.toPipeline(Aggregation.DEFAULT_CONTEXT));
    }

    private static Document matchOf(List<Document> pipeline, int index) {
        return pipeline.get(index).get("$match", Document.class);
    }

    @Test
    void prefixesADocumentField() {
        List<Document> result = rewrite(newAggregation(match(where("status").is("OPEN"))));

        assertThat(result).containsExactly(
                new Document("$match", new Document("fullDocument.status", "OPEN")));
    }

    @Test
    void prefixesANestedDocumentPath() {
        List<Document> result = rewrite(newAggregation(match(where("customer.country").is("BE"))));

        assertThat(matchOf(result, 0)).containsOnlyKeys("fullDocument.customer.country");
    }

    @Test
    void leavesEveryTopLevelEventFieldAlone() {
        for (String field : ChangeEventFields.TOP_LEVEL) {
            List<Document> result = rewrite(newAggregation(match(where(field).is("x"))));
            assertThat(matchOf(result, 0)).as(field).containsOnlyKeys(field);

            List<Document> nested = rewrite(newAggregation(match(where(field + ".sub").is("x"))));
            assertThat(matchOf(nested, 0)).as(field + ".sub").containsOnlyKeys(field + ".sub");
        }
    }

    @Test
    void doesNotPrefixAFieldAlreadyWrittenUnderFullDocument() {
        List<Document> result = rewrite(newAggregation(match(where("fullDocument.status").is("OPEN"))));

        assertThat(matchOf(result, 0)).containsOnlyKeys("fullDocument.status");
    }

    @Test
    void eventIdStaysTheEventId() {
        List<Document> result = rewrite(newAggregation(match(where("_id").is("x"))));

        assertThat(matchOf(result, 0)).containsOnlyKeys("_id");
    }

    @Test
    void aDocumentFieldThatOnlyStartsLikeAnEventFieldIsPrefixed() {
        // "nsCode" is not "ns": the comparison is on the first path segment, not on a string prefix.
        List<Document> result = rewrite(newAggregation(match(where("nsCode").is("x"))));

        assertThat(matchOf(result, 0)).containsOnlyKeys("fullDocument.nsCode");
    }

    @Test
    void mixesEventAndDocumentConditionsInOneMatch() {
        List<Document> result = rewrite(newAggregation(
                match(where("operationType").in("insert", "update").and("status").is("OPEN"))));

        assertThat(matchOf(result, 0)).containsOnlyKeys("operationType", "fullDocument.status");
    }

    @Test
    @SuppressWarnings("unchecked")
    void descendsIntoNestedLogicalOperators() {
        Criteria criteria = new Criteria().orOperator(
                where("operationType").is("delete"),
                new Criteria().andOperator(
                        where("status").is("OPEN"),
                        new Criteria().norOperator(where("archived").is(true))));

        Document match = matchOf(rewrite(newAggregation(match(criteria))), 0);

        List<Document> or = (List<Document>) match.get("$or");
        assertThat(or.get(0)).containsOnlyKeys("operationType");
        List<Document> and = (List<Document>) or.get(1).get("$and");
        assertThat(and.get(0)).containsOnlyKeys("fullDocument.status");
        List<Document> nor = (List<Document>) and.get(1).get("$nor");
        assertThat(nor.get(0)).containsOnlyKeys("fullDocument.archived");
    }

    @Test
    void operandsOfAFieldAreRelativeToItAndLeftAlone() {
        List<Document> result = rewrite(newAggregation(
                match(where("lines").elemMatch(where("sku").is("A").and("qty").gt(2)))));

        Document match = matchOf(result, 0);
        assertThat(match).containsOnlyKeys("fullDocument.lines");
        Document elemMatch = match.get("fullDocument.lines", Document.class).get("$elemMatch", Document.class);
        assertThat(elemMatch).containsOnlyKeys("sku", "qty");
    }

    @Test
    void anArrayOfDocumentsComparedByEqualityIsNotRewritten() {
        // Spring Data's prefixing context rewrites the keys of documents inside any array value,
        // turning this operand into [{ "fullDocument.sku": "A" }]. Only logical operators hold queries.
        List<Document> operand = List.of(new Document("sku", "A"));
        List<Document> result = DocumentScopePipeline.toEventScope(
                List.of(new Document("$match", new Document("lines", operand))));

        assertThat(matchOf(result, 0)).isEqualTo(new Document("fullDocument.lines", operand));
    }

    @Test
    void exprIsLeftAsWritten() {
        Document expr = new Document("$eq", List.of("$status", "OPEN"));
        List<Document> result = DocumentScopePipeline.toEventScope(
                List.of(new Document("$match", new Document("$expr", expr).append("status", "OPEN"))));

        Document match = matchOf(result, 0);
        assertThat(match.get("$expr")).isEqualTo(expr);
        assertThat(match).containsOnlyKeys("$expr", "fullDocument.status");
    }

    @Test
    void stopsAtTheFirstStageThatReshapesTheEvent() {
        List<Document> result = rewrite(newAggregation(
                match(where("status").is("OPEN")),
                project("operationType").and("fullDocument.status").as("state"),
                match(where("state").is("OPEN"))));

        assertThat(matchOf(result, 0)).containsOnlyKeys("fullDocument.status");
        assertThat(result.get(1)).containsOnlyKeys("$project");
        assertThat(matchOf(result, 2)).as("after $project the stage no longer reads a change event")
                .containsOnlyKeys("state");
    }

    @Test
    void doesNotMutateTheInput() {
        Document original = new Document("$match", new Document("status", "OPEN"));
        List<Document> input = List.of(original);

        DocumentScopePipeline.toEventScope(input);

        assertThat(original).isEqualTo(new Document("$match", new Document("status", "OPEN")));
    }
}
