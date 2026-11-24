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

import io.flowwarden.stream.PipelineScope;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;

import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PipelineMethodTest {

    @Test
    void fromSupplierResolvesToDocumentList() {
        PipelineMethod pm = PipelineMethod.fromSupplier(
                () -> List.of(new Document("$match", new Document("operationType", "insert"))));

        List<Document> resolved = pm.resolve(new Object());

        assertEquals(1, resolved.size());
    }

    @Test
    void fromSupplierThrowsWhenSupplierReturnsNull() {
        PipelineMethod pm = PipelineMethod.fromSupplier(() -> null);

        assertThrows(IllegalStateException.class, () -> pm.resolve(new Object()));
    }

    @Test
    void fromSupplierWrapsSupplierExceptionInIllegalStateException() {
        // Regression: the raw supplier call used to sit outside the try block, so a
        // RuntimeException from the supplier escaped uncontextualized instead of matching
        // the documented IllegalStateException contract (and the reflective @Pipeline path's
        // behavior of wrapping the cause with a clear message).
        RuntimeException cause = new IllegalArgumentException("bad pipeline config");
        PipelineMethod pm = PipelineMethod.fromSupplier(() -> {
            throw cause;
        });

        IllegalStateException thrown = assertThrows(IllegalStateException.class, () -> pm.resolve(new Object()));
        assertSame(cause, thrown.getCause());
        assertInstanceOf(String.class, thrown.getMessage());
    }

    // --- scope ---

    static class Pipelines {
        Aggregation aggregation() {
            return Aggregation.newAggregation(Aggregation.match(
                    org.springframework.data.mongodb.core.query.Criteria.where("status").is("OPEN")));
        }

        List<AggregationOperation> operations() {
            return List.of(Aggregation.match(
                    org.springframework.data.mongodb.core.query.Criteria.where("status").is("OPEN")));
        }

        List<Bson> bson() {
            return List.of(new Document("$match", new Document("status", "OPEN")));
        }
    }

    private static List<Document> resolve(String methodName, PipelineMethod.ReturnStyle style, PipelineScope scope)
            throws Exception {
        Method method = Pipelines.class.getDeclaredMethod(methodName);
        return new PipelineMethod(method, style, scope).resolve(new Pipelines());
    }

    private static final Document AS_WRITTEN = new Document("$match", new Document("status", "OPEN"));
    private static final Document PREFIXED = new Document("$match", new Document("fullDocument.status", "OPEN"));

    @Test
    void eventScopeIsTheDefaultAndSendsAnAggregationAsWritten() throws Exception {
        Method method = Pipelines.class.getDeclaredMethod("aggregation");
        PipelineMethod pm = new PipelineMethod(method, PipelineMethod.ReturnStyle.AGGREGATION);

        assertEquals(PipelineScope.EVENT, pm.scope());
        assertEquals(List.of(AS_WRITTEN), pm.resolve(new Pipelines()));
        assertEquals(List.of(AS_WRITTEN),
                resolve("operations", PipelineMethod.ReturnStyle.AGGREGATION_OPERATION_LIST, PipelineScope.EVENT));
    }

    @Test
    void documentScopePrefixesAnAggregation() throws Exception {
        assertEquals(List.of(PREFIXED),
                resolve("aggregation", PipelineMethod.ReturnStyle.AGGREGATION, PipelineScope.DOCUMENT));
    }

    @Test
    void documentScopePrefixesAListOfAggregationOperations() throws Exception {
        assertEquals(List.of(PREFIXED),
                resolve("operations", PipelineMethod.ReturnStyle.AGGREGATION_OPERATION_LIST, PipelineScope.DOCUMENT));
    }

    @Test
    void rawBsonIsNeverRewrittenWhateverTheScope() throws Exception {
        // The validator rejects this combination at startup; the resolution itself must still not touch raw BSON.
        assertEquals(List.of(AS_WRITTEN),
                resolve("bson", PipelineMethod.ReturnStyle.BSON_LIST, PipelineScope.DOCUMENT));
        assertEquals(List.of(AS_WRITTEN),
                PipelineMethod.fromSupplier(() -> List.of(AS_WRITTEN), PipelineScope.DOCUMENT).resolve(new Object()));
    }

    @Test
    void aggregationSupplierHonoursTheScope() {
        Pipelines pipelines = new Pipelines();

        assertEquals(List.of(PREFIXED),
                PipelineMethod.fromAggregationSupplier(pipelines::aggregation, PipelineScope.DOCUMENT)
                        .resolve(new Object()));
        assertEquals(List.of(AS_WRITTEN),
                PipelineMethod.fromAggregationSupplier(pipelines::aggregation, PipelineScope.EVENT)
                        .resolve(new Object()));
    }

    @Test
    void aggregationSupplierReturningNullFailsLikeTheBsonOne() {
        PipelineMethod pm = PipelineMethod.fromAggregationSupplier(() -> null, PipelineScope.DOCUMENT);

        assertThrows(IllegalStateException.class, () -> pm.resolve(new Object()));
    }
}
