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
import org.bson.conversions.Bson;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.TypeBasedAggregationOperationContext;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Encapsulates a {@code @Pipeline}-annotated method and its invocation strategy.
 *
 * <p>The method is invoked once at stream startup to produce the aggregation pipeline
 * stages that MongoDB applies server-side.</p>
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public final class PipelineMethod {

    /**
     * Describes the return type of the {@code @Pipeline} method.
     */
    public enum ReturnStyle {
        /** {@code List<Bson>} — native BSON pipeline */
        BSON_LIST,
        /** {@code List<AggregationOperation>} — Spring Data operations */
        AGGREGATION_OPERATION_LIST,
        /** {@code Aggregation} — Spring Data aggregation object */
        AGGREGATION
    }

    private final Method method;
    private final ReturnStyle returnStyle;

    public PipelineMethod(Method method, ReturnStyle returnStyle) {
        this.method = Objects.requireNonNull(method, "method must not be null");
        this.returnStyle = Objects.requireNonNull(returnStyle, "returnStyle must not be null");
    }

    public Method method() {
        return method;
    }

    public ReturnStyle returnStyle() {
        return returnStyle;
    }

    /**
     * Invokes the pipeline method on the given bean and returns the pipeline
     * as a {@code List<Document>} suitable for Spring Data MongoDB's
     * {@code ChangeStreamRequest.filter()} and {@code ChangeStreamOptions.filter()}.
     *
     * @param bean the target bean instance
     * @return the aggregation pipeline as a list of BSON documents, never null
     * @throws IllegalStateException if the pipeline method fails or returns null
     */
    @SuppressWarnings("unchecked")
    public List<Document> resolve(Object bean) {
        try {
            Object result = method.invoke(bean);
            if (result == null) {
                throw new IllegalStateException(
                        "@Pipeline method " + method.getDeclaringClass().getSimpleName()
                                + "#" + method.getName() + " returned null");
            }

            return switch (returnStyle) {
                case BSON_LIST -> toBsonDocumentList((List<Bson>) result);
                case AGGREGATION_OPERATION_LIST -> toDocumentListFromOperations((List<AggregationOperation>) result);
                case AGGREGATION -> toDocumentListFromAggregation((Aggregation) result);
            };
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new IllegalStateException(
                    "@Pipeline method " + method.getDeclaringClass().getSimpleName()
                            + "#" + method.getName() + " threw an exception", cause);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(
                    "Cannot invoke @Pipeline method " + method.getDeclaringClass().getSimpleName()
                            + "#" + method.getName(), e);
        }
    }

    private static List<Document> toBsonDocumentList(List<Bson> bsonList) {
        List<Document> result = new ArrayList<>(bsonList.size());
        for (Bson bson : bsonList) {
            if (bson instanceof Document doc) {
                result.add(doc);
            } else {
                result.add(Document.parse(bson.toBsonDocument().toJson()));
            }
        }
        return result;
    }

    private static List<Document> toDocumentListFromOperations(List<AggregationOperation> operations) {
        Aggregation aggregation = Aggregation.newAggregation(operations);
        return toDocumentListFromAggregation(aggregation);
    }

    private static List<Document> toDocumentListFromAggregation(Aggregation aggregation) {
        AggregationOperationContext context = Aggregation.DEFAULT_CONTEXT;
        return aggregation.toPipeline(context);
    }

    @Override
    public String toString() {
        return method.getDeclaringClass().getSimpleName() + "#" + method.getName()
                + "(" + returnStyle + ")";
    }
}
