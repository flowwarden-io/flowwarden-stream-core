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
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

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
    private final Supplier<?> pipelineSupplier;
    private final PipelineScope scope;

    /** A {@code @Pipeline} method whose stages are sent as written ({@link PipelineScope#EVENT}). */
    public PipelineMethod(Method method, ReturnStyle returnStyle) {
        this(method, returnStyle, PipelineScope.EVENT);
    }

    public PipelineMethod(Method method, ReturnStyle returnStyle, PipelineScope scope) {
        this.method = Objects.requireNonNull(method, "method must not be null");
        this.returnStyle = Objects.requireNonNull(returnStyle, "returnStyle must not be null");
        this.pipelineSupplier = null;
        this.scope = Objects.requireNonNull(scope, "scope must not be null");
    }

    private PipelineMethod(Supplier<?> pipelineSupplier, ReturnStyle returnStyle, PipelineScope scope) {
        this.method = null;
        this.returnStyle = returnStyle;
        this.pipelineSupplier = Objects.requireNonNull(pipelineSupplier, "pipelineSupplier must not be null");
        this.scope = Objects.requireNonNull(scope, "scope must not be null");
    }

    /** Creates a {@link PipelineMethod} backed by a functional supplier of raw BSON stages (no reflection). */
    public static PipelineMethod fromSupplier(Supplier<List<Bson>> pipelineSupplier) {
        return new PipelineMethod(pipelineSupplier, ReturnStyle.BSON_LIST, PipelineScope.EVENT);
    }

    /**
     * Creates a {@link PipelineMethod} backed by a functional supplier of raw BSON stages with
     * an explicit scope. {@link PipelineScope#DOCUMENT} is rejected by the definition validator
     * for this style — the factory itself accepts it so the validator can name the stream.
     */
    public static PipelineMethod fromSupplier(Supplier<List<Bson>> pipelineSupplier, PipelineScope scope) {
        return new PipelineMethod(pipelineSupplier, ReturnStyle.BSON_LIST, scope);
    }

    /** Creates a {@link PipelineMethod} backed by a functional supplier of a Spring Data aggregation. */
    public static PipelineMethod fromAggregationSupplier(Supplier<Aggregation> aggregationSupplier,
                                                         PipelineScope scope) {
        return new PipelineMethod(aggregationSupplier, ReturnStyle.AGGREGATION, scope);
    }

    public Method method() {
        return method;
    }

    public ReturnStyle returnStyle() {
        return returnStyle;
    }

    /** What the field names of the pipeline are written against. Never null. */
    public PipelineScope scope() {
        return scope;
    }

    /**
     * Invokes the pipeline method (or functional supplier) and returns the stages
     * exactly as they are sent to MongoDB, for the main cursor and for the heartbeat
     * probe alike.
     *
     * <p>The stages apply to the <strong>change event</strong>. FlowWarden hands them
     * to Spring Data as plain documents, which Spring Data forwards untouched: the
     * {@code fullDocument.} prefixing that Spring Data's change stream API applies to
     * an {@code Aggregation} filter does <em>not</em> happen there. With
     * {@link PipelineScope#DOCUMENT} the equivalent rewrite is done here, once, by
     * {@link DocumentScopePipeline}; with {@link PipelineScope#EVENT} the stages are
     * returned as written.</p>
     *
     * @param bean the target bean instance (ignored for a functional supplier)
     * @return the aggregation pipeline as a list of BSON documents, never null
     * @throws IllegalStateException if the pipeline method/supplier fails or returns null
     */
    public List<Document> resolve(Object bean) {
        List<Document> stages = toDocuments(invoke(bean));
        // A raw BSON pipeline is never rewritten, whatever the scope says: the
        // validator rejects that combination before a stream can start.
        if (scope == PipelineScope.DOCUMENT && returnStyle != ReturnStyle.BSON_LIST) {
            return DocumentScopePipeline.toEventScope(stages);
        }
        return stages;
    }

    private Object invoke(Object bean) {
        if (pipelineSupplier != null) {
            Object result;
            try {
                result = pipelineSupplier.get();
            } catch (RuntimeException e) {
                throw new IllegalStateException("Pipeline supplier threw an exception", e);
            }
            if (result == null) {
                throw new IllegalStateException("Pipeline supplier returned null");
            }
            return result;
        }
        try {
            Object result = method.invoke(bean);
            if (result == null) {
                throw new IllegalStateException(
                        "@Pipeline method " + method.getDeclaringClass().getSimpleName()
                                + "#" + method.getName() + " returned null");
            }
            return result;
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

    @SuppressWarnings("unchecked")
    private List<Document> toDocuments(Object result) {
        return switch (returnStyle) {
            case BSON_LIST -> toBsonDocumentList((List<Bson>) result);
            case AGGREGATION_OPERATION_LIST -> toDocumentListFromOperations((List<AggregationOperation>) result);
            case AGGREGATION -> toDocumentListFromAggregation((Aggregation) result);
        };
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
        return aggregation.toPipeline(Aggregation.DEFAULT_CONTEXT);
    }

    @Override
    public String toString() {
        if (pipelineSupplier != null) {
            return scope == PipelineScope.EVENT ? "functional(pipeline)" : "functional(pipeline, scope=" + scope + ")";
        }
        return method.getDeclaringClass().getSimpleName() + "#" + method.getName()
                + "(" + returnStyle + (scope == PipelineScope.EVENT ? "" : ", scope=" + scope) + ")";
    }
}
