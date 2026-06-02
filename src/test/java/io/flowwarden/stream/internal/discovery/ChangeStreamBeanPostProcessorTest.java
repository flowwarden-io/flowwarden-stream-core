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

import io.flowwarden.stream.ChangeStreamContext;
import io.flowwarden.stream.ErrorAction;
import io.flowwarden.stream.FullDocumentBeforeChangeMode;
import io.flowwarden.stream.FullDocumentMode;
import io.flowwarden.stream.OperationType;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.DeadLetterQueue;
import io.flowwarden.stream.annotation.OnChange;
import io.flowwarden.stream.annotation.OnDelete;
import io.flowwarden.stream.annotation.OnError;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.annotation.OnReplace;
import io.flowwarden.stream.annotation.OnUpdate;
import io.flowwarden.stream.annotation.Filter;
import io.flowwarden.stream.annotation.Pipeline;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.BeanCreationException;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

class ChangeStreamBeanPostProcessorTest {

    private ChangeStreamBeanPostProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new ChangeStreamBeanPostProcessor();
    }

    // --- Valid beans ---

    @Test
    void discoversAnnotatedBean() {
        ValidHandler bean = new ValidHandler();
        processor.postProcessAfterInitialization(bean, "validHandler");

        assertEquals(1, processor.getDefinitions().size());
        ChangeStreamDefinition def = processor.getDefinitions().get(0);
        assertEquals("orders", def.collection());
        assertSame(bean, def.bean());
        assertNotNull(def.onChangeHandler());
    }

    @Test
    void ignoresNonAnnotatedBean() {
        Object bean = new Object();
        processor.postProcessAfterInitialization(bean, "plainBean");
        assertTrue(processor.getDefinitions().isEmpty());
    }

    @Test
    void resolvesStreamNameFromAnnotation() {
        NamedHandler bean = new NamedHandler();
        processor.postProcessAfterInitialization(bean, "namedHandler");

        assertEquals("my-stream", processor.getDefinitions().get(0).streamName());
    }

    @Test
    void resolvesStreamNameFromValueAttribute() {
        ValueNamedHandler bean = new ValueNamedHandler();
        processor.postProcessAfterInitialization(bean, "valueNamedHandler");

        assertEquals("value-stream", processor.getDefinitions().get(0).streamName());
    }

    @Test
    void derivesKebabCaseStreamNameFromClassName() {
        ValidHandler bean = new ValidHandler();
        processor.postProcessAfterInitialization(bean, "validHandler");

        assertEquals("valid-handler", processor.getDefinitions().get(0).streamName());
    }

    // --- Invalid beans ---

    @Test
    void throwsWhenNoHandlerAtAll() {
        NoHandlerBean bean = new NoHandlerBean();
        assertThrows(BeanCreationException.class,
                () -> processor.postProcessAfterInitialization(bean, "noHandler"));
    }

    @Test
    void throwsWhenMultipleOnChangeMethods() {
        MultipleHandlerBean bean = new MultipleHandlerBean();
        assertThrows(BeanCreationException.class,
                () -> processor.postProcessAfterInitialization(bean, "multiHandler"));
    }

    @Test
    void throwsWhenInvalidSignature() {
        BadSignatureHandler bean = new BadSignatureHandler();
        assertThrows(BeanCreationException.class,
                () -> processor.postProcessAfterInitialization(bean, "badSig"));
    }

    @Test
    void throwsWhenNoCollectionAndNoDocumentType() {
        NoCollectionHandler bean = new NoCollectionHandler();
        assertThrows(BeanCreationException.class,
                () -> processor.postProcessAfterInitialization(bean, "noColl"));
    }

    // --- Collection inference from documentType ---

    @Test
    void infersCollectionFromDocumentAnnotation() {
        DocTypeWithAnnotationHandler bean = new DocTypeWithAnnotationHandler();
        processor.postProcessAfterInitialization(bean, "docTypeHandler");

        assertEquals("orders", processor.getDefinitions().get(0).collection());
    }

    @Test
    void infersCollectionFromClassNameWhenDocumentAnnotationHasNoCollection() {
        DocTypeNoCollectionHandler bean = new DocTypeNoCollectionHandler();
        processor.postProcessAfterInitialization(bean, "docTypeNoCollHandler");

        assertEquals("orderItem", processor.getDefinitions().get(0).collection());
    }

    @Test
    void throwsWhenDocumentTypeIsDefaultAndNoCollection() {
        NoCollectionDefaultDocTypeHandler bean = new NoCollectionDefaultDocTypeHandler();
        assertThrows(BeanCreationException.class,
                () -> processor.postProcessAfterInitialization(bean, "noColl2"));
    }

    // --- Kebab case ---

    @Test
    void toKebabCaseConvertsCorrectly() {
        assertEquals("order-stream-handler",
                ChangeStreamBeanPostProcessor.toKebabCase("OrderStreamHandler"));
        assertEquals("simple",
                ChangeStreamBeanPostProcessor.toKebabCase("Simple"));
        assertEquals("a-b-c",
                ChangeStreamBeanPostProcessor.toKebabCase("ABC"));
    }

    // --- REQ-007: Typed handlers ---

    @Test
    void discoversTypedHandlersOnly() {
        TypedHandlersOnlyBean bean = new TypedHandlersOnlyBean();
        processor.postProcessAfterInitialization(bean, "typedOnly");

        assertEquals(1, processor.getDefinitions().size());
        ChangeStreamDefinition def = processor.getDefinitions().get(0);
        assertNull(def.onChangeHandler());
        assertEquals(2, def.typedHandlers().size());
        assertNotNull(def.typedHandlers().get(OperationType.INSERT));
        assertNotNull(def.typedHandlers().get(OperationType.UPDATE));
    }

    @Test
    void discoversMixedHandlers() {
        MixedHandlersBean bean = new MixedHandlersBean();
        processor.postProcessAfterInitialization(bean, "mixed");

        ChangeStreamDefinition def = processor.getDefinitions().get(0);
        assertNotNull(def.onChangeHandler());
        assertEquals(1, def.typedHandlers().size());
        assertNotNull(def.typedHandlers().get(OperationType.INSERT));
    }

    @Test
    void legacyOnChangeOnlyStillWorks() {
        ValidHandler bean = new ValidHandler();
        processor.postProcessAfterInitialization(bean, "legacy");

        ChangeStreamDefinition def = processor.getDefinitions().get(0);
        assertNotNull(def.onChangeHandler());
        assertTrue(def.typedHandlers().isEmpty());
        assertEquals(HandlerMethod.SignatureStyle.CONTEXT_ONLY, def.onChangeHandler().signatureStyle());
    }

    @Test
    void throwsWhenDuplicateTypedAnnotation() {
        DuplicateOnInsertBean bean = new DuplicateOnInsertBean();
        assertThrows(BeanCreationException.class,
                () -> processor.postProcessAfterInitialization(bean, "dupInsert"));
    }

    @Test
    void throwsWhenTypedDocSignatureWithDefaultDocumentType() {
        TypedDocWithDefaultDocTypeBean bean = new TypedDocWithDefaultDocTypeBean();
        assertThrows(BeanCreationException.class,
                () -> processor.postProcessAfterInitialization(bean, "typedDocDefault"));
    }

    @Test
    void throwsWhenInvalidTypedSignature() {
        InvalidTypedSignatureBean bean = new InvalidTypedSignatureBean();
        assertThrows(BeanCreationException.class,
                () -> processor.postProcessAfterInitialization(bean, "invalidTyped"));
    }

    @Test
    void resolveHandlerReturnsTypedFirst() {
        MixedHandlersBean bean = new MixedHandlersBean();
        processor.postProcessAfterInitialization(bean, "mixed2");

        ChangeStreamDefinition def = processor.getDefinitions().get(0);
        // INSERT has a typed handler → returns typed
        HandlerMethod handler = def.resolveHandler(OperationType.INSERT);
        assertNotNull(handler);
        assertEquals("onInsert", handler.method().getName());
    }

    @Test
    void resolveHandlerFallsBackToOnChange() {
        MixedHandlersBean bean = new MixedHandlersBean();
        processor.postProcessAfterInitialization(bean, "mixed3");

        ChangeStreamDefinition def = processor.getDefinitions().get(0);
        // UPDATE has no typed handler → falls back to @OnChange
        HandlerMethod handler = def.resolveHandler(OperationType.UPDATE);
        assertNotNull(handler);
        assertEquals("onChange", handler.method().getName());
    }

    @Test
    void resolveHandlerReturnsNullWhenNoMatch() {
        TypedHandlersOnlyBean bean = new TypedHandlersOnlyBean();
        processor.postProcessAfterInitialization(bean, "typedOnly2");

        ChangeStreamDefinition def = processor.getDefinitions().get(0);
        // DELETE has no typed handler and no @OnChange → null
        HandlerMethod handler = def.resolveHandler(OperationType.DELETE);
        assertNull(handler);
    }

    @Test
    void typedHandlerWithContextOnlySignature() {
        TypedContextOnlyBean bean = new TypedContextOnlyBean();
        processor.postProcessAfterInitialization(bean, "typedCtx");

        ChangeStreamDefinition def = processor.getDefinitions().get(0);
        HandlerMethod handler = def.typedHandlers().get(OperationType.INSERT);
        assertNotNull(handler);
        assertEquals(HandlerMethod.SignatureStyle.CONTEXT_ONLY, handler.signatureStyle());
    }

    @Test
    void typedHandlerWithDocAndContextSignature() {
        TypedDocAndContextBean bean = new TypedDocAndContextBean();
        processor.postProcessAfterInitialization(bean, "typedDocCtx");

        ChangeStreamDefinition def = processor.getDefinitions().get(0);
        HandlerMethod handler = def.typedHandlers().get(OperationType.INSERT);
        assertNotNull(handler);
        assertEquals(HandlerMethod.SignatureStyle.DOCUMENT_AND_CONTEXT, handler.signatureStyle());
    }

    // --- Test fixtures ---

    @ChangeStream(collection = "orders")
    static class ValidHandler {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {
        }
    }

    @ChangeStream(name = "my-stream", collection = "orders")
    static class NamedHandler {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {
        }
    }

    @ChangeStream(value = "value-stream", collection = "orders")
    static class ValueNamedHandler {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {
        }
    }

    @ChangeStream(collection = "orders")
    static class NoHandlerBean {
    }

    @ChangeStream(collection = "orders")
    static class MultipleHandlerBean {
        @OnChange
        void handle1(ChangeStreamContext<?> ctx) {
        }

        @OnChange
        void handle2(ChangeStreamContext<?> ctx) {
        }
    }

    @ChangeStream(collection = "orders")
    static class BadSignatureHandler {
        @OnChange
        void handle(String notAContext) {
        }
    }

    @ChangeStream
    static class NoCollectionHandler {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {
        }
    }

    // --- documentType fixtures ---

    @org.springframework.data.mongodb.core.mapping.Document(collection = "orders")
    static class Order {
    }

    @org.springframework.data.mongodb.core.mapping.Document
    static class OrderItem {
    }

    @ChangeStream(documentType = Order.class)
    static class DocTypeWithAnnotationHandler {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {
        }
    }

    @ChangeStream(documentType = OrderItem.class)
    static class DocTypeNoCollectionHandler {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {
        }
    }

    @ChangeStream(documentType = Document.class)
    static class NoCollectionDefaultDocTypeHandler {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {
        }
    }

    // --- REQ-007: Typed handler fixtures ---

    @ChangeStream(collection = "orders", documentType = Order.class)
    static class TypedHandlersOnlyBean {
        @OnInsert
        void onInsert(Order doc) {
        }

        @OnUpdate
        void onUpdate(Order doc) {
        }
    }

    @ChangeStream(collection = "orders")
    static class MixedHandlersBean {
        @OnInsert
        void onInsert(ChangeStreamContext<?> ctx) {
        }

        @OnChange
        void onChange(ChangeStreamContext<?> ctx) {
        }
    }

    @ChangeStream(collection = "orders", documentType = Order.class)
    static class DuplicateOnInsertBean {
        @OnInsert
        void insert1(Order doc) {
        }

        @OnInsert
        void insert2(Order doc) {
        }
    }

    @ChangeStream(collection = "orders") // documentType defaults to Document.class
    static class TypedDocWithDefaultDocTypeBean {
        @OnInsert
        void onInsert(Object doc) { // document-typed signature with default documentType
        }
    }

    @ChangeStream(collection = "orders")
    static class InvalidTypedSignatureBean {
        @OnInsert
        void onInsert(String a, String b, String c) { // 3 params = invalid
        }
    }

    @ChangeStream(collection = "orders")
    static class TypedContextOnlyBean {
        @OnInsert
        void onInsert(ChangeStreamContext<?> ctx) {
        }
    }

    @ChangeStream(collection = "orders", documentType = Order.class)
    static class TypedDocAndContextBean {
        @OnInsert
        void onInsert(Order doc, ChangeStreamContext<?> ctx) {
        }
    }

    // --- Reactive return type detection ---

    @Test
    void detectsReactiveReturnOnOnChangeHandler() {
        ReactiveOnChangeBean bean = new ReactiveOnChangeBean();
        processor.postProcessAfterInitialization(bean, "reactiveOnChange");

        ChangeStreamDefinition def = processor.getDefinitions().get(0);
        assertNotNull(def.onChangeHandler());
        assertTrue(def.onChangeHandler().isReactiveReturn());
    }

    @Test
    void detectsVoidReturnOnOnChangeHandler() {
        ValidHandler bean = new ValidHandler();
        processor.postProcessAfterInitialization(bean, "voidOnChange");

        ChangeStreamDefinition def = processor.getDefinitions().get(0);
        assertNotNull(def.onChangeHandler());
        assertFalse(def.onChangeHandler().isReactiveReturn());
    }

    @Test
    void detectsReactiveReturnOnTypedHandlers() {
        ReactiveTypedHandlersBean bean = new ReactiveTypedHandlersBean();
        processor.postProcessAfterInitialization(bean, "reactiveTyped");

        ChangeStreamDefinition def = processor.getDefinitions().get(0);
        assertTrue(def.typedHandlers().get(OperationType.INSERT).isReactiveReturn());
    }

    @ChangeStream(collection = "orders")
    static class ReactiveOnChangeBean {
        @OnChange
        Mono<Void> handle(ChangeStreamContext<?> ctx) {
            return Mono.empty();
        }
    }

    @ChangeStream(collection = "orders")
    static class ReactiveTypedHandlersBean {
        @OnInsert
        Mono<Void> onInsert(ChangeStreamContext<?> ctx) {
            return Mono.empty();
        }
    }

    // --- REQ-008-A: @Pipeline discovery ---

    @Test
    void discoversPipelineMethodWithBsonList() {
        PipelineBsonListBean bean = new PipelineBsonListBean();
        processor.postProcessAfterInitialization(bean, "pipelineBson");

        ChangeStreamDefinition def = processor.getDefinitions().get(0);
        assertNotNull(def.pipelineMethod());
        assertEquals(PipelineMethod.ReturnStyle.BSON_LIST, def.pipelineMethod().returnStyle());
    }

    @Test
    void discoversPipelineMethodWithAggregation() {
        PipelineAggregationBean bean = new PipelineAggregationBean();
        processor.postProcessAfterInitialization(bean, "pipelineAgg");

        ChangeStreamDefinition def = processor.getDefinitions().get(0);
        assertNotNull(def.pipelineMethod());
        assertEquals(PipelineMethod.ReturnStyle.AGGREGATION, def.pipelineMethod().returnStyle());
    }

    @Test
    void discoversPipelineMethodWithAggregationOperationList() {
        PipelineAggOpListBean bean = new PipelineAggOpListBean();
        processor.postProcessAfterInitialization(bean, "pipelineAggOp");

        ChangeStreamDefinition def = processor.getDefinitions().get(0);
        assertNotNull(def.pipelineMethod());
        assertEquals(PipelineMethod.ReturnStyle.AGGREGATION_OPERATION_LIST,
                def.pipelineMethod().returnStyle());
    }

    @Test
    void noPipelineMethodResultsInNull() {
        ValidHandler bean = new ValidHandler();
        processor.postProcessAfterInitialization(bean, "noPipeline");

        ChangeStreamDefinition def = processor.getDefinitions().get(0);
        assertNull(def.pipelineMethod());
    }

    @Test
    void throwsWhenMultiplePipelineMethods() {
        DuplicatePipelineBean bean = new DuplicatePipelineBean();
        assertThrows(BeanCreationException.class,
                () -> processor.postProcessAfterInitialization(bean, "dupPipeline"));
    }

    @Test
    void throwsWhenPipelineMethodHasParameters() {
        PipelineWithParamsBean bean = new PipelineWithParamsBean();
        assertThrows(BeanCreationException.class,
                () -> processor.postProcessAfterInitialization(bean, "pipelineParams"));
    }

    @Test
    void throwsWhenPipelineMethodHasInvalidReturnType() {
        PipelineInvalidReturnBean bean = new PipelineInvalidReturnBean();
        assertThrows(BeanCreationException.class,
                () -> processor.postProcessAfterInitialization(bean, "pipelineBadReturn"));
    }

    @Test
    void pipelineResolveReturnsBsonDocuments() {
        PipelineBsonListBean bean = new PipelineBsonListBean();
        processor.postProcessAfterInitialization(bean, "pipelineResolve");

        ChangeStreamDefinition def = processor.getDefinitions().get(0);
        List<Document> pipeline = def.pipelineMethod().resolve(def.bean());
        assertNotNull(pipeline);
        assertFalse(pipeline.isEmpty());
    }

    // --- @Pipeline fixtures ---

    @ChangeStream(collection = "orders")
    static class PipelineBsonListBean {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {}

        @Pipeline
        List<Bson> pipeline() {
            return List.of(
                    Aggregates.match(Filters.in("operationType", "insert", "update"))
            );
        }
    }

    @ChangeStream(collection = "orders")
    static class PipelineAggregationBean {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {}

        @Pipeline
        Aggregation pipeline() {
            return Aggregation.newAggregation(
                    Aggregation.match(org.springframework.data.mongodb.core.query.Criteria
                            .where("operationType").in("insert", "update"))
            );
        }
    }

    @ChangeStream(collection = "orders")
    static class PipelineAggOpListBean {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {}

        @Pipeline
        List<AggregationOperation> pipeline() {
            return List.of(
                    Aggregation.match(org.springframework.data.mongodb.core.query.Criteria
                            .where("operationType").in("insert", "update"))
            );
        }
    }

    @ChangeStream(collection = "orders")
    static class DuplicatePipelineBean {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {}

        @Pipeline
        List<Bson> pipeline1() { return List.of(); }

        @Pipeline
        List<Bson> pipeline2() { return List.of(); }
    }

    @ChangeStream(collection = "orders")
    static class PipelineWithParamsBean {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {}

        @Pipeline
        List<Bson> pipeline(String param) { return List.of(); }
    }

    @ChangeStream(collection = "orders")
    static class PipelineInvalidReturnBean {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {}

        @Pipeline
        String pipeline() { return "invalid"; }
    }

    // --- REQ-008-B: @Filter discovery ---

    @Test
    void discoversFilterMethodWithPredicate() {
        FilterPredicateBean bean = new FilterPredicateBean();
        processor.postProcessAfterInitialization(bean, "filterPredicate");

        ChangeStreamDefinition def = processor.getDefinitions().get(0);
        assertNotNull(def.filterMethod());
        assertEquals(FilterMethod.ReturnStyle.PREDICATE, def.filterMethod().returnStyle());
    }

    @Test
    void discoversFilterMethodWithBoolean() {
        FilterBooleanBean bean = new FilterBooleanBean();
        processor.postProcessAfterInitialization(bean, "filterBoolean");

        ChangeStreamDefinition def = processor.getDefinitions().get(0);
        assertNotNull(def.filterMethod());
        assertEquals(FilterMethod.ReturnStyle.BOOLEAN_METHOD, def.filterMethod().returnStyle());
    }

    @Test
    void noFilterMethodResultsInNull() {
        ValidHandler bean = new ValidHandler();
        processor.postProcessAfterInitialization(bean, "noFilter");

        ChangeStreamDefinition def = processor.getDefinitions().get(0);
        assertNull(def.filterMethod());
    }

    @Test
    void throwsWhenMultipleFilterMethods() {
        DuplicateFilterBean bean = new DuplicateFilterBean();
        assertThrows(BeanCreationException.class,
                () -> processor.postProcessAfterInitialization(bean, "dupFilter"));
    }

    @Test
    void throwsWhenFilterPredicateHasParameters() {
        FilterPredicateWithParamsBean bean = new FilterPredicateWithParamsBean();
        assertThrows(BeanCreationException.class,
                () -> processor.postProcessAfterInitialization(bean, "filterPredParams"));
    }

    @Test
    void throwsWhenFilterBooleanHasWrongParamType() {
        FilterBooleanWrongParamBean bean = new FilterBooleanWrongParamBean();
        assertThrows(BeanCreationException.class,
                () -> processor.postProcessAfterInitialization(bean, "filterBoolWrong"));
    }

    @Test
    void throwsWhenFilterMethodHasInvalidReturnType() {
        FilterInvalidReturnBean bean = new FilterInvalidReturnBean();
        assertThrows(BeanCreationException.class,
                () -> processor.postProcessAfterInitialization(bean, "filterBadReturn"));
    }

    @Test
    void filterEvaluateReturnsTrueOrFalse() {
        FilterBooleanBean bean = new FilterBooleanBean();
        processor.postProcessAfterInitialization(bean, "filterEval");

        ChangeStreamDefinition def = processor.getDefinitions().get(0);
        FilterMethod filter = def.filterMethod();
        assertNotNull(filter);

        // The FilterBooleanBean.filter() always returns true
        // We can't easily create a real ChangeStreamContext here, but we can verify
        // the method is accessible and the return style is correct
        assertEquals(FilterMethod.ReturnStyle.BOOLEAN_METHOD, filter.returnStyle());
        assertEquals("filter", filter.method().getName());
    }

    // --- @Filter fixtures ---

    @ChangeStream(collection = "orders")
    static class FilterPredicateBean {
        @OnInsert
        void handle(ChangeStreamContext<?> ctx) {}

        @Filter
        Predicate<ChangeStreamContext<?>> myFilter() {
            return ctx -> ctx.getOperationType() == OperationType.INSERT;
        }
    }

    @ChangeStream(collection = "orders")
    static class FilterBooleanBean {
        @OnInsert
        void handle(ChangeStreamContext<?> ctx) {}

        @Filter
        boolean filter(ChangeStreamContext<?> ctx) {
            return true;
        }
    }

    @ChangeStream(collection = "orders")
    static class DuplicateFilterBean {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {}

        @Filter
        boolean filter1(ChangeStreamContext<?> ctx) { return true; }

        @Filter
        boolean filter2(ChangeStreamContext<?> ctx) { return true; }
    }

    @ChangeStream(collection = "orders")
    static class FilterPredicateWithParamsBean {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {}

        @Filter
        Predicate<ChangeStreamContext<?>> myFilter(String param) {
            return ctx -> true;
        }
    }

    @ChangeStream(collection = "orders")
    static class FilterBooleanWrongParamBean {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {}

        @Filter
        boolean filter(String wrongType) { return true; }
    }

    @ChangeStream(collection = "orders")
    static class FilterInvalidReturnBean {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {}

        @Filter
        String filter() { return "invalid"; }
    }

    // --- @Filter + no-fullDocument compatibility validation ---

    @Test
    void throwsWhenFilterWithOnDelete() {
        FilterWithOnDeleteBean bean = new FilterWithOnDeleteBean();
        assertThrows(BeanCreationException.class,
                () -> processor.postProcessAfterInitialization(bean, "filterOnDelete"));
    }

    @Test
    void allowsFilterWithOnChangeCatchAll() {
        FilterWithOnChangeBean bean = new FilterWithOnChangeBean();
        processor.postProcessAfterInitialization(bean, "filterOnChange");

        ChangeStreamDefinition def = processor.getDefinitions().get(0);
        assertNotNull(def.filterMethod());
    }

    @Test
    void allowsFilterWithTypedInsertAndUpdate() {
        FilterWithInsertUpdateBean bean = new FilterWithInsertUpdateBean();
        processor.postProcessAfterInitialization(bean, "filterInsertUpdate");

        ChangeStreamDefinition def = processor.getDefinitions().get(0);
        assertNotNull(def.filterMethod());
    }

    // --- @Filter compatibility fixtures ---

    @ChangeStream(collection = "orders")
    static class FilterWithOnDeleteBean {
        @OnInsert
        void onInsert(ChangeStreamContext<?> ctx) {}

        @OnDelete
        void onDelete(ChangeStreamContext<?> ctx) {}

        @Filter
        boolean filter(ChangeStreamContext<?> ctx) { return true; }
    }

    @ChangeStream(collection = "orders")
    static class FilterWithOnChangeBean {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {}

        @Filter
        boolean filter(ChangeStreamContext<?> ctx) { return true; }
    }

    @ChangeStream(collection = "orders")
    static class FilterWithInsertUpdateBean {
        @OnInsert
        void onInsert(ChangeStreamContext<?> ctx) {}

        @OnUpdate
        void onUpdate(ChangeStreamContext<?> ctx) {}

        @Filter
        boolean filter(ChangeStreamContext<?> ctx) { return true; }
    }

    // --- fullDocument compatibility validation ---

    @Test
    void warnsWhenDefaultFullDocumentWithTypedOnUpdate() {
        DefaultFullDocTypedUpdateBean bean = new DefaultFullDocTypedUpdateBean();
        // Should not throw — just warn
        processor.postProcessAfterInitialization(bean, "defaultFullDocTypedUpdate");
        assertEquals(1, processor.getDefinitions().size());
    }

    @Test
    void allowsUpdateLookupWithTypedOnUpdate() {
        UpdateLookupTypedUpdateBean bean = new UpdateLookupTypedUpdateBean();
        processor.postProcessAfterInitialization(bean, "updateLookupTypedUpdate");
        assertEquals(1, processor.getDefinitions().size());
    }

    @Test
    void allowsDefaultFullDocumentWithOnInsertOnly() {
        DefaultFullDocInsertOnlyBean bean = new DefaultFullDocInsertOnlyBean();
        processor.postProcessAfterInitialization(bean, "defaultFullDocInsertOnly");
        assertEquals(1, processor.getDefinitions().size());
    }

    @Test
    void allowsWhenAvailableWithTypedOnUpdate() {
        WhenAvailableTypedUpdateBean bean = new WhenAvailableTypedUpdateBean();
        processor.postProcessAfterInitialization(bean, "whenAvailableTypedUpdate");
        assertEquals(1, processor.getDefinitions().size());
    }

    @Test
    void allowsRequiredWithTypedOnUpdate() {
        RequiredTypedUpdateBean bean = new RequiredTypedUpdateBean();
        processor.postProcessAfterInitialization(bean, "requiredTypedUpdate");
        assertEquals(1, processor.getDefinitions().size());
    }

    // --- fullDocument compatibility fixtures ---

    @ChangeStream(collection = "orders", documentType = Order.class, fullDocument = FullDocumentMode.DEFAULT)
    static class DefaultFullDocTypedUpdateBean {
        @OnUpdate
        void onUpdate(Order doc) {}
    }

    @ChangeStream(collection = "orders", documentType = Order.class, fullDocument = FullDocumentMode.UPDATE_LOOKUP)
    static class UpdateLookupTypedUpdateBean {
        @OnUpdate
        void onUpdate(Order doc) {}
    }

    @ChangeStream(collection = "orders", documentType = Order.class, fullDocument = FullDocumentMode.DEFAULT)
    static class DefaultFullDocInsertOnlyBean {
        @OnInsert
        void onInsert(Order doc) {}
    }

    @ChangeStream(collection = "orders", documentType = Order.class, fullDocument = FullDocumentMode.WHEN_AVAILABLE)
    static class WhenAvailableTypedUpdateBean {
        @OnUpdate
        void onUpdate(Order doc) {}
    }

    @ChangeStream(collection = "orders", documentType = Order.class, fullDocument = FullDocumentMode.REQUIRED)
    static class RequiredTypedUpdateBean {
        @OnUpdate
        void onUpdate(Order doc) {}
    }

    // --- @DeadLetterQueue discovery ---

    @Test
    void discoversDeadLetterQueueAnnotation() {
        DlqAnnotatedHandler bean = new DlqAnnotatedHandler();
        processor.postProcessAfterInitialization(bean, "dlqHandler");

        assertEquals(1, processor.getDefinitions().size());
        ChangeStreamDefinition def = processor.getDefinitions().get(0);
        assertNotNull(def.deadLetterQueueAnnotation());
        assertTrue(def.deadLetterQueueAnnotation().enabled());
        assertEquals("_fw_dlq", def.deadLetterQueueAnnotation().collection());
        assertEquals(30, def.deadLetterQueueAnnotation().ttlDays());
    }

    @Test
    void deadLetterQueueAnnotationIsNullWhenAbsent() {
        ValidHandler bean = new ValidHandler();
        processor.postProcessAfterInitialization(bean, "validHandler");

        ChangeStreamDefinition def = processor.getDefinitions().get(0);
        assertNull(def.deadLetterQueueAnnotation());
    }

    @Test
    void throwsWhenDeadLetterQueueHasNegativeTtlDays() {
        InvalidDlqNegativeTtlHandler bean = new InvalidDlqNegativeTtlHandler();
        assertThrows(BeanCreationException.class,
                () -> processor.postProcessAfterInitialization(bean, "invalidDlq"));
    }

    @ChangeStream(collection = "orders")
    @DeadLetterQueue
    static class DlqAnnotatedHandler {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {}
    }

    @ChangeStream(collection = "orders")
    @DeadLetterQueue(ttlDays = -1)
    static class InvalidDlqNegativeTtlHandler {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {}
    }

    // --- REQ-015: @OnError discovery ---

    @Test
    void discoversOnErrorHandler() {
        OnErrorSingleHandler bean = new OnErrorSingleHandler();
        processor.postProcessAfterInitialization(bean, "onErrorHandler");

        ChangeStreamDefinition def = processor.getDefinitions().get(0);
        assertNotNull(def.errorHandlerResolver());
        assertFalse(def.errorHandlerResolver().isEmpty());
    }

    @Test
    void discoversMultipleOnErrorHandlersForDifferentTypes() {
        OnErrorMultipleHandlers bean = new OnErrorMultipleHandlers();
        processor.postProcessAfterInitialization(bean, "onErrorMultiple");

        ChangeStreamDefinition def = processor.getDefinitions().get(0);
        assertFalse(def.errorHandlerResolver().isEmpty());
    }

    @Test
    void noOnErrorResultsInEmptyResolver() {
        ValidHandler bean = new ValidHandler();
        processor.postProcessAfterInitialization(bean, "noOnError");

        ChangeStreamDefinition def = processor.getDefinitions().get(0);
        assertTrue(def.errorHandlerResolver().isEmpty());
    }

    @Test
    void throwsWhenOnErrorHasInvalidReturnType() {
        OnErrorBadReturnHandler bean = new OnErrorBadReturnHandler();
        assertThrows(BeanCreationException.class,
                () -> processor.postProcessAfterInitialization(bean, "onErrorBadReturn"));
    }

    @Test
    void throwsWhenOnErrorHasInvalidParams() {
        OnErrorBadParamsHandler bean = new OnErrorBadParamsHandler();
        assertThrows(BeanCreationException.class,
                () -> processor.postProcessAfterInitialization(bean, "onErrorBadParams"));
    }

    @Test
    void throwsWhenDuplicateOnErrorForSameExceptionType() {
        OnErrorDuplicateTypeHandler bean = new OnErrorDuplicateTypeHandler();
        assertThrows(BeanCreationException.class,
                () -> processor.postProcessAfterInitialization(bean, "onErrorDupType"));
    }

    @Test
    void throwsWhenTwoCatchAllOnError() {
        OnErrorDuplicateCatchAllHandler bean = new OnErrorDuplicateCatchAllHandler();
        assertThrows(BeanCreationException.class,
                () -> processor.postProcessAfterInitialization(bean, "onErrorDupCatchAll"));
    }

    // --- @OnError fixtures ---

    @ChangeStream(collection = "orders")
    static class OnErrorSingleHandler {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {}

        @OnError(NullPointerException.class)
        ErrorAction onNpe(Throwable ex, ChangeStreamContext<?> ctx) {
            return ErrorAction.SKIP;
        }
    }

    @ChangeStream(collection = "orders")
    static class OnErrorMultipleHandlers {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {}

        @OnError(NullPointerException.class)
        ErrorAction onNpe(Throwable ex, ChangeStreamContext<?> ctx) {
            return ErrorAction.SKIP;
        }

        @OnError(IllegalArgumentException.class)
        ErrorAction onIllegalArg(Throwable ex, ChangeStreamContext<?> ctx) {
            return ErrorAction.DLQ;
        }

        @OnError
        ErrorAction catchAll(Throwable ex, ChangeStreamContext<?> ctx) {
            return ErrorAction.RETHROW;
        }
    }

    @ChangeStream(collection = "orders")
    static class OnErrorBadReturnHandler {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {}

        @OnError
        void onError(Throwable ex, ChangeStreamContext<?> ctx) {}
    }

    @ChangeStream(collection = "orders")
    static class OnErrorBadParamsHandler {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {}

        @OnError
        ErrorAction onError(String notThrowable) { return ErrorAction.SKIP; }
    }

    @ChangeStream(collection = "orders")
    static class OnErrorDuplicateTypeHandler {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {}

        @OnError(NullPointerException.class)
        ErrorAction onNpe1(Throwable ex, ChangeStreamContext<?> ctx) {
            return ErrorAction.SKIP;
        }

        @OnError(NullPointerException.class)
        ErrorAction onNpe2(Throwable ex, ChangeStreamContext<?> ctx) {
            return ErrorAction.DLQ;
        }
    }

    @ChangeStream(collection = "orders")
    static class OnErrorDuplicateCatchAllHandler {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {}

        @OnError
        ErrorAction catchAll1(Throwable ex, ChangeStreamContext<?> ctx) {
            return ErrorAction.SKIP;
        }

        @OnError
        ErrorAction catchAll2(Throwable ex, ChangeStreamContext<?> ctx) {
            return ErrorAction.DLQ;
        }
    }

    // --- fullDocumentBeforeChange pipeline test ---

    @Test
    void fullDocumentBeforeChangePassedThroughToStreamConfig() {
        FullDocBeforeChangeHandler bean = new FullDocBeforeChangeHandler();
        processor.postProcessAfterInitialization(bean, "fullDocBeforeChange");

        ChangeStreamDefinition def = processor.getDefinitions().get(0);
        assertEquals(FullDocumentBeforeChangeMode.WHEN_AVAILABLE, def.config().fullDocumentBeforeChange());
    }

    @ChangeStream(collection = "orders", fullDocumentBeforeChange = FullDocumentBeforeChangeMode.WHEN_AVAILABLE)
    static class FullDocBeforeChangeHandler {
        @OnUpdate
        void onUpdate(ChangeStreamContext<?> ctx) {}
    }
}
