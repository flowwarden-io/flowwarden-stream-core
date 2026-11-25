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
import io.flowwarden.stream.OperationType;
import io.flowwarden.stream.PipelineScope;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.Filter;
import io.flowwarden.stream.annotation.OnChange;
import io.flowwarden.stream.annotation.OnError;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.annotation.OnUpdate;
import io.flowwarden.stream.annotation.Pipeline;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.query.Criteria;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Annotated methods declared in a superclass or an interface of a {@code @ChangeStream}
 * class are discovered like the ones declared by the class itself.
 */
class ChangeStreamBeanPostProcessorInheritanceTest {

    private ChangeStreamBeanPostProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new ChangeStreamBeanPostProcessor();
    }

    private ChangeStreamDefinition discover(Object bean) {
        processor.postProcessAfterInitialization(bean, "bean");
        assertEquals(1, processor.getDefinitions().size());
        return processor.getDefinitions().get(0);
    }

    // --- Inherited methods are discovered ---

    @Test
    void discoversHandlerDeclaredInGenericSuperclass() throws Exception {
        OrdersStream bean = new OrdersStream();
        ChangeStreamDefinition def = discover(bean);

        HandlerMethod handler = def.typedHandlers().get(OperationType.INSERT);
        assertNotNull(handler);
        assertEquals(HandlerMethod.SignatureStyle.DOCUMENT_ONLY, handler.signatureStyle());

        Order order = new Order();
        handler.method().invoke(bean, order);
        assertEquals(List.of(order), bean.received);
    }

    @Test
    void discoversPipelineDeclaredInSuperclass() {
        ChangeStreamDefinition def = discover(new OrdersStream());

        assertNotNull(def.pipelineMethod());
        assertEquals("pipeline", def.pipelineMethod().method().getName());
    }

    @Test
    void discoversFilterAndErrorHandlerDeclaredInSuperclass() {
        ChangeStreamDefinition def = discover(new OrdersStream());

        assertNotNull(def.filterMethod());
        assertFalse(def.errorHandlerResolver().isEmpty());
    }

    @Test
    void discoversOnChangeDeclaredInSuperclass() {
        ChangeStreamDefinition def = discover(new InheritedOnChangeStream());

        assertNotNull(def.onChangeHandler());
    }

    @Test
    void discoversPrivateHandlerDeclaredInSuperclass() throws Exception {
        PrivateHandlerStream bean = new PrivateHandlerStream();
        ChangeStreamDefinition def = discover(bean);

        def.typedHandlers().get(OperationType.INSERT).method().invoke(bean, new Order());
        assertEquals(1, bean.calls);
    }

    @Test
    void discoversDefaultMethodOfInterface() throws Exception {
        DefaultMethodStream bean = new DefaultMethodStream();
        ChangeStreamDefinition def = discover(bean);

        def.typedHandlers().get(OperationType.INSERT).method().invoke(bean, new Order());
        assertEquals(1, bean.calls.size());
    }

    @Test
    void discoversMethodsThroughSeveralLevels() {
        ChangeStreamDefinition def = discover(new GrandChildStream());

        assertNotNull(def.typedHandlers().get(OperationType.INSERT));
        assertNotNull(def.typedHandlers().get(OperationType.UPDATE));
        assertNotNull(def.pipelineMethod());
    }

    // --- Annotation on an interface, implementation inherited from a superclass ---

    @Test
    void discoversInterfacePipelineImplementedBySuperclass() {
        ChangeStreamDefinition def = discover(new ContractPipelineStream());

        assertNotNull(def.pipelineMethod());
        assertEquals(ContractBase.class, def.pipelineMethod().method().getDeclaringClass());
        assertEquals(PipelineScope.DOCUMENT, def.pipelineMethod().scope());
    }

    @Test
    void discoversInterfaceHandlerImplementedBySuperclass() throws Exception {
        ContractPipelineStream bean = new ContractPipelineStream();
        ChangeStreamDefinition def = discover(bean);

        HandlerMethod handler = def.typedHandlers().get(OperationType.INSERT);
        assertNotNull(handler);
        handler.method().invoke(bean, new Order());
        assertEquals(1, bean.inserts);
    }

    @Test
    void discoversInterfaceErrorHandlerImplementedBySuperclass() {
        ChangeStreamDefinition def = discover(new ContractPipelineStream());

        assertTrue(def.errorHandlerResolver().resolve(new IllegalStateException("boom")).isPresent());
        assertTrue(def.errorHandlerResolver().resolve(new RuntimeException("other")).isEmpty());
    }

    @Test
    void annotationOnImplementationWinsOverInterfaceOne() {
        ChangeStreamDefinition def = discover(new ContractScopeOverrideStream());

        assertEquals(PipelineScope.EVENT, def.pipelineMethod().scope());
    }

    // --- Overrides ---

    @Test
    void overrideWithoutAnnotationReplacesInheritedHandler() throws Exception {
        OverridingStream bean = new OverridingStream();
        ChangeStreamDefinition def = discover(bean);

        HandlerMethod handler = def.typedHandlers().get(OperationType.INSERT);
        assertEquals(OverridingStream.class, handler.method().getDeclaringClass());

        handler.method().invoke(bean, new Order());
        assertTrue(bean.received.isEmpty(), "the superclass body must not run");
        assertEquals(1, bean.overridden);
    }

    @Test
    void annotatedOverrideOfGenericMethodCountsOnce() {
        ChangeStreamDefinition def = discover(new AnnotatedOverridingStream());

        HandlerMethod handler = def.typedHandlers().get(OperationType.INSERT);
        assertEquals(AnnotatedOverridingStream.class, handler.method().getDeclaringClass());
        assertFalse(handler.method().isBridge());
    }

    @Test
    void overriddenPipelineReplacesInheritedOne() {
        ChangeStreamDefinition def = discover(new OverriddenPipelineStream());

        assertEquals(OverriddenPipelineStream.class, def.pipelineMethod().method().getDeclaringClass());
    }

    // --- Cardinality and signature rules apply to the whole hierarchy ---

    @Test
    void rejectsTwoDistinctHandlersAcrossHierarchy() {
        BeanCreationException ex = assertThrows(BeanCreationException.class,
                () -> processor.postProcessAfterInitialization(new SecondInsertHandlerStream(), "bean"));

        assertTrue(ex.getMessage().contains("must have at most one @OnInsert method, found 2"), ex.getMessage());
        assertTrue(ex.getMessage().contains(SecondInsertHandlerStream.class.getName() + "#alsoOnInsert"),
                ex.getMessage());
        assertTrue(ex.getMessage().contains(BaseStream.class.getName() + "#onInsert"), ex.getMessage());
    }

    @Test
    void rejectsTwoDistinctPipelinesAcrossHierarchy() {
        BeanCreationException ex = assertThrows(BeanCreationException.class,
                () -> processor.postProcessAfterInitialization(new SecondPipelineStream(), "bean"));

        assertTrue(ex.getMessage().contains("must have at most one @Pipeline method, found 2"), ex.getMessage());
    }

    @Test
    void rejectsInheritedGenericHandlerBoundToAnotherType() {
        BeanCreationException ex = assertThrows(BeanCreationException.class,
                () -> processor.postProcessAfterInitialization(new MismatchedStream(), "bean"));

        assertTrue(ex.getMessage().contains(Customer.class.getName()), ex.getMessage());
        assertTrue(ex.getMessage().contains(Order.class.getName()), ex.getMessage());
    }

    @Test
    void classWithoutAnnotatedSuperclassIsUnaffected() {
        ChangeStreamDefinition def = discover(new PlainStream());

        assertNotNull(def.typedHandlers().get(OperationType.INSERT));
        assertNull(def.pipelineMethod());
        assertNull(def.filterMethod());
    }

    // --- Fixtures ---

    static class Order {
    }

    static class Customer {
    }

    abstract static class BaseStream<T> {

        final List<T> received = new ArrayList<>();

        @Pipeline
        Aggregation pipeline() {
            return Aggregation.newAggregation(Aggregation.match(Criteria.where("fullDocument.status").is("OPEN")));
        }

        @Filter
        boolean accept(ChangeStreamContext<?> ctx) {
            return true;
        }

        @OnInsert
        void onInsert(T document) {
            received.add(document);
        }

        @OnError(IllegalStateException.class)
        ErrorAction onError(Throwable ex, ChangeStreamContext<?> ctx) {
            return ErrorAction.SKIP;
        }
    }

    @ChangeStream(name = "orders-stream", collection = "orders", documentType = Order.class)
    static class OrdersStream extends BaseStream<Order> {
    }

    @ChangeStream(name = "overriding-stream", collection = "orders", documentType = Order.class)
    static class OverridingStream extends BaseStream<Order> {

        int overridden;

        @Override
        void onInsert(Order document) {
            overridden++;
        }
    }

    @ChangeStream(name = "annotated-overriding-stream", collection = "orders", documentType = Order.class)
    static class AnnotatedOverridingStream extends BaseStream<Order> {

        @Override
        @OnInsert
        void onInsert(Order document) {
        }
    }

    @ChangeStream(name = "overridden-pipeline-stream", collection = "orders", documentType = Order.class)
    static class OverriddenPipelineStream extends BaseStream<Order> {

        @Override
        Aggregation pipeline() {
            return Aggregation.newAggregation(Aggregation.match(Criteria.where("fullDocument.status").is("PAID")));
        }
    }

    @ChangeStream(name = "second-insert-stream", collection = "orders", documentType = Order.class)
    static class SecondInsertHandlerStream extends BaseStream<Order> {

        @OnInsert
        void alsoOnInsert(Order document) {
        }
    }

    @ChangeStream(name = "second-pipeline-stream", collection = "orders", documentType = Order.class)
    static class SecondPipelineStream extends BaseStream<Order> {

        @Pipeline
        Aggregation otherPipeline() {
            return Aggregation.newAggregation(Aggregation.match(Criteria.where("fullDocument.status").is("PAID")));
        }
    }

    @ChangeStream(name = "mismatched-stream", collection = "orders", documentType = Order.class)
    static class MismatchedStream extends BaseStream<Customer> {
    }

    abstract static class MiddleStream extends BaseStream<Order> {

        @OnUpdate
        void onUpdate(ChangeStreamContext<Order> ctx) {
        }
    }

    @ChangeStream(name = "grand-child-stream", collection = "orders", documentType = Order.class)
    static class GrandChildStream extends MiddleStream {
    }

    abstract static class BaseOnChange {

        @OnChange
        void onChange(ChangeStreamContext<?> ctx) {
        }
    }

    @ChangeStream(name = "inherited-on-change-stream", collection = "orders")
    static class InheritedOnChangeStream extends BaseOnChange {
    }

    abstract static class BasePrivateHandler {

        int calls;

        @OnInsert
        private void onInsert(Order document) {
            calls++;
        }
    }

    @ChangeStream(name = "private-handler-stream", collection = "orders", documentType = Order.class)
    static class PrivateHandlerStream extends BasePrivateHandler {
    }

    interface InsertRecorder {

        List<Order> calls();

        @OnInsert
        default void onInsert(Order document) {
            calls().add(document);
        }
    }

    @ChangeStream(name = "default-method-stream", collection = "orders", documentType = Order.class)
    static class DefaultMethodStream implements InsertRecorder {

        final List<Order> calls = new ArrayList<>();

        @Override
        public List<Order> calls() {
            return calls;
        }
    }

    interface Contract {

        @Pipeline(scope = PipelineScope.DOCUMENT)
        Aggregation pipeline();

        @OnInsert
        void onInsert(Order document);

        @OnError(IllegalStateException.class)
        ErrorAction onError(Throwable ex, ChangeStreamContext<?> ctx);
    }

    /** Implements the methods of {@link Contract} without knowing the interface. */
    static class ContractBase {

        int inserts;

        public Aggregation pipeline() {
            return Aggregation.newAggregation(Aggregation.match(Criteria.where("status").is("OPEN")));
        }

        public void onInsert(Order document) {
            inserts++;
        }

        public ErrorAction onError(Throwable ex, ChangeStreamContext<?> ctx) {
            return ErrorAction.SKIP;
        }
    }

    @ChangeStream(name = "contract-stream", collection = "orders", documentType = Order.class)
    static class ContractPipelineStream extends ContractBase implements Contract {
    }

    @ChangeStream(name = "contract-scope-override-stream", collection = "orders", documentType = Order.class)
    static class ContractScopeOverrideStream extends ContractBase implements Contract {

        @Override
        @Pipeline(scope = PipelineScope.EVENT)
        public Aggregation pipeline() {
            return Aggregation.newAggregation(Aggregation.match(Criteria.where("fullDocument.status").is("OPEN")));
        }
    }

    @ChangeStream(name = "plain-stream", collection = "orders", documentType = Order.class)
    static class PlainStream {

        @OnInsert
        void onInsert(Order document) {
        }
    }
}
