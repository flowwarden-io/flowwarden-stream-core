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
import io.flowwarden.stream.OnHistoryLost;
import io.flowwarden.stream.OperationType;
import io.flowwarden.stream.StartPosition;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.annotation.DeadLetterQueue;
import io.flowwarden.stream.annotation.RetryPolicy;
import io.flowwarden.stream.registration.CheckpointSpec;
import io.flowwarden.stream.registration.DeadLetterQueueSpec;
import io.flowwarden.stream.registration.RetryPolicySpec;
import io.flowwarden.stream.registration.StreamSpec;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.convert.MongoConverter;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StreamSpecConverterTest {

    @org.springframework.data.mongodb.core.mapping.Document(collection = "orders")
    static class Order {
    }

    @Test
    void convertsDocumentHandlerAndInvokesIt() throws Exception {
        AtomicReference<Object> receivedDoc = new AtomicReference<>();
        AtomicReference<ChangeStreamContext<?>> receivedCtx = new AtomicReference<>();

        StreamSpec<Order> spec = StreamSpec.builder("order-stream", Order.class)
                .onInsert((order, ctx) -> {
                    receivedDoc.set(order);
                    receivedCtx.set(ctx);
                })
                .build();

        ChangeStreamDefinition definition = StreamSpecConverter.convert(spec, new Object(), "testBean");

        assertEquals("order-stream", definition.streamName());
        assertEquals("orders", definition.collection()); // inferred from @Document
        HandlerMethod handler = definition.typedHandlers().get(OperationType.INSERT);
        assertNotNull(handler);

        ChangeStreamContext<?> ctx = mock(ChangeStreamContext.class);
        MongoConverter converter = mock(MongoConverter.class);
        Order converted = new Order();
        when(converter.read(eq(Order.class), any(Document.class))).thenReturn(converted);

        handler.invoke(definition.bean(), ctx, new Document("_id", 1), converter, Order.class);

        assertSame(converted, receivedDoc.get());
        assertSame(ctx, receivedCtx.get());
    }

    @Test
    void convertsExplicitCollectionOverInference() {
        StreamSpec<Order> spec = StreamSpec.builder("order-stream", Order.class)
                .collection("custom_orders")
                .onChange(ctx -> { })
                .build();

        ChangeStreamDefinition definition = StreamSpecConverter.convert(spec, new Object(), "testBean");

        assertEquals("custom_orders", definition.collection());
    }

    @Test
    void synthesizesCheckpointAnnotationFromSpec() {
        StreamSpec<Order> spec = StreamSpec.builder("order-stream", Order.class)
                .collection("orders")
                .checkpoint(CheckpointSpec.builder()
                        .saveEveryN(2)
                        .saveIntervalSeconds(10)
                        .idleHeartbeatIntervalSeconds(60)
                        .startPosition(StartPosition.LATEST)
                        .onHistoryLost(OnHistoryLost.RESUME_FROM_NOW)
                        .build())
                .onChange(ctx -> { })
                .build();

        ChangeStreamDefinition definition = StreamSpecConverter.convert(spec, new Object(), "testBean");
        Checkpoint checkpoint = definition.checkpointAnnotation();

        assertNotNull(checkpoint);
        assertEquals(2, checkpoint.saveEveryN());
        assertEquals(10, checkpoint.saveIntervalSeconds());
        assertEquals(60, checkpoint.idleHeartbeatIntervalSeconds());
        assertEquals(StartPosition.LATEST, checkpoint.startPosition());
        assertEquals(OnHistoryLost.RESUME_FROM_NOW, checkpoint.onHistoryLost());
    }

    @Test
    void synthesizesRetryPolicyAndDeadLetterQueueAnnotationsFromSpec() {
        StreamSpec<Order> spec = StreamSpec.builder("order-stream", Order.class)
                .collection("orders")
                .retryPolicy(RetryPolicySpec.builder()
                        .maxAttempts(5)
                        .initialDelay("1s")
                        .maxDelay("1m")
                        .multiplier(1.5)
                        .noRetryOn(java.util.List.of(IllegalStateException.class))
                        .jitter(false)
                        .build())
                .deadLetterQueue(DeadLetterQueueSpec.builder()
                        .retentionDays(7)
                        .includeOriginalDocument(false)
                        .includeStackTrace(false)
                        .build())
                .onChange(ctx -> { })
                .build();

        ChangeStreamDefinition definition = StreamSpecConverter.convert(spec, new Object(), "testBean");

        RetryPolicy retryPolicy = definition.retryPolicyAnnotation();
        assertEquals(5, retryPolicy.maxAttempts());
        assertEquals("1s", retryPolicy.initialDelay());
        assertEquals(1.5, retryPolicy.multiplier());
        assertEquals(1, retryPolicy.noRetryOn().length);

        DeadLetterQueue dlq = definition.deadLetterQueueAnnotation();
        assertEquals(7, dlq.retentionDays());
        assertEquals(false, dlq.includeOriginalDocument());
    }

    @Test
    void throwsWhenDocumentTypeIsRawDocumentAndNoCollectionGiven() {
        // Parity with the annotation path (ChangeStreamBeanPostProcessor#resolveCollection):
        // Document.class with no explicit collection must fail fast, not silently resolve
        // to a "document" collection.
        StreamSpec<Document> spec = StreamSpec.builder("raw-stream", Document.class)
                .onChange(ctx -> { })
                .build();

        org.springframework.beans.factory.BeanCreationException ex = org.junit.jupiter.api.Assertions.assertThrows(
                org.springframework.beans.factory.BeanCreationException.class,
                () -> StreamSpecConverter.convert(spec, new Object(), "testBean"));
        org.junit.jupiter.api.Assertions.assertTrue(
                ex.getMessage().contains("must specify a collection or a documentType with @Document"));
    }

    @Test
    void leavesUnspecifiedAnnotationsNull() {
        StreamSpec<Order> spec = StreamSpec.builder("order-stream", Order.class)
                .collection("orders")
                .onChange(ctx -> { })
                .build();

        ChangeStreamDefinition definition = StreamSpecConverter.convert(spec, new Object(), "testBean");

        assertNull(definition.checkpointAnnotation());
        assertNull(definition.retryPolicyAnnotation());
        assertNull(definition.deadLetterQueueAnnotation());
        assertNull(definition.mongoDlqOptionsAnnotation());
    }
}
