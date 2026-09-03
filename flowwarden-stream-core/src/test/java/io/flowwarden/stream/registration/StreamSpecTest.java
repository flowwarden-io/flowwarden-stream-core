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
package io.flowwarden.stream.registration;

import io.flowwarden.stream.DeploymentMode;
import io.flowwarden.stream.FullDocumentMode;
import io.flowwarden.stream.OperationType;
import io.flowwarden.stream.core.DocumentHandler;
import io.flowwarden.stream.core.ReactiveContextHandler;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamSpecTest {

    @Test
    void appliesDefaultsMatchingChangeStreamAnnotation() {
        StreamSpec<Order> spec = StreamSpec.builder("orders-stream", Order.class).build();

        assertEquals("orders-stream", spec.name());
        assertEquals(Order.class, spec.documentType());
        assertEquals("", spec.collection());
        assertEquals("", spec.database());
        assertTrue(spec.enabled());
        assertTrue(spec.autoStart());
        assertEquals(FullDocumentMode.DEFAULT, spec.fullDocument());
        assertEquals(DeploymentMode.ALL_INSTANCES, spec.deploymentMode());
        assertTrue(spec.checkpoint().isEmpty());
        assertTrue(spec.typedHandlers().isEmpty());
        assertTrue(spec.onChangeHandler().isEmpty());
    }

    @Test
    void checkpointDefaultsMatchCheckpointAnnotation() {
        CheckpointSpec defaults = CheckpointSpec.defaults();
        assertEquals(1, defaults.saveEveryN());
        assertEquals(5, defaults.saveIntervalSeconds());
        assertEquals(300, defaults.idleHeartbeatIntervalSeconds());
    }

    @Test
    void retryPolicyDefaultsMatchRetryPolicyAnnotation() {
        RetryPolicySpec defaults = RetryPolicySpec.defaults();
        assertEquals(3, defaults.maxAttempts());
        assertEquals("500ms", defaults.initialDelay());
        assertEquals("30s", defaults.maxDelay());
        assertEquals(2.0, defaults.multiplier());
        assertTrue(defaults.jitter());
        assertEquals(3, defaults.noRetryOn().size());
    }

    @Test
    void deadLetterQueueDefaultsMatchDeadLetterQueueAnnotation() {
        DeadLetterQueueSpec defaults = DeadLetterQueueSpec.defaults();
        assertTrue(defaults.enabled());
        assertEquals(30, defaults.retentionDays());
        assertTrue(defaults.includeOriginalDocument());
        assertTrue(defaults.includeStackTrace());
    }

    @Test
    void retryPolicySpecDefensivelyCopiesInputLists() {
        java.util.List<Class<? extends Throwable>> mutableRetryOn = new java.util.ArrayList<>();
        mutableRetryOn.add(IllegalStateException.class);

        RetryPolicySpec spec = RetryPolicySpec.builder().retryOn(mutableRetryOn).build();
        mutableRetryOn.add(RuntimeException.class);

        assertEquals(1, spec.retryOn().size());
        assertThrows(UnsupportedOperationException.class, () -> spec.retryOn().add(RuntimeException.class));
    }

    @Test
    void retryPolicySpecRejectsNullDelay() {
        assertThrows(NullPointerException.class, () -> RetryPolicySpec.builder().initialDelay(null));
    }

    @Test
    void checkpointSpecRejectsNullStartPosition() {
        assertThrows(NullPointerException.class, () -> CheckpointSpec.builder().startPosition(null));
    }

    @Test
    void mongoDlqOptionsSpecRejectsNullCollection() {
        assertThrows(NullPointerException.class, () -> MongoDlqOptionsSpec.builder().collection(null));
    }

    @Test
    void onInsertWithDocumentHandlerIsStoredAsTypedHandler() {
        StreamSpec<Order> spec = StreamSpec.builder("orders-stream", Order.class)
                .onInsert((order, ctx) -> { })
                .build();

        assertTrue(spec.handler(OperationType.INSERT).isPresent());
        assertTrue(spec.handler(OperationType.INSERT).get() instanceof TypedHandler.Document<?>);
    }

    @Test
    void onInsertWithContextHandlerIsStoredAsTypedHandler() {
        StreamSpec<Order> spec = StreamSpec.builder("orders-stream", Order.class)
                .onInsert(ctx -> { })
                .build();

        assertTrue(spec.handler(OperationType.INSERT).get() instanceof TypedHandler.Context<?>);
    }

    @Test
    void throwsWhenOnInsertDocumentHandlerIsNull() {
        StreamSpec.Builder<Order> builder = StreamSpec.builder("orders-stream", Order.class);
        assertThrows(NullPointerException.class, () -> builder.onInsert((DocumentHandler) null));
    }

    @Test
    void throwsWhenOnInsertReactiveContextHandlerIsNull() {
        StreamSpec.Builder<Order> builder = StreamSpec.builder("orders-stream", Order.class);
        assertThrows(NullPointerException.class, () -> builder.onInsertReactive((ReactiveContextHandler) null));
    }

    @Test
    void throwsWhenSameOperationRegisteredTwice() {
        StreamSpec.Builder<Order> builder = StreamSpec.builder("orders-stream", Order.class)
                .onInsert(ctx -> { });

        assertThrows(IllegalStateException.class, () -> builder.onInsert(ctx -> { }));
    }

    @Test
    void throwsWhenOnChangeRegisteredTwice() {
        StreamSpec.Builder<Order> builder = StreamSpec.builder("orders-stream", Order.class)
                .onChange(ctx -> { });

        assertThrows(IllegalStateException.class, () -> builder.onChange(ctx -> { }));
    }

    @Test
    void throwsWhenNameIsBlank() {
        assertThrows(IllegalArgumentException.class, () -> StreamSpec.builder("  ", Order.class));
    }

    @Test
    void buildDoesNotEagerlyValidateBusinessRules() {
        // No handler at all: build() itself must not throw — that check is deferred to the
        // shared validator (StreamContributorProcessor), so it produces the same error class
        // as the equivalent annotation misconfiguration.
        StreamSpec<Order> spec = StreamSpec.builder("orders-stream", Order.class).build();
        assertFalse(spec.onChangeHandler().isPresent());
    }

    private static class Order {
    }
}
