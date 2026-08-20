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

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.flowwarden.stream.FlowWardenMetrics;
import io.flowwarden.stream.spi.StopReason;
import io.flowwarden.stream.spi.StreamMetricsProvider;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.springframework.data.mongodb.core.messaging.Message;
import org.springframework.data.mongodb.core.messaging.MessageListener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class FlowWardenMessageListenerWrapperTest {

    @AfterEach
    void tearDown() {
        FlowWardenMetrics.setProvider(StreamMetricsProvider.noOp());
    }

    @Test
    void onMessage_runtimeExceptionFromDelegate_emitsCrashedAndRethrows() {
        StreamMetricsProvider metrics = mock(StreamMetricsProvider.class);
        FlowWardenMetrics.setProvider(metrics);

        RuntimeException cause = new RuntimeException("simulated handler crash");
        MessageListener<ChangeStreamDocument<Document>, Document> delegate = msg -> {
            throw cause;
        };
        Runnable onCrash = mock(Runnable.class);
        FlowWardenMessageListenerWrapper wrapper =
                new FlowWardenMessageListenerWrapper(delegate, "test-stream", onCrash);

        // The rethrow is wrapped in the provenance marker: the container's
        // ErrorHandler tells listener crashes (fail-stop) apart from cursor
        // deaths (managed restart) by this type.
        assertThatThrownBy(() -> wrapper.onMessage(fakeMessage()))
                .isInstanceOf(FlowWardenMessageListenerWrapper.ListenerCrashedException.class)
                .hasCause(cause);

        verify(metrics).onStreamStopped("test-stream", StopReason.CRASHED, cause);
    }

    @Test
    void onMessage_errorFromDelegate_isAlsoCaughtAndPropagated() {
        StreamMetricsProvider metrics = mock(StreamMetricsProvider.class);
        FlowWardenMetrics.setProvider(metrics);

        OutOfMemoryError cause = new OutOfMemoryError("simulated OOM");
        MessageListener<ChangeStreamDocument<Document>, Document> delegate = msg -> {
            throw cause;
        };
        Runnable onCrash = mock(Runnable.class);
        FlowWardenMessageListenerWrapper wrapper =
                new FlowWardenMessageListenerWrapper(delegate, "test-stream", onCrash);

        // Even an Error travels inside the (RuntimeException) marker: it now
        // reaches the ErrorHandler's fail-stop path instead of silently
        // killing the executor thread.
        assertThatThrownBy(() -> wrapper.onMessage(fakeMessage()))
                .isInstanceOf(FlowWardenMessageListenerWrapper.ListenerCrashedException.class)
                .hasCause(cause);

        verify(metrics).onStreamStopped("test-stream", StopReason.CRASHED, cause);
    }

    @Test
    void onMessage_crash_invokesOnCrashBeforeEmittingStreamStopped() {
        StreamMetricsProvider metrics = mock(StreamMetricsProvider.class);
        FlowWardenMetrics.setProvider(metrics);

        RuntimeException cause = new RuntimeException("boom");
        MessageListener<ChangeStreamDocument<Document>, Document> delegate = msg -> {
            throw cause;
        };
        Runnable onCrash = mock(Runnable.class);
        FlowWardenMessageListenerWrapper wrapper =
                new FlowWardenMessageListenerWrapper(delegate, "test-stream", onCrash);

        assertThatThrownBy(() -> wrapper.onMessage(fakeMessage()))
                .isInstanceOf(FlowWardenMessageListenerWrapper.ListenerCrashedException.class)
                .hasCause(cause);

        // Cleanup first: onCrash publishes the lock-free termination signal;
        // a slow metrics provider must not delay it.
        InOrder order = inOrder(onCrash, metrics);
        order.verify(onCrash).run();
        order.verify(metrics).onStreamStopped("test-stream", StopReason.CRASHED, cause);
    }

    @Test
    void onMessage_successfulDelegate_doesNotEmitStreamStoppedNorInvokeOnCrash() {
        StreamMetricsProvider metrics = mock(StreamMetricsProvider.class);
        FlowWardenMetrics.setProvider(metrics);

        MessageListener<ChangeStreamDocument<Document>, Document> delegate = msg -> {
            // no-op, succeeds
        };
        Runnable onCrash = mock(Runnable.class);
        FlowWardenMessageListenerWrapper wrapper =
                new FlowWardenMessageListenerWrapper(delegate, "test-stream", onCrash);

        wrapper.onMessage(fakeMessage());

        verify(metrics, never()).onStreamStopped(any(), any(), any());
        verify(onCrash, never()).run();
    }

    @Test
    void onMessage_onCrashThrows_originalCauseStillPropagatesWithSuppressed() {
        FlowWardenMetrics.setProvider(mock(StreamMetricsProvider.class));

        RuntimeException originalCause = new RuntimeException("delegate boom");
        RuntimeException cleanupError = new RuntimeException("cleanup boom");
        MessageListener<ChangeStreamDocument<Document>, Document> delegate = msg -> {
            throw originalCause;
        };
        Runnable onCrash = () -> {
            throw cleanupError;
        };
        FlowWardenMessageListenerWrapper wrapper =
                new FlowWardenMessageListenerWrapper(delegate, "test-stream", onCrash);

        assertThatThrownBy(() -> wrapper.onMessage(fakeMessage()))
                .isInstanceOf(FlowWardenMessageListenerWrapper.ListenerCrashedException.class)
                .hasCause(originalCause);

        assertThat(originalCause.getSuppressed()).containsExactly(cleanupError);
    }

    @Test
    void onMessage_throwingMetricsProvider_markerAndCleanupStillGuaranteed() {
        // Round 3: an unmarked listener crash would be misclassified as a
        // cursor death (evict + restart over a still-active reader). The
        // provider emission is best-effort — marker and cleanup always win.
        StreamMetricsProvider metrics = mock(StreamMetricsProvider.class);
        org.mockito.Mockito.doThrow(new RuntimeException("provider boom"))
                .when(metrics).onStreamStopped(any(), any(), any());
        FlowWardenMetrics.setProvider(metrics);

        RuntimeException cause = new RuntimeException("delegate boom");
        MessageListener<ChangeStreamDocument<Document>, Document> delegate = msg -> {
            throw cause;
        };
        Runnable onCrash = mock(Runnable.class);
        FlowWardenMessageListenerWrapper wrapper =
                new FlowWardenMessageListenerWrapper(delegate, "test-stream", onCrash);

        assertThatThrownBy(() -> wrapper.onMessage(fakeMessage()))
                .isInstanceOf(FlowWardenMessageListenerWrapper.ListenerCrashedException.class)
                .hasCause(cause);

        verify(onCrash).run();
        assertThat(cause.getSuppressed())
                .anyMatch(s -> "provider boom".equals(s.getMessage()));
    }

    @Test
    void constructor_rejectsNullDelegate() {
        assertThatThrownBy(() ->
                new FlowWardenMessageListenerWrapper(null, "test-stream", () -> {}))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void constructor_rejectsNullStreamName() {
        MessageListener<ChangeStreamDocument<Document>, Document> delegate = msg -> {};
        assertThatThrownBy(() ->
                new FlowWardenMessageListenerWrapper(delegate, null, () -> {}))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void constructor_rejectsNullOnCrash() {
        MessageListener<ChangeStreamDocument<Document>, Document> delegate = msg -> {};
        assertThatThrownBy(() ->
                new FlowWardenMessageListenerWrapper(delegate, "test-stream", null))
                .isInstanceOf(NullPointerException.class);
    }

    @SuppressWarnings("unchecked")
    private static Message<ChangeStreamDocument<Document>, Document> fakeMessage() {
        return mock(Message.class);
    }
}
