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
import org.springframework.data.mongodb.core.messaging.Message;
import org.springframework.data.mongodb.core.messaging.MessageListener;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
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
        FlowWardenMessageListenerWrapper wrapper =
                new FlowWardenMessageListenerWrapper(delegate, "test-stream");

        assertThatThrownBy(() -> wrapper.onMessage(fakeMessage()))
                .isSameAs(cause);

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
        FlowWardenMessageListenerWrapper wrapper =
                new FlowWardenMessageListenerWrapper(delegate, "test-stream");

        assertThatThrownBy(() -> wrapper.onMessage(fakeMessage()))
                .isSameAs(cause);

        verify(metrics).onStreamStopped("test-stream", StopReason.CRASHED, cause);
    }

    @Test
    void onMessage_successfulDelegate_doesNotEmitStreamStopped() {
        StreamMetricsProvider metrics = mock(StreamMetricsProvider.class);
        FlowWardenMetrics.setProvider(metrics);

        MessageListener<ChangeStreamDocument<Document>, Document> delegate = msg -> {
            // no-op, succeeds
        };
        FlowWardenMessageListenerWrapper wrapper =
                new FlowWardenMessageListenerWrapper(delegate, "test-stream");

        wrapper.onMessage(fakeMessage());

        verify(metrics, never()).onStreamStopped(any(), any(), any());
    }

    @Test
    void constructor_rejectsNullDelegate() {
        assertThatThrownBy(() ->
                new FlowWardenMessageListenerWrapper(null, "test-stream"))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void constructor_rejectsNullStreamName() {
        MessageListener<ChangeStreamDocument<Document>, Document> delegate = msg -> {};
        assertThatThrownBy(() ->
                new FlowWardenMessageListenerWrapper(delegate, null))
                .isInstanceOf(NullPointerException.class);
    }

    @SuppressWarnings("unchecked")
    private static Message<ChangeStreamDocument<Document>, Document> fakeMessage() {
        return mock(Message.class);
    }
}
