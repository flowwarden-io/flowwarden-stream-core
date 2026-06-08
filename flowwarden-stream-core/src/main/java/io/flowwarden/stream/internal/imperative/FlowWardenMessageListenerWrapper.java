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
import org.bson.Document;
import org.springframework.data.mongodb.core.messaging.Message;
import org.springframework.data.mongodb.core.messaging.MessageListener;

import java.util.Objects;

/**
 * Wraps a {@link MessageListener} so any {@link Throwable} escaping the
 * delegate is reported via {@link FlowWardenMetrics#get()
 * onStreamStopped(streamName, CRASHED, cause)} before propagating to Spring.
 *
 * <p>Spring's {@code MessageListenerContainer} does not expose an error
 * callback that stream-core can hook. Without this wrapper, an uncaught
 * exception from the listener silently kills the container's worker thread
 * and the console keeps reporting the stream as {@code RUNNING} forever.</p>
 */
final class FlowWardenMessageListenerWrapper
        implements MessageListener<ChangeStreamDocument<Document>, Document> {

    private final MessageListener<ChangeStreamDocument<Document>, Document> delegate;
    private final String streamName;

    FlowWardenMessageListenerWrapper(
            MessageListener<ChangeStreamDocument<Document>, Document> delegate,
            String streamName) {
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
        this.streamName = Objects.requireNonNull(streamName, "streamName must not be null");
    }

    @Override
    public void onMessage(Message<ChangeStreamDocument<Document>, Document> message) {
        try {
            delegate.onMessage(message);
        } catch (Throwable t) {
            FlowWardenMetrics.get().onStreamStopped(streamName, StopReason.CRASHED, t);
            throw t;
        }
    }
}
