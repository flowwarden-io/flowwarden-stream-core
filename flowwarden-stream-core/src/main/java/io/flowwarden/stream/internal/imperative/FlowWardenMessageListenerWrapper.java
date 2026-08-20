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
 * onStreamStopped(streamName, CRASHED, cause)} and the supplied {@code onCrash}
 * cleanup callback fires before the throwable propagates to Spring.
 *
 * <p>Spring's {@code MessageListenerContainer} does not expose an error
 * callback that stream-core can hook. Without this wrapper, an uncaught
 * exception from the listener silently kills the container's worker thread
 * and the console keeps reporting the stream as {@code RUNNING} forever.</p>
 *
 * <p>The {@code onCrash} callback is the manager's hook to evict the stream's
 * entries from its internal state maps (e.g. {@code streams},
 * {@code lastActivityTimes}, {@code eventCounters}) so the public
 * {@code isRunning} / {@code getLastEventTime} APIs stop lying once the
 * container thread is dead. Any exception thrown by the cleanup callback is
 * suppressed onto the original throwable &mdash; cleanup is best-effort and
 * must never replace the cause Spring will see.</p>
 *
 * <p>The rethrown throwable is wrapped in {@link ListenerCrashedException}:
 * the container routes it to the registered {@code ErrorHandler}, which must
 * tell listener-level crashes (fail-stop: cancel the subscription, no
 * restart) apart from cursor deaths (managed restart) — the marker is that
 * provenance.</p>
 */
final class FlowWardenMessageListenerWrapper
        implements MessageListener<ChangeStreamDocument<Document>, Document> {

    /**
     * Provenance marker for the manager's {@code ErrorHandler}: the wrapped
     * cause escaped the <em>listener</em>, the cursor itself is fine.
     */
    static final class ListenerCrashedException extends RuntimeException {
        ListenerCrashedException(Throwable cause) {
            super(cause);
        }
    }

    private final MessageListener<ChangeStreamDocument<Document>, Document> delegate;
    private final String streamName;
    private final Runnable onCrash;

    FlowWardenMessageListenerWrapper(
            MessageListener<ChangeStreamDocument<Document>, Document> delegate,
            String streamName,
            Runnable onCrash) {
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
        this.streamName = Objects.requireNonNull(streamName, "streamName must not be null");
        this.onCrash = Objects.requireNonNull(onCrash, "onCrash must not be null");
    }

    @Override
    public void onMessage(Message<ChangeStreamDocument<Document>, Document> message) {
        try {
            delegate.onMessage(message);
        } catch (Throwable t) {
            // Cleanup FIRST: onCrash publishes the lock-free termination
            // signal — a slow (or throwing) metrics provider must not delay
            // it, and neither call may replace the cause, skip the other, or
            // prevent the provenance marker from reaching the ErrorHandler
            // (an unmarked listener crash would be misclassified as a cursor
            // death and restarted over a still-active reader).
            try {
                onCrash.run();
            } catch (RuntimeException cleanupError) {
                t.addSuppressed(cleanupError);
            }
            try {
                FlowWardenMetrics.get().onStreamStopped(streamName, StopReason.CRASHED, t);
            } catch (RuntimeException metricsError) {
                t.addSuppressed(metricsError);
            }
            throw new ListenerCrashedException(t);
        }
    }
}
