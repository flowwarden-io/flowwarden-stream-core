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
package io.flowwarden.stream.spi;

/**
 * SPI for collecting metrics from FlowWarden Change Streams.
 *
 * <p>The Core ships with a {@linkplain #noOp() no-op} implementation.
 * The FlowWarden Reporter (or any custom provider) implements this
 * interface and registers it via
 * {@link io.flowwarden.stream.FlowWardenMetrics#setProvider}.</p>
 *
 * <p>All methods are fire-and-forget: implementations must not throw
 * exceptions or block the calling thread.</p>
 */
public interface StreamMetricsProvider {

    /**
     * Called when a Change Stream starts listening.
     *
     * @param streamName stream identifier
     * @param config     stream configuration snapshot
     */
    void onStreamStarted(String streamName, StreamConfiguration config);

    /**
     * Called when a Change Stream is stopped (via actuator, shutdown, or leadership loss).
     *
     * @param streamName stream identifier
     */
    default void onStreamStopped(String streamName) {}

    /**
     * Called when an event is received from MongoDB, before handler execution.
     *
     * @param streamName stream identifier
     * @param metadata   lightweight event metadata
     */
    void onEventReceived(String streamName, ChangeEventMetadata metadata);

    /**
     * Called after a handler finishes processing an event.
     *
     * @param streamName    stream identifier
     * @param durationNanos handler execution time in nanoseconds
     * @param success       {@code true} if the handler completed without error
     */
    void onEventProcessed(String streamName, long durationNanos, boolean success);

    /**
     * Called when a handler throws an exception.
     *
     * @param streamName    stream identifier
     * @param error         the exception thrown
     * @param willRetry     {@code true} if the event will be retried
     * @param attemptNumber current attempt number (1-based)
     * @param metadata      event metadata (operation type, document key, etc.)
     */
    void onEventError(String streamName, Throwable error, boolean willRetry,
                      int attemptNumber, ChangeEventMetadata metadata);

    /**
     * Called when a resume token checkpoint is persisted.
     *
     * @param streamName  stream identifier
     * @param resumeToken the persisted resume token
     */
    void onCheckpoint(String streamName, String resumeToken);

    /**
     * Called when the internal event buffer status changes.
     *
     * @param streamName  stream identifier
     * @param currentSize current number of events in the buffer
     * @param maxSize     maximum buffer capacity
     */
    void onBufferStatus(String streamName, int currentSize, int maxSize);

    /**
     * Called when a backpressure action is triggered.
     *
     * @param streamName stream identifier
     * @param action     the backpressure action taken
     */
    void onBackpressure(String streamName, BackpressureAction action);

    /**
     * Called when a failed event is sent to the Dead Letter Queue.
     *
     * @param streamName stream identifier
     */
    void onEventSentToDlq(String streamName);

    /**
     * Called with oplog window size information collected from the MongoDB replica set.
     *
     * <p>Stream-core collects this by reading the first and last timestamps from
     * {@code local.oplog.rs}. If the collection is inaccessible (e.g. Atlas shared tier),
     * {@code status} will be {@code "UNAVAILABLE"}.</p>
     *
     * @param logLengthHours oplog window duration in hours (0 if unavailable)
     * @param status         "OK", "UNAVAILABLE", or "ERROR"
     */
    void onOplogStats(double logLengthHours, String status);

    /**
     * Called when this instance's leadership role changes for a stream.
     *
     * @param streamName the stream name
     * @param role       "LEADER", "STANDBY", or "NOT_APPLICABLE"
     * @param instanceId the instance ID
     */
    default void onLeadershipChange(String streamName, String role, String instanceId) {}

    /**
     * Returns the shared no-op implementation that silently ignores all calls.
     */
    static StreamMetricsProvider noOp() {
        return NoOpStreamMetricsProvider.INSTANCE;
    }
}
