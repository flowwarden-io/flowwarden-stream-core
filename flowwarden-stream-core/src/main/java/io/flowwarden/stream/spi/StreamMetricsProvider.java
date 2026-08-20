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
     * Called when a Change Stream stops, for any reason.
     *
     * <p>Emitted in three situations:</p>
     * <ul>
     *   <li>{@link StopReason#GRACEFUL} &mdash; explicit {@code stopStream()},
     *       JVM shutdown, leadership loss. {@code cause} is {@code null}.</li>
     *   <li>{@link StopReason#CRASHED} &mdash; uncaught {@link Throwable} killed
     *       the imperative listener container thread, or the reactive pipeline
     *       terminated on an unexpected {@code onError} / {@code onComplete}
     *       signal. {@code cause} carries the escaping throwable
     *       (or {@code null} if it could not be captured).</li>
     * </ul>
     *
     * <p>The {@code CRASHED} signal exists so monitoring can distinguish a
     * silently dead stream from an intentional stop, instead of reporting
     * {@code RUNNING} indefinitely.</p>
     *
     * @param streamName stream identifier
     * @param reason     why the stream stopped
     * @param cause      non-null {@code Throwable} when {@code reason == CRASHED}
     *                   and capturable; {@code null} otherwise
     */
    default void onStreamStopped(String streamName, StopReason reason, Throwable cause) {}

    /**
     * Called when a stream that died at runtime (cursor death — network
     * outage, primary stepdown, non-resumable server error) has been
     * successfully resubscribed by the managed restart loop.
     *
     * <p>Always follows an {@code onStreamStopped(streamName, CRASHED, cause)}
     * for the same incident, and an {@link #onStreamStarted} fires for the
     * new subscription as usual — this signal adds the restart-specific
     * context: how many attempts the recovery took and what killed the
     * previous subscription. A recurring stream of these is an operational
     * signal (flaky network, undersized oplog) even though each individual
     * incident healed itself.</p>
     *
     * @param streamName stream identifier
     * @param attempt    the attempt number that succeeded (1 = first try)
     * @param cause      the error that killed the previous subscription, or
     *                   {@code null} if it could not be captured
     */
    default void onStreamRestarted(String streamName, int attempt, Throwable cause) {}

    /**
     * Called when the watched collection was invalidated (dropped, its
     * database dropped, or renamed) — MongoDB closed the underlying change
     * stream cursor. Always followed by an
     * {@code onStreamStopped(streamName, CRASHED, cause)} for the cursor
     * death itself.
     *
     * <p>What happens next depends on the cause and the stream's
     * {@code onHistoryLost} strategy: a drop under a self-repairing strategy
     * heals automatically (fresh certified position, managed resubscription —
     * an {@link #onStreamRestarted} follows); a rename, or any invalidation
     * under {@code FAIL}, stops the stream terminally (the declared
     * collection identity is gone, or the stream refuses to skip history) —
     * an operator action is required.</p>
     *
     * @param streamName stream identifier
     * @param cause      the operation that invalidated the stream:
     *                   {@code DROP}, {@code DROP_DATABASE} or {@code RENAME}
     *                   ({@code DROP} when the pre-invalidate event was not
     *                   observed)
     */
    default void onStreamInvalidated(String streamName,
                                     io.flowwarden.stream.OperationType cause) {}

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
     * Called after a resume token checkpoint has been successfully persisted
     * by the configured {@link CheckpointStore}.
     *
     * <p>Emitted only on confirmed write success. If the store throws, this
     * callback is NOT invoked &mdash; see {@link #onCheckpointFailed} instead.</p>
     *
     * @param streamName  stream identifier
     * @param resumeToken the persisted resume token
     */
    void onCheckpoint(String streamName, String resumeToken);

    /**
     * Called when a {@link CheckpointStore} write throws while persisting a
     * resume token (e.g. {@code MongoWriteConcernException} on {@code wtimeout},
     * Redis command timeout, JDBC transient error).
     *
     * <p>A checkpoint write failure does NOT stop the stream: stream-core logs
     * the failure, emits this signal, and keeps processing events. The
     * checkpoint is resume metadata, so transient loss only matters if the
     * process restarts before the next successful checkpoint.</p>
     *
     * <p>Use this callback to wire alerts (e.g. "checkpoint failure rate &gt; X
     * over Y minutes") or stale-checkpoint detection in custom providers.</p>
     *
     * @param streamName stream identifier
     * @param cause      the exception thrown by the {@code CheckpointStore}
     */
    default void onCheckpointFailed(String streamName, Throwable cause) {}

    /**
     * Called when a stream resumes from {@code lastSeenToken} because
     * {@code lastProcessedToken} has aged out of the oplog (resume cascade
     * level 2 under {@code ResumeStrategy.PROCESSED_FIRST}).
     *
     * <p>This is a graceful degradation signal: the stream did not require
     * the operator to intervene, but events that were in flight at crash
     * time (handler not yet acknowledged) are not redelivered.</p>
     *
     * @param streamName stream identifier
     */
    default void onResumeFallbackToSeen(String streamName) {}

    /**
     * Called when a stream resumes from {@code lastProcessedToken} because
     * {@code lastSeenToken} has aged out of the oplog (resume cascade
     * level 2 under {@code ResumeStrategy.SEEN_FIRST}).
     *
     * <p>This is a graceful degradation signal: the heartbeat-fresh seen
     * token was unusable, so the stream fell back to the older processed
     * token. The MongoDB oplog scan may be long depending on how far behind
     * {@code lastProcessedToken} is.</p>
     *
     * @param streamName stream identifier
     */
    default void onResumeFallbackToProcessed(String streamName) {}

    /**
     * Called when neither {@code lastProcessedToken} nor {@code lastSeenToken}
     * are valid in the oplog and the configured {@code @Checkpoint.onHistoryLost}
     * strategy is applied (resume cascade level 3).
     *
     * @param streamName stream identifier
     */
    default void onResumeHistoryLost(String streamName) {}

    /**
     * Called when a checkpoint heartbeat probe fails while the stream itself
     * keeps running: transient backend error, bounded-read timeout, a reply
     * carrying a null post-batch resume token, or a chained resume token that
     * has already aged out of the oplog ({@code ChangeStreamHistoryLost}).
     *
     * <p>A probe failure never stops the stream and never writes to the
     * checkpoint — {@code lastHeartbeatTimestamp} does not move. Repeated
     * failures therefore surface as an aging heartbeat; this callback carries
     * the cause so monitoring can distinguish a transient outage from a
     * resume point that is no longer recoverable.</p>
     *
     * <p>Distinct from {@link #onResumeHistoryLost(String)}, which describes
     * the startup resume cascade: a heartbeat failure happens while a live
     * cursor keeps delivering events.</p>
     *
     * @param streamName stream identifier
     * @param cause      the failure cause
     */
    default void onHeartbeatProbeFailed(String streamName, Throwable cause) {}

    /**
     * Called periodically with the current lag between {@code lastSeenToken}
     * and {@code lastProcessedToken}. Useful for monitoring streams whose
     * handlers fall behind or get stuck.
     *
     * @param streamName  stream identifier
     * @param lagSeconds  time difference between seen and processed timestamps in seconds (0 if unknown)
     * @param lagEvents   approximate count of events between seen and processed (0 if unknown)
     */
    default void onCheckpointLag(String streamName, long lagSeconds, long lagEvents) {}

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
     * Called after a failed event has been successfully persisted to the
     * Dead Letter Queue by the configured {@link DlqStore}.
     *
     * <p>Emitted only on confirmed write success. If the store throws, this
     * callback is NOT invoked &mdash; see {@link #onEventDlqFailed} instead.</p>
     *
     * @param streamName stream identifier
     */
    void onEventSentToDlq(String streamName);

    /**
     * Called when a {@link DlqStore} write throws while persisting a failed
     * event (e.g. {@code MongoWriteConcernException} on {@code wtimeout},
     * Redis command timeout, JDBC transient error).
     *
     * <p>A DLQ write failure is operationally critical: the event has neither
     * been processed by the handler nor archived for later replay. Stream-core
     * logs the failure and emits this signal; the auto post-retry path then
     * resumes the stream, while manual {@code ctx.sendToDlq(...)} paths
     * re-throw to the user handler.</p>
     *
     * <p>Use this callback to wire alerts on DLQ store outages in custom
     * metrics providers.</p>
     *
     * @param streamName stream identifier
     * @param cause      the exception thrown by the {@code DlqStore}
     */
    default void onEventDlqFailed(String streamName, Throwable cause) {}

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
