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
     * <p>Emitted in two situations:</p>
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
     * successfully resubscribed by the managed restart loop — meaning the
     * new cursor is known, or after a bounded wait assumed, to be alive, not
     * merely that the start method returned.
     *
     * <p>Always follows an {@code onStreamStopped(streamName, CRASHED, cause)}
     * for the same incident, and an {@link #onStreamStarted} fires for the
     * new subscription as usual — this signal adds the restart-specific
     * context: how many attempts the recovery took and what killed the
     * previous subscription. A recurring stream of these is an operational
     * signal (flaky network, undersized oplog) even though each individual
     * incident healed itself.</p>
     *
     * <p>The cursor opens asynchronously after {@code onStreamStarted}, so
     * this signal is emitted on the generation's proof of life — the cursor
     * is <em>known alive</em>: its first delivered event, or (IMPERATIVE)
     * the reading task's completed start. A resubscribed generation whose
     * open is refused dies without ever emitting it — the loop continues its
     * attempt count instead. As a heuristic applied in both modes, an idle
     * generation that outlives the stream's {@code @RestartPolicy}
     * {@code maxDelay} (60s by default) without dying is <em>assumed
     * alive</em> and emits it then: in REACTIVE mode, where the first event
     * is the only open signal, this is the earliest an idle restart can be
     * reported. That bound is configured independently of the driver's
     * timeouts, so nothing guarantees an open fails within it; an open that
     * fails later is counted as a fresh death at attempt 1.</p>
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
     * stream cursor. Followed by an
     * {@code onStreamStopped(streamName, CRASHED, null)} for the cursor
     * death itself, unless that death was already reported for the same
     * stream generation.
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
     * <p>
     * Always about one event: {@code attemptNumber} is 1-based and
     * {@code metadata} is never {@code null}. Stream-level failures — a
     * cursor death, a refused open, a collection invalidation — are not
     * reported here; they reach the provider through
     * {@link #onStreamStopped(String, StopReason, Throwable)} with
     * {@link StopReason#CRASHED} (or {@link #onStreamInvalidated}).
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
     * level 2).
     *
     * <p>This is a graceful degradation signal: the stream did not require
     * the operator to intervene, but events settled after the certified
     * position that the aged-out processed anchor covered are not
     * redelivered.</p>
     *
     * @param streamName stream identifier
     */
    default void onResumeFallbackToSeen(String streamName) {}

    /**
     * Called when neither {@code lastProcessedToken} nor {@code lastSeenToken}
     * are valid in the oplog and the configured {@code @Checkpoint.onHistoryLost}
     * strategy is applied (resume cascade level 3).
     *
     * @param streamName stream identifier
     */
    default void onResumeHistoryLost(String streamName) {}

    /**
     * Called when the stream's {@code @Reconcile} hook has returned and the
     * stream is about to resume: the application reconciled the lost window.
     * Emitted after the fact, never before the hook completed.
     *
     * @param streamName stream identifier
     * @param attempt    the hook invocation ({@code 1}, or {@code 2} for the
     *                   bounded second invocation after the oplog rolled
     *                   during the first)
     * @param elapsed    how long the hook ran
     */
    default void onHistoryLostReconciled(String streamName, int attempt, java.time.Duration elapsed) {}

    /**
     * Called when the stream's {@code @Reconcile} hook threw, or when the
     * reconciliation outlived the oplog window twice: the stream does not
     * start ({@code onStreamStopped(CRASHED, cause)} follows with a
     * {@link io.flowwarden.stream.ReconciliationFailedException}).
     *
     * @param streamName stream identifier
     * @param attempt    the hook invocation that failed
     * @param cause      what the hook threw, or the
     *                   {@code ReconciliationFailedException} when the
     *                   window rolled out twice
     */
    default void onHistoryLostReconciliationFailed(String streamName, int attempt, Throwable cause) {}

    /**
     * Called when a checkpoint heartbeat probe fails while the stream itself
     * keeps running: transient backend error, bounded-read timeout, a reply
     * carrying a null post-batch resume token, or a chained resume token that
     * has already aged out of the oplog ({@code ChangeStreamHistoryLost}).
     *
     * <p>A probe failure never stops the stream. It does not write to the
     * checkpoint either — {@code lastHeartbeatTimestamp} does not move — with
     * one exception: when an <em>idle</em> probe finds that the chained
     * position has rolled out of the oplog, the heartbeat repairs the
     * position in place (fresh server-certified position, dead tokens
     * purged), which refreshes {@code lastHeartbeatTimestamp} and emits
     * {@link #onCheckpoint(String, String)}. Otherwise repeated failures
     * surface as an aging heartbeat; this callback carries the cause so
     * monitoring can distinguish a transient outage from a resume point that
     * is no longer recoverable.</p>
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
     * Reserved for buffer observability. <strong>Not emitted by stream-core
     * today</strong>: no code path reports buffer status yet. Implementations
     * should treat it as a no-op until a release documents its emission.
     *
     * @param streamName  stream identifier
     * @param currentSize current number of events in the buffer
     * @param maxSize     maximum buffer capacity
     */
    void onBufferStatus(String streamName, int currentSize, int maxSize);

    /**
     * Reserved for backpressure observability. <strong>Not emitted by
     * stream-core today</strong>: no code path triggers a
     * {@link BackpressureAction} yet.
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
     * {@code status} will be {@code "UNAVAILABLE"}; any failure while reading
     * it is reported the same way.</p>
     *
     * @param logLengthHours oplog window duration in hours (0 if unavailable)
     * @param status         {@code "OK"} or {@code "UNAVAILABLE"}
     */
    void onOplogStats(double logLengthHours, String status);

    /**
     * Standing DLQ backlog gauge: the number of {@code PENDING} entries
     * sitting in the stream's DLQ, from {@link DlqStore#count(String)}.
     * Complements {@link #onEventSentToDlq}, which only observes write
     * activity — this is the backlog itself, drain included.
     *
     * <p>Emitted periodically (same cadence as {@link #onOplogStats}) for
     * every stream with a DLQ configured, and refreshed right after each
     * successful DLQ write. Never emitted when the configured store cannot
     * count (a negative {@code count}), so a publish-only backend produces
     * no lying zero-gauge.</p>
     *
     * @param streamName   the stream name
     * @param pendingCount the current pending entry count (never negative)
     */
    default void onDlqBacklog(String streamName, long pendingCount) {
    }

    /**
     * Called when this instance's leadership role changes for a
     * {@code SINGLE_LEADER} stream. Never called for {@code ALL_INSTANCES}
     * streams, which do not take part in an election.
     *
     * @param streamName the stream name
     * @param role       {@code "LEADER"} or {@code "STANDBY"}
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
