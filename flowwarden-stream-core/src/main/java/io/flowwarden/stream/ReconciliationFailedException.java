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
package io.flowwarden.stream;

import java.time.Instant;

/**
 * Thrown when a stream's {@link io.flowwarden.stream.annotation.Reconcile @Reconcile}
 * hook could not complete the reconciliation of a lost history window, so
 * the stream refuses to resume.
 *
 * <p>A {@link HistoryLostException}: the start is terminal for this attempt
 * exactly as under {@link OnHistoryLost#FAIL} — the managed restart loop
 * gives up, a {@code SINGLE_LEADER} lease is released, and
 * {@code onStreamStopped(CRASHED, cause)} is emitted. The expired tokens are
 * left untouched in the checkpoint: they are the durable marker that a
 * reconciliation is still due, and the next start (a corrected deployment, a
 * larger oplog) re-enters the recovery and calls the hook again. A failed
 * reconciliation never lets a stream resume on a stale view.</p>
 *
 * <p>Two causes:</p>
 * <ul>
 *   <li>the hook threw — {@link #getCause()} is that throwable;</li>
 *   <li>the reconciliation does not fit in the oplog window: the position
 *       resolved before the hook rolled out of the oplog while it ran, the
 *       second, bounded invocation ran, and its position rolled out too.
 *       Grow the oplog or make the reconciliation faster; {@link #getCause()}
 *       is {@code null}.</li>
 * </ul>
 */
public class ReconciliationFailedException extends HistoryLostException {

    private final int attempt;

    /**
     * The hook threw.
     *
     * @param streamName              the stream
     * @param lastCheckpointTimestamp the start of the lost window, may be null
     * @param attempt                 the hook invocation that failed (1 or 2)
     * @param hookFailure             what the hook threw
     */
    public ReconciliationFailedException(String streamName, Instant lastCheckpointTimestamp,
                                         int attempt, Throwable hookFailure) {
        super(streamName, lastCheckpointTimestamp,
                "Reconciliation of the lost history of stream '" + streamName + "' failed "
                        + "(attempt " + attempt + ", last known checkpoint activity at "
                        + lastCheckpointTimestamp + "): the @Reconcile hook threw "
                        + hookFailure + ". The stream will not resume on an unreconciled "
                        + "view; the expired tokens are kept so the next start reconciles "
                        + "again.",
                hookFailure);
        this.attempt = attempt;
    }

    /**
     * The reconciliation outlived the oplog window twice.
     *
     * @param streamName              the stream
     * @param lastCheckpointTimestamp the start of the lost window, may be null
     * @param secondWindowEnd         the end of the second, residual window
     *                                that rolled out as well
     */
    public ReconciliationFailedException(String streamName, Instant lastCheckpointTimestamp,
                                         Instant secondWindowEnd) {
        super(streamName, lastCheckpointTimestamp,
                "Reconciliation of the lost history of stream '" + streamName
                        + "' does not fit in the oplog window: the resume position resolved "
                        + "before the @Reconcile hook rolled out of the oplog while it ran, "
                        + "and so did the position of the second, bounded invocation (up to "
                        + secondWindowEnd + "). Grow the oplog so it covers the time a "
                        + "reconciliation takes, or make the reconciliation faster. The "
                        + "expired tokens are kept so the next start reconciles again.",
                null);
        this.attempt = 2;
    }

    /**
     * The hook invocation that failed: {@code 1}, or {@code 2} for the
     * bounded second invocation.
     */
    public int getAttempt() {
        return attempt;
    }
}
