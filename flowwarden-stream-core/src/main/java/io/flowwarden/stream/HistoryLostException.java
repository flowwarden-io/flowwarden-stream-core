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
 * Thrown when the saved resume token has expired from the MongoDB oplog and
 * the stream cannot start: under {@link OnHistoryLost#FAIL}, or — as the
 * {@link ReconciliationFailedException} subclass — when the stream's
 * {@code @Reconcile} hook failed under a self-repairing strategy. Terminal
 * for the restart loop in both cases.
 */
public class HistoryLostException extends RuntimeException {

    private final String streamName;
    private final Instant lastCheckpointTimestamp;

    public HistoryLostException(String streamName, Instant lastCheckpointTimestamp) {
        super("Change stream history lost for stream '" + streamName
                + "'. Last known checkpoint activity was at " + lastCheckpointTimestamp
                + ". The resume token has expired from the oplog and FAIL is in effect — "
                + "this stream will keep failing on every start until an operator intervenes. "
                + "Either delete this stream's document from the checkpoint collection "
                + "(_fw_checkpoints with the shipped Mongo stores) to restart from a fresh "
                + "position, or set @Checkpoint(onHistoryLost = RESUME_FROM_NOW) / "
                + "RESUME_FROM_OPLOG_START to recover automatically.");
        this.streamName = streamName;
        this.lastCheckpointTimestamp = lastCheckpointTimestamp;
    }

    /**
     * For subclasses that describe a specific history-lost outcome with their
     * own message, such as {@link ReconciliationFailedException}.
     *
     * @param streamName              the stream
     * @param lastCheckpointTimestamp last known checkpoint activity, may be null
     * @param message                 the operator-facing message
     * @param cause                   the underlying failure, may be null
     */
    protected HistoryLostException(String streamName, Instant lastCheckpointTimestamp,
                                   String message, Throwable cause) {
        super(message, cause);
        this.streamName = streamName;
        this.lastCheckpointTimestamp = lastCheckpointTimestamp;
    }

    public String getStreamName() {
        return streamName;
    }

    public Instant getLastCheckpointTimestamp() {
        return lastCheckpointTimestamp;
    }
}
