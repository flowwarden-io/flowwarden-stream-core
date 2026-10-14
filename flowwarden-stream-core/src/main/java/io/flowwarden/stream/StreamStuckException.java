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

import java.time.Duration;

/**
 * Signals that a running stream was declared <em>stuck</em>: its idle
 * heartbeat probe found matching events waiting undelivered on
 * {@code stuckAfterProbes} consecutive occasions while the stream itself
 * delivered nothing and no handler was running — the shape of a cursor whose
 * {@code getMore} hangs on a half-open connection without the driver ever
 * reporting it.
 *
 * <p>Carried as the {@code cause} of the {@code onStreamStopped(CRASHED)}
 * signal the stream emits when it is handed to the managed restart loop,
 * so a metrics provider can tell a detected hang from a cursor error.</p>
 *
 * @see io.flowwarden.stream.annotation.Checkpoint#stuckAfterProbes()
 */
public class StreamStuckException extends RuntimeException {

    private final String streamName;
    private final int consecutiveAbstentions;
    private final Duration stuckFor;

    public StreamStuckException(String streamName, int consecutiveAbstentions, Duration stuckFor) {
        super("Stream '" + streamName + "' is stuck: " + consecutiveAbstentions
                + " consecutive idle probes found events waiting undelivered while the stream "
                + "delivered nothing for " + stuckFor + " and no handler was running — the cursor "
                + "appears dead (typically a half-open connection the driver never times out; "
                + "consider setting socketTimeoutMS on the Mongo client).");
        this.streamName = streamName;
        this.consecutiveAbstentions = consecutiveAbstentions;
        this.stuckFor = stuckFor;
    }

    public String getStreamName() {
        return streamName;
    }

    /** Consecutive idle-probe abstentions that led to the verdict. */
    public int getConsecutiveAbstentions() {
        return consecutiveAbstentions;
    }

    /** How long the stream had delivered nothing when it was declared stuck. */
    public Duration getStuckFor() {
        return stuckFor;
    }
}
