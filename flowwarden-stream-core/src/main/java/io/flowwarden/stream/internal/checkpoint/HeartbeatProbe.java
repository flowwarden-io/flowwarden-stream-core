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
package io.flowwarden.stream.internal.checkpoint;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

/**
 * Opens an ephemeral, bounded change stream cursor to obtain a post-batch
 * resume token (PBRT) certified by the server.
 *
 * <p>Implementations must replicate the main stream's collection, resolved
 * {@code @Pipeline} stages, {@code fullDocument} and
 * {@code fullDocumentBeforeChange} options (a pipeline matching on
 * {@code fullDocument.*} matches fewer events without {@code UPDATE_LOOKUP} —
 * replicating the options is a correctness requirement), stamp the cursor with
 * an identifying {@code comment}, force an actual server read, and always
 * close the server-side cursor.</p>
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public interface HeartbeatProbe {

    /**
     * Probes the stream's change stream from the given position.
     *
     * @param resumeAfter the token to chain from (never null — probing from
     *                    "now" cannot certify anything about a lagging main
     *                    cursor; the only now-anchored read is
     *                    {@link #initialPosition()})
     * @return the probe outcome; never throws
     */
    ProbeOutcome probe(BsonDocument resumeAfter);

    /**
     * Probes the stream's change stream from an operation time instead of a
     * resume token — the {@code RESUME_FROM_OPLOG_START} recovery, where the
     * dead tokens have been cleared and the stream resumes at the oldest
     * available oplog entry. An {@code EMPTY} outcome certifies nothing
     * matching remains to replay from that time, so its PBRT is a safe
     * initial position.
     *
     * @param operationTime the cluster time to start scanning at (never
     *                      "now" — that would skip the very history this
     *                      recovery promises to replay)
     * @return the probe outcome; never throws
     */
    ProbeOutcome probeFromOperationTime(BsonTimestamp operationTime);

    /**
     * Captures the server's current position for a stream that has no prior
     * one, from the change stream aggregate's <em>initial</em> reply — before
     * any {@code getMore}, and therefore before any event can be returned and
     * lost in a cursor hand-off. The main stream must be opened with
     * {@code resumeAfter(returned PBRT)}: every event committed after this
     * capture, including during the hand-off window, falls inside the main
     * stream's resume range.
     *
     * @return the initial PBRT; never null
     * @throws RuntimeException if the aggregate fails or the reply carries no
     *                          post-batch resume token — bootstrap is a
     *                          startup precondition, not a best effort
     */
    BsonDocument initialPosition();
}
