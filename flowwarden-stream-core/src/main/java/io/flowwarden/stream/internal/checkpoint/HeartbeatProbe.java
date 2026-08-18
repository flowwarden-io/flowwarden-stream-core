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
     * @param resumeAfter the token to chain from, or {@code null} to open the
     *                    cursor at the current position (bootstrap only —
     *                    never used while a prior position exists)
     * @return the probe outcome; never throws
     */
    ProbeOutcome probe(BsonDocument resumeAfter);
}
