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

import java.time.Instant;

/**
 * Holds the latest known resume position of a stream, used by the periodic
 * checkpoint heartbeat.
 *
 * <p>The {@link Source} distinguishes a token carried by an event actually
 * settled by the listener ({@code EVENT}) from a resume position installed
 * at startup ({@code SEED} — cascade choice or bootstrap PBRT). Only
 * {@code EVENT} tokens are candidates for the processed-anchor flush (a
 * resume seed is not a settlement); both kinds are valid chaining positions
 * for the probe.</p>
 *
 * <p>{@code timestamp} is the wall-clock instant the snapshot was installed
 * (the idleness clock); {@code clusterTime} is the event's server cluster
 * time when known ({@code EVENT} snapshots) — the value persisted as
 * {@code lastProcessedTimestamp}, so a replayed old event never reports a
 * current anchor age. {@code SEED} snapshots carry no cluster time.</p>
 */
public record TokenSnapshot(BsonDocument token, Instant timestamp, Source source,
                            Instant clusterTime) {

    public enum Source { EVENT, SEED }

    /**
     * Convenience constructor for event-carried tokens without a known
     * cluster time.
     */
    public TokenSnapshot(BsonDocument token, Instant timestamp) {
        this(token, timestamp, Source.EVENT, null);
    }

    public TokenSnapshot(BsonDocument token, Instant timestamp, Source source) {
        this(token, timestamp, source, null);
    }

    /**
     * The timestamp to persist alongside the token: the event's cluster time
     * when known, the installation instant otherwise.
     */
    public Instant anchorTimestamp() {
        return clusterTime != null ? clusterTime : timestamp;
    }
}
