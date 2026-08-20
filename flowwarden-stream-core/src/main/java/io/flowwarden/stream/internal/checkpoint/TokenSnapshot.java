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
 * delivered to the listener ({@code EVENT}) from a resume position installed
 * at startup ({@code SEED} — cascade choice or bootstrap PBRT). The heartbeat
 * only treats {@code EVENT} tokens as fresh deliveries; both kinds are valid
 * chaining positions for the probe. A {@code PROCESSED_FIRST} resume seeds the
 * (possibly old) processed token — persisting it as a "new" seen position
 * would regress the level-2 safety net.</p>
 */
public record TokenSnapshot(BsonDocument token, Instant timestamp, Source source) {

    public enum Source { EVENT, SEED }

    /**
     * Convenience constructor for event-carried tokens.
     */
    public TokenSnapshot(BsonDocument token, Instant timestamp) {
        this(token, timestamp, Source.EVENT);
    }
}
