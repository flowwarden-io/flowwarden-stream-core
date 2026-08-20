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

import java.util.Objects;

/**
 * Result of a single heartbeat probe against a change stream.
 *
 * <p>Exactly three outcomes exist, mirroring the three write behaviors of the
 * heartbeat tick:</p>
 * <ul>
 *   <li>{@link Type#EMPTY} — the server certified that no matching event
 *       exists between the chained resume token and {@link #pbrt()}. The
 *       heartbeat may advance (new PBRT) or re-certify (same PBRT).</li>
 *   <li>{@link Type#EVENT_PENDING} — the probe returned an event: undelivered
 *       events sit between the chained token and the oplog head. The heartbeat
 *       must abstain; progress belongs to the main stream.</li>
 *   <li>{@link Type#FAILED} — timeout, backend error, expired token, or a
 *       reply carrying a null post-batch resume token. No checkpoint write of
 *       any kind.</li>
 * </ul>
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public record ProbeOutcome(Type type, BsonDocument pbrt, Throwable cause) {

    public enum Type { EMPTY, EVENT_PENDING, FAILED }

    public static ProbeOutcome empty(BsonDocument pbrt) {
        Objects.requireNonNull(pbrt, "pbrt must not be null for an EMPTY outcome");
        return new ProbeOutcome(Type.EMPTY, pbrt, null);
    }

    public static ProbeOutcome eventPending() {
        return new ProbeOutcome(Type.EVENT_PENDING, null, null);
    }

    public static ProbeOutcome failed(Throwable cause) {
        return new ProbeOutcome(Type.FAILED, null, cause);
    }
}
