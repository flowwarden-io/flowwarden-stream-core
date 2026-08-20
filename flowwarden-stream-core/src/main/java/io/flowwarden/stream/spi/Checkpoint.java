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

import org.bson.BsonDocument;

import java.time.Instant;
import java.util.Map;

/**
 * Immutable checkpoint representing the state of a Change Stream's progress.
 *
 * <p>Holds dual resume tokens: {@code lastSeenToken} tracks the last position
 * certified gap-free (an event received by the listener, or an interval the
 * server certified empty via the heartbeat probe), while
 * {@code lastProcessedToken} tracks the last event successfully handled.
 * This distinction enables at-least-once delivery guarantees.</p>
 *
 * <p>{@code lastHeartbeatTimestamp} records the last time a recoverable
 * position was <em>confirmed</em> — a fresh event token saved, or a successful
 * empty heartbeat probe (position write or re-certification of the unchanged
 * position). It is never updated on probe abstentions or failures, so its age
 * is the single operational signal for resume-point health.</p>
 *
 * @param streamName             unique stream identifier
 * @param instanceId             pod/instance identifier (nullable)
 * @param lastSeenToken          resume token of the last gap-free position (nullable)
 * @param lastSeenTimestamp      when that position was established (nullable)
 * @param lastProcessedToken     resume token of the last event successfully processed (nullable)
 * @param lastProcessedTimestamp timestamp of the last event successfully processed (nullable)
 * @param lastHeartbeatTimestamp last time a recoverable position was confirmed (nullable)
 * @param metadata               additional key-value data
 */
public record Checkpoint(
        String streamName,
        String instanceId,
        BsonDocument lastSeenToken,
        Instant lastSeenTimestamp,
        BsonDocument lastProcessedToken,
        Instant lastProcessedTimestamp,
        Instant lastHeartbeatTimestamp,
        Map<String, Object> metadata
) {

    /**
     * Convenience constructor without {@code lastHeartbeatTimestamp}, preserved
     * for callers predating the heartbeat field.
     */
    public Checkpoint(String streamName,
                      String instanceId,
                      BsonDocument lastSeenToken,
                      Instant lastSeenTimestamp,
                      BsonDocument lastProcessedToken,
                      Instant lastProcessedTimestamp,
                      Map<String, Object> metadata) {
        this(streamName, instanceId, lastSeenToken, lastSeenTimestamp,
                lastProcessedToken, lastProcessedTimestamp, null, metadata);
    }
}
