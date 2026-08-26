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
 * <p>Holds two anchors of distinct natures: {@code lastProcessedToken} is
 * the durability anchor of the <em>work</em> — the token of the last
 * terminally settled event (handler success, {@code @Filter} rejection, no
 * matching handler, or a terminal skip/DLQ decision) — while
 * {@code lastSeenToken} is the durability anchor of the <em>position</em> —
 * exclusively a server-certified post-batch resume token written by the
 * idle heartbeat (or the bootstrap/recovery establishment). The resume
 * cascade tries the processed anchor first (strict at-least-once), the
 * certified position as the safety net. Checkpoints written by earlier
 * versions may hold an event token in {@code lastSeenToken}; it remains a
 * valid resume position.</p>
 *
 * <p>{@code lastHeartbeatTimestamp} records the last time a recoverable
 * position was <em>confirmed</em> — a processed-anchor write, or a
 * successful empty heartbeat probe (position write or re-certification of
 * the unchanged position). It is never updated on probe abstentions or
 * failures, so its age is the single operational signal for resume-point
 * health.</p>
 *
 * @param streamName             unique stream identifier
 * @param instanceId             pod/instance identifier (nullable)
 * @param lastSeenToken          server-certified resume position (nullable)
 * @param lastSeenTimestamp      when that position was certified (nullable)
 * @param lastProcessedToken     resume token of the last terminally settled event (nullable)
 * @param lastProcessedTimestamp cluster time of the last terminally settled event (nullable)
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
