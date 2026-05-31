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
 * <p>Holds dual resume tokens: {@code lastSeenToken} tracks the last event received,
 * while {@code lastProcessedToken} tracks the last event successfully handled.
 * This distinction enables at-least-once delivery guarantees.</p>
 *
 * @param streamName           unique stream identifier
 * @param instanceId           pod/instance identifier (nullable)
 * @param lastSeenToken        resume token of the last event received (nullable)
 * @param lastSeenTimestamp     timestamp of the last event received (nullable)
 * @param lastProcessedToken   resume token of the last event successfully processed (nullable)
 * @param lastProcessedTimestamp timestamp of the last event successfully processed (nullable)
 * @param metadata             additional key-value data
 */
public record Checkpoint(
        String streamName,
        String instanceId,
        BsonDocument lastSeenToken,
        Instant lastSeenTimestamp,
        BsonDocument lastProcessedToken,
        Instant lastProcessedTimestamp,
        Map<String, Object> metadata
) {
}
