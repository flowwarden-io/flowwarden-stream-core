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
import org.bson.BsonValue;
import org.bson.Document;

import java.time.Instant;
import java.util.Map;

/**
 * Immutable record representing an event that failed processing and was sent
 * to the Dead Letter Queue.
 *
 * <p>Aligned with the {@code _fw_dlq} MongoDB collection schema.</p>
 *
 * @param id              unique identifier (UUID)
 * @param streamName      name of the originating stream
 * @param operationType   MongoDB operation type (INSERT, UPDATE, etc.)
 * @param documentKey     {@code _id} of the source document
 * @param fullDocument    original document (nullable)
 * @param resumeToken     resume token of the event
 * @param error           error information
 * @param attempts        number of processing attempts
 * @param status          event status (e.g. {@link #STATUS_PENDING})
 * @param firstAttemptAt  timestamp of the first attempt
 * @param lastAttemptAt   timestamp of the last attempt
 * @param createdAt       timestamp when the DLQ entry was created
 * @param expiresAt       optional expiry timestamp (nullable, computed with ttlDays)
 * @param metadata        additional key-value data
 */
public record FailedEvent(
        String id,
        String streamName,
        String operationType,
        BsonValue documentKey,
        Document fullDocument,
        BsonDocument resumeToken,
        ErrorInfo error,
        int attempts,
        String status,
        Instant firstAttemptAt,
        Instant lastAttemptAt,
        Instant createdAt,
        Instant expiresAt,
        Map<String, Object> metadata
) {

    public static final String STATUS_PENDING = "PENDING";

    /**
     * Error details for a failed event.
     *
     * @param type       exception class name
     * @param message    error message
     * @param stackTrace stack trace (nullable)
     */
    public record ErrorInfo(
            String type,
            String message,
            String stackTrace
    ) {}
}
