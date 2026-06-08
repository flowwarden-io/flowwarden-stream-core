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

import io.flowwarden.stream.OperationType;

import java.time.Instant;

/**
 * Lightweight metadata about a Change Stream event, passed to
 * {@link StreamMetricsProvider#onEventReceived} and
 * {@link StreamMetricsProvider#onEventError}.
 *
 * @param eventId        unique event identifier
 * @param operationType  type of MongoDB operation
 * @param collectionName source collection name
 * @param documentKey    the {@code _id} of the affected document (may be null for DROP/INVALIDATE)
 * @param clusterTime    MongoDB cluster timestamp of the operation (second precision)
 * @param wallTime       MongoDB wall-clock time of the operation (millisecond precision, MongoDB 6.0+, nullable)
 */
public record ChangeEventMetadata(
        String eventId,
        OperationType operationType,
        String collectionName,
        String documentKey,
        Instant clusterTime,
        Instant wallTime) {
}
