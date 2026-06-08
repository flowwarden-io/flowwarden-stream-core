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

import java.util.List;

/**
 * Lightweight snapshot of a stream's configuration, passed to
 * {@link StreamMetricsProvider#onStreamStarted}.
 *
 * @param streamName unique stream identifier
 * @param collection MongoDB collection being watched
 * @param database   MongoDB database name
 * @param mode       runtime mode (IMPERATIVE or REACTIVE)
 * @param zone             logical zone name from {@code @ChangeStream(zone = ...)}
 * @param mongoTemplateRef bean name of the custom MongoTemplate (empty if using the default)
 * @param checkpoint       checkpoint configuration (null if not configured)
 * @param retry            retry policy configuration (null if not configured)
 * @param dlq              dead letter queue configuration (null if not configured)
 * @param handlers         list of registered handler types (e.g., "INSERT", "UPDATE", "ON_CHANGE")
 * @param deploymentMode   deployment strategy (ALL_INSTANCES, SINGLE_LEADER)
 */
public record StreamConfiguration(
        String streamName,
        String collection,
        String database,
        String mode,
        String zone,
        String mongoTemplateRef,
        CheckpointConfig checkpoint,
        RetryConfig retry,
        DlqConfig dlq,
        List<String> handlers,
        String deploymentMode) {

    /**
     * Checkpoint configuration snapshot.
     * Enum values are serialized as strings for SPI portability.
     */
    public record CheckpointConfig(
            int saveEveryN,
            int saveIntervalSeconds,
            String startPosition,
            String onHistoryLost) {
    }

    /**
     * Retry policy configuration snapshot.
     */
    public record RetryConfig(
            int maxAttempts,
            String initialDelay,
            String maxDelay,
            double multiplier,
            boolean jitter) {
    }

    /**
     * Dead letter queue configuration snapshot.
     *
     * <p>Backend-agnostic. Backend-specific tuning (e.g. the MongoDB collection)
     * is not exposed in this snapshot.</p>
     */
    public record DlqConfig(
            boolean enabled,
            int retentionDays,
            boolean includeOriginalDocument,
            boolean includeStackTrace) {
    }
}
