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
package io.flowwarden.stream.internal.dlq;

import io.flowwarden.stream.annotation.DeadLetterQueue;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Immutable configuration parsed from a {@link DeadLetterQueue} annotation.
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public record DeadLetterQueueConfig(
        boolean enabled,
        String collection,
        int ttlDays,
        boolean includeOriginalDocument,
        boolean includeStackTrace
) {

    /**
     * Creates a {@link DeadLetterQueueConfig} from the given annotation.
     */
    public static DeadLetterQueueConfig fromAnnotation(DeadLetterQueue ann) {
        return new DeadLetterQueueConfig(
                ann.enabled(),
                ann.collection(),
                ann.ttlDays(),
                ann.includeOriginalDocument(),
                ann.includeStackTrace());
    }

    /**
     * Computes expiresAt from createdAt + ttlDays.
     *
     * @param createdAt the creation timestamp
     * @return the expiry timestamp, or {@code null} if ttlDays == 0 (permanent)
     */
    public Instant computeExpiresAt(Instant createdAt) {
        if (ttlDays == 0) {
            return null;
        }
        return createdAt.plus(ttlDays, ChronoUnit.DAYS);
    }
}
