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

import io.flowwarden.stream.annotation.DeadLetterQueue;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Cross-cutting DLQ policy passed to {@link DlqStore#save(FailedEvent, DlqPolicy)}.
 *
 * <p>Carries only the fields every DLQ backend can honour. Backend-specific
 * tuning (e.g. the MongoDB collection name) is resolved by the impl itself
 * from its own companion annotation (such as
 * {@link io.flowwarden.stream.annotation.MongoDlqOptions}) plus configuration
 * properties.</p>
 *
 * @param retentionDays           retention in days; {@code 0} means permanent
 * @param includeOriginalDocument whether the original document is included in the DLQ entry
 * @param includeStackTrace       whether the stack trace is included in the DLQ entry
 */
public record DlqPolicy(
        int retentionDays,
        boolean includeOriginalDocument,
        boolean includeStackTrace) {

    /**
     * Builds a policy from a {@link DeadLetterQueue} annotation.
     */
    public static DlqPolicy fromAnnotation(DeadLetterQueue ann) {
        return new DlqPolicy(
                ann.retentionDays(),
                ann.includeOriginalDocument(),
                ann.includeStackTrace());
    }

    /**
     * Computes the entry expiry from a creation timestamp and {@link #retentionDays}.
     *
     * @param createdAt creation timestamp
     * @return expiry timestamp, or {@code null} when {@code retentionDays == 0} (permanent)
     */
    public Instant computeExpiresAt(Instant createdAt) {
        if (retentionDays == 0) {
            return null;
        }
        return createdAt.plus(retentionDays, ChronoUnit.DAYS);
    }
}
