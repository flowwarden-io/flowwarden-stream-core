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
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;

class DeadLetterQueueConfigTest {

    @DeadLetterQueue
    static class DefaultDlq {
    }

    @DeadLetterQueue(
            enabled = false,
            collection = "custom_dlq",
            ttlDays = 90,
            includeOriginalDocument = false,
            includeStackTrace = false
    )
    static class CustomDlq {
    }

    @DeadLetterQueue(ttlDays = 0)
    static class PermanentDlq {
    }

    @Test
    void fromAnnotationMapsDefaultValues() {
        DeadLetterQueue ann = DefaultDlq.class.getAnnotation(DeadLetterQueue.class);
        DeadLetterQueueConfig config = DeadLetterQueueConfig.fromAnnotation(ann);

        assertThat(config.enabled()).isTrue();
        assertThat(config.collection()).isEqualTo("_fw_dlq");
        assertThat(config.ttlDays()).isEqualTo(30);
        assertThat(config.includeOriginalDocument()).isTrue();
        assertThat(config.includeStackTrace()).isTrue();
    }

    @Test
    void fromAnnotationMapsCustomValues() {
        DeadLetterQueue ann = CustomDlq.class.getAnnotation(DeadLetterQueue.class);
        DeadLetterQueueConfig config = DeadLetterQueueConfig.fromAnnotation(ann);

        assertThat(config.enabled()).isFalse();
        assertThat(config.collection()).isEqualTo("custom_dlq");
        assertThat(config.ttlDays()).isEqualTo(90);
        assertThat(config.includeOriginalDocument()).isFalse();
        assertThat(config.includeStackTrace()).isFalse();
    }

    @Test
    void computeExpiresAtWithPositiveTtl() {
        DeadLetterQueue ann = DefaultDlq.class.getAnnotation(DeadLetterQueue.class);
        DeadLetterQueueConfig config = DeadLetterQueueConfig.fromAnnotation(ann);

        Instant createdAt = Instant.parse("2026-01-01T00:00:00Z");
        Instant expiresAt = config.computeExpiresAt(createdAt);

        assertThat(expiresAt).isEqualTo(createdAt.plus(30, ChronoUnit.DAYS));
    }

    @Test
    void computeExpiresAtWithZeroTtlReturnsNull() {
        DeadLetterQueue ann = PermanentDlq.class.getAnnotation(DeadLetterQueue.class);
        DeadLetterQueueConfig config = DeadLetterQueueConfig.fromAnnotation(ann);

        Instant createdAt = Instant.parse("2026-01-01T00:00:00Z");
        Instant expiresAt = config.computeExpiresAt(createdAt);

        assertThat(expiresAt).isNull();
    }
}
