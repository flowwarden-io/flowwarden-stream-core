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
package io.flowwarden.stream.internal.restart;

import io.flowwarden.stream.annotation.RestartPolicy;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class RestartPolicyConfigTest {

    @RestartPolicy
    static class DefaultHandler {
    }

    @RestartPolicy(maxAttempts = 3, initialDelay = "500ms", maxDelay = "2s", multiplier = 3.0, jitter = true)
    static class CustomHandler {
    }

    // --- defaults ---

    @Test
    void defaults_matchTheAnnotationDefaults() {
        RestartPolicy ann = DefaultHandler.class.getAnnotation(RestartPolicy.class);

        assertThat(RestartPolicyConfig.fromAnnotation(ann)).isEqualTo(RestartPolicyConfig.DEFAULTS);
        assertThat(RestartPolicyConfig.DEFAULTS.isBounded()).isFalse();
    }

    @Test
    void defaults_backoffIsOneSecondDoublingCappedAtSixtySeconds() {
        // The cadence every stream had before @RestartPolicy existed:
        // 1, 2, 4, 8, 16, 32, then capped at 60 — never a hot loop against a
        // hard-down server, never more than a minute behind a recovered one.
        RestartPolicyConfig config = RestartPolicyConfig.DEFAULTS;

        assertThat(config.computeDelayMillis(1)).isEqualTo(1_000L);
        assertThat(config.computeDelayMillis(2)).isEqualTo(2_000L);
        assertThat(config.computeDelayMillis(3)).isEqualTo(4_000L);
        assertThat(config.computeDelayMillis(6)).isEqualTo(32_000L);
        assertThat(config.computeDelayMillis(7)).isEqualTo(60_000L);
        assertThat(config.computeDelayMillis(50)).isEqualTo(60_000L);
    }

    @Test
    void defaults_neverExhaust() {
        assertThat(RestartPolicyConfig.DEFAULTS.isExhausted(1)).isFalse();
        assertThat(RestartPolicyConfig.DEFAULTS.isExhausted(Integer.MAX_VALUE)).isFalse();
    }

    @Test
    void defaults_overflowingExponentStaysAtTheCap() {
        // 2^(attempt-1) overflows a double's range long before Integer.MAX_VALUE;
        // the cap must still hold rather than an infinite/NaN cast.
        assertThat(RestartPolicyConfig.DEFAULTS.computeDelayMillis(Integer.MAX_VALUE)).isEqualTo(60_000L);
    }

    // --- fromAnnotation ---

    @Test
    void fromAnnotationWithCustomValues() {
        RestartPolicy ann = CustomHandler.class.getAnnotation(RestartPolicy.class);
        RestartPolicyConfig config = RestartPolicyConfig.fromAnnotation(ann);

        assertThat(config.maxAttempts()).isEqualTo(3);
        assertThat(config.initialDelay()).isEqualTo(Duration.ofMillis(500));
        assertThat(config.maxDelay()).isEqualTo(Duration.ofSeconds(2));
        assertThat(config.multiplier()).isEqualTo(3.0);
        assertThat(config.jitter()).isTrue();
        assertThat(config.isBounded()).isTrue();
    }

    // --- isExhausted ---

    @Test
    void boundedPolicy_exhaustsBeyondMaxAttempts() {
        RestartPolicyConfig config = new RestartPolicyConfig(3, Duration.ofSeconds(1), Duration.ofSeconds(60), 2.0, false);

        assertThat(config.isExhausted(1)).isFalse();
        assertThat(config.isExhausted(3)).as("the last allowed attempt still runs").isFalse();
        assertThat(config.isExhausted(4)).isTrue();
    }

    // --- computeDelayMillis ---

    @Test
    void computeDelayWithJitterStaysWithinTwentyPercent() {
        RestartPolicy ann = CustomHandler.class.getAnnotation(RestartPolicy.class);
        RestartPolicyConfig config = RestartPolicyConfig.fromAnnotation(ann);
        // initialDelay=500ms, multiplier=3.0, maxDelay=2s, jitter=true

        for (int i = 0; i < 20; i++) {
            assertThat(config.computeDelayMillis(1)).isBetween(400L, 600L);      // ~500ms
            assertThat(config.computeDelayMillis(2)).isBetween(1_200L, 1_800L);  // ~1500ms
            assertThat(config.computeDelayMillis(3)).isBetween(1_600L, 2_400L);  // 4500ms capped to 2s
        }
    }

    @Test
    void computeDelayWithJitterNeverReturnsZero() {
        // Review finding: a 1ms delay times a jitter factor in [0.8, 1.0)
        // truncates to 0 — which the validator's "strictly positive" rule
        // exists to prevent. The floor is 1ms.
        RestartPolicyConfig config = new RestartPolicyConfig(0, Duration.ofMillis(1), Duration.ofMillis(1), 2.0, true);

        for (int i = 0; i < 1_000; i++) {
            assertThat(config.computeDelayMillis(1)).isGreaterThanOrEqualTo(1L);
            assertThat(config.computeDelayMillis(5)).isGreaterThanOrEqualTo(1L);
        }
    }

    @Test
    void computeDelayWithoutJitterIsDeterministicAndCapped() {
        RestartPolicyConfig config = new RestartPolicyConfig(0, Duration.ofMillis(500), Duration.ofSeconds(2), 3.0, false);

        assertThat(config.computeDelayMillis(1)).isEqualTo(500L);
        assertThat(config.computeDelayMillis(2)).isEqualTo(1_500L);
        assertThat(config.computeDelayMillis(3)).isEqualTo(2_000L);
    }
}
