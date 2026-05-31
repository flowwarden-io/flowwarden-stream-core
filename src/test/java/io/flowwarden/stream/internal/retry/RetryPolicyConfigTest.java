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
package io.flowwarden.stream.internal.retry;

import io.flowwarden.stream.annotation.RetryPolicy;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetryPolicyConfigTest {

    // --- Duration parsing ---

    @Test
    void parsesMilliseconds() {
        assertThat(RetryPolicyConfig.parseDuration("500ms")).isEqualTo(Duration.ofMillis(500));
    }

    @Test
    void parsesSeconds() {
        assertThat(RetryPolicyConfig.parseDuration("30s")).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void parsesMinutes() {
        assertThat(RetryPolicyConfig.parseDuration("1m")).isEqualTo(Duration.ofMinutes(1));
    }

    @Test
    void rejectsInvalidDurationFormat() {
        assertThatThrownBy(() -> RetryPolicyConfig.parseDuration("invalid"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid duration format");
    }

    // --- fromAnnotation ---

    @RetryPolicy
    static class DefaultHandler {
    }

    @RetryPolicy(
            maxAttempts = 5,
            initialDelay = "1s",
            maxDelay = "1m",
            multiplier = 3.0,
            retryOn = {RuntimeException.class},
            noRetryOn = {UnsupportedOperationException.class},
            jitter = false
    )
    static class CustomHandler {
    }

    @Test
    void fromAnnotationWithDefaults() {
        RetryPolicy ann = DefaultHandler.class.getAnnotation(RetryPolicy.class);
        RetryPolicyConfig config = RetryPolicyConfig.fromAnnotation(ann);

        assertThat(config.maxAttempts()).isEqualTo(3);
        assertThat(config.initialDelay()).isEqualTo(Duration.ofMillis(500));
        assertThat(config.maxDelay()).isEqualTo(Duration.ofSeconds(30));
        assertThat(config.multiplier()).isEqualTo(2.0);
        assertThat(config.retryOn()).isEmpty();
        assertThat(config.noRetryOn()).containsExactlyInAnyOrder(
                IllegalArgumentException.class,
                NullPointerException.class,
                ClassCastException.class);
        assertThat(config.jitter()).isTrue();
    }

    @Test
    void fromAnnotationWithCustomValues() {
        RetryPolicy ann = CustomHandler.class.getAnnotation(RetryPolicy.class);
        RetryPolicyConfig config = RetryPolicyConfig.fromAnnotation(ann);

        assertThat(config.maxAttempts()).isEqualTo(5);
        assertThat(config.initialDelay()).isEqualTo(Duration.ofSeconds(1));
        assertThat(config.maxDelay()).isEqualTo(Duration.ofMinutes(1));
        assertThat(config.multiplier()).isEqualTo(3.0);
        assertThat(config.retryOn()).containsExactly(RuntimeException.class);
        assertThat(config.noRetryOn()).containsExactly(UnsupportedOperationException.class);
        assertThat(config.jitter()).isFalse();
    }

    // --- computeDelayMillis ---

    @Test
    void computeDelayWithoutJitter() {
        RetryPolicy ann = CustomHandler.class.getAnnotation(RetryPolicy.class);
        RetryPolicyConfig config = RetryPolicyConfig.fromAnnotation(ann);
        // multiplier=3.0, initialDelay=1s, maxDelay=1m, jitter=false

        // attempt 1: 1000 * 3^0 = 1000ms
        assertThat(config.computeDelayMillis(1)).isEqualTo(1000L);
        // attempt 2: 1000 * 3^1 = 3000ms
        assertThat(config.computeDelayMillis(2)).isEqualTo(3000L);
        // attempt 3: 1000 * 3^2 = 9000ms
        assertThat(config.computeDelayMillis(3)).isEqualTo(9000L);
        // attempt 4: 1000 * 3^3 = 27000ms
        assertThat(config.computeDelayMillis(4)).isEqualTo(27000L);
    }

    @Test
    void computeDelayIsCappedAtMaxDelay() {
        RetryPolicy ann = CustomHandler.class.getAnnotation(RetryPolicy.class);
        RetryPolicyConfig config = RetryPolicyConfig.fromAnnotation(ann);
        // maxDelay = 1m = 60000ms, multiplier=3.0, initialDelay=1s

        // attempt 5: 1000 * 3^4 = 81000ms → capped to 60000ms
        assertThat(config.computeDelayMillis(5)).isEqualTo(60000L);
    }

    @Test
    void computeDelayWithDefaultsBackoffSequence() {
        RetryPolicy ann = DefaultHandler.class.getAnnotation(RetryPolicy.class);
        RetryPolicyConfig config = RetryPolicyConfig.fromAnnotation(ann);
        // multiplier=2.0, initialDelay=500ms, maxDelay=30s, jitter=true

        // With jitter, delay should be within ±20% of expected
        for (int i = 0; i < 20; i++) {
            long delay1 = config.computeDelayMillis(1); // expected ~500ms
            assertThat(delay1).isBetween(400L, 600L);

            long delay2 = config.computeDelayMillis(2); // expected ~1000ms
            assertThat(delay2).isBetween(800L, 1200L);

            long delay3 = config.computeDelayMillis(3); // expected ~2000ms
            assertThat(delay3).isBetween(1600L, 2400L);
        }
    }

    // --- shouldRetry ---

    @Test
    void shouldRetryWhenUnderMaxAttempts() {
        RetryPolicy ann = DefaultHandler.class.getAnnotation(RetryPolicy.class);
        RetryPolicyConfig config = RetryPolicyConfig.fromAnnotation(ann);

        assertThat(config.shouldRetry(new RuntimeException("fail"), 1)).isTrue();
        assertThat(config.shouldRetry(new RuntimeException("fail"), 2)).isTrue();
    }

    @Test
    void shouldNotRetryWhenAtMaxAttempts() {
        RetryPolicy ann = DefaultHandler.class.getAnnotation(RetryPolicy.class);
        RetryPolicyConfig config = RetryPolicyConfig.fromAnnotation(ann);
        // maxAttempts=3

        assertThat(config.shouldRetry(new RuntimeException("fail"), 3)).isFalse();
        assertThat(config.shouldRetry(new RuntimeException("fail"), 4)).isFalse();
    }

    @Test
    void shouldNotRetryOnNoRetryExceptions() {
        RetryPolicy ann = DefaultHandler.class.getAnnotation(RetryPolicy.class);
        RetryPolicyConfig config = RetryPolicyConfig.fromAnnotation(ann);

        assertThat(config.shouldRetry(new IllegalArgumentException(), 1)).isFalse();
        assertThat(config.shouldRetry(new NullPointerException(), 1)).isFalse();
        assertThat(config.shouldRetry(new ClassCastException(), 1)).isFalse();
    }

    @Test
    void shouldRetryOnlyRetryOnExceptions() {
        RetryPolicy ann = CustomHandler.class.getAnnotation(RetryPolicy.class);
        RetryPolicyConfig config = RetryPolicyConfig.fromAnnotation(ann);
        // retryOn = {RuntimeException.class}, noRetryOn = {UnsupportedOperationException.class}

        assertThat(config.shouldRetry(new RuntimeException("fail"), 1)).isTrue();
        // IOException is not in retryOn → no retry
        assertThat(config.shouldRetry(new java.io.IOException("io"), 1)).isFalse();
    }

    @Test
    void noRetryOnTakesPrecedenceOverRetryOn() {
        RetryPolicy ann = CustomHandler.class.getAnnotation(RetryPolicy.class);
        RetryPolicyConfig config = RetryPolicyConfig.fromAnnotation(ann);
        // retryOn = {RuntimeException.class}, noRetryOn = {UnsupportedOperationException.class}
        // UnsupportedOperationException extends RuntimeException

        assertThat(config.shouldRetry(new UnsupportedOperationException(), 1)).isFalse();
    }
}
