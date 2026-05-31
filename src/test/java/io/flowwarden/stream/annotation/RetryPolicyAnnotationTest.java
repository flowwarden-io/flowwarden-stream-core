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
package io.flowwarden.stream.annotation;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RetryPolicyAnnotationTest {

    @RetryPolicy
    static class DefaultRetryHandler {
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
    static class CustomRetryHandler {
    }

    @Test
    void defaultValues() {
        RetryPolicy ann = DefaultRetryHandler.class.getAnnotation(RetryPolicy.class);

        assertThat(ann.maxAttempts()).isEqualTo(3);
        assertThat(ann.initialDelay()).isEqualTo("500ms");
        assertThat(ann.maxDelay()).isEqualTo("30s");
        assertThat(ann.multiplier()).isEqualTo(2.0);
        assertThat(ann.retryOn()).isEmpty();
        assertThat(ann.noRetryOn()).containsExactlyInAnyOrder(
                IllegalArgumentException.class,
                NullPointerException.class,
                ClassCastException.class);
        assertThat(ann.jitter()).isTrue();
    }

    @Test
    void customValues() {
        RetryPolicy ann = CustomRetryHandler.class.getAnnotation(RetryPolicy.class);

        assertThat(ann.maxAttempts()).isEqualTo(5);
        assertThat(ann.initialDelay()).isEqualTo("1s");
        assertThat(ann.maxDelay()).isEqualTo("1m");
        assertThat(ann.multiplier()).isEqualTo(3.0);
        assertThat(ann.retryOn()).containsExactly(RuntimeException.class);
        assertThat(ann.noRetryOn()).containsExactly(UnsupportedOperationException.class);
        assertThat(ann.jitter()).isFalse();
    }
}
