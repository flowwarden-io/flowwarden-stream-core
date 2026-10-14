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

class RestartPolicyAnnotationTest {

    @RestartPolicy
    static class DefaultRestartHandler {
    }

    @RestartPolicy(
            maxAttempts = 10,
            initialDelay = "500ms",
            maxDelay = "5m",
            multiplier = 3.0,
            jitter = true
    )
    static class CustomRestartHandler {
    }

    @Test
    void defaultValues_areTheCadenceOfAnUnannotatedStream() {
        RestartPolicy ann = DefaultRestartHandler.class.getAnnotation(RestartPolicy.class);

        assertThat(ann.maxAttempts()).as("0 = unlimited").isZero();
        assertThat(ann.initialDelay()).isEqualTo("1s");
        assertThat(ann.maxDelay()).isEqualTo("60s");
        assertThat(ann.multiplier()).isEqualTo(2.0);
        assertThat(ann.jitter()).isFalse();
    }

    @Test
    void customValues() {
        RestartPolicy ann = CustomRestartHandler.class.getAnnotation(RestartPolicy.class);

        assertThat(ann.maxAttempts()).isEqualTo(10);
        assertThat(ann.initialDelay()).isEqualTo("500ms");
        assertThat(ann.maxDelay()).isEqualTo("5m");
        assertThat(ann.multiplier()).isEqualTo(3.0);
        assertThat(ann.jitter()).isTrue();
    }
}
