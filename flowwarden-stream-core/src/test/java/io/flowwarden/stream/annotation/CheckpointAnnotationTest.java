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

import io.flowwarden.stream.StartPosition;
import org.junit.jupiter.api.Test;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static org.assertj.core.api.Assertions.assertThat;

class CheckpointAnnotationTest {

    @Checkpoint
    static class DefaultCheckpoint {
    }

    @Checkpoint(saveEveryN = 5, saveIntervalSeconds = 10, startPosition = StartPosition.LATEST)
    static class CustomCheckpoint {
    }

    @Checkpoint(saveIntervalSeconds = 0)
    static class DisabledInterval {
    }

    @Test
    void defaultValues() {
        Checkpoint cp = DefaultCheckpoint.class.getAnnotation(Checkpoint.class);
        assertThat(cp).isNotNull();
        assertThat(cp.saveEveryN()).isEqualTo(1);
        assertThat(cp.saveIntervalSeconds()).isEqualTo(5);
        assertThat(cp.startPosition()).isEqualTo(StartPosition.RESUME);
    }

    @Test
    void customValues() {
        Checkpoint cp = CustomCheckpoint.class.getAnnotation(Checkpoint.class);
        assertThat(cp).isNotNull();
        assertThat(cp.saveEveryN()).isEqualTo(5);
        assertThat(cp.saveIntervalSeconds()).isEqualTo(10);
        assertThat(cp.startPosition()).isEqualTo(StartPosition.LATEST);
    }

    @Test
    void disabledIntervalIsZero() {
        Checkpoint cp = DisabledInterval.class.getAnnotation(Checkpoint.class);
        assertThat(cp).isNotNull();
        assertThat(cp.saveIntervalSeconds()).isEqualTo(0);
    }

    @Test
    void retentionIsRuntime() {
        Retention retention = Checkpoint.class.getAnnotation(Retention.class);
        assertThat(retention).isNotNull();
        assertThat(retention.value()).isEqualTo(RetentionPolicy.RUNTIME);
    }

    @Test
    void targetIsType() {
        Target target = Checkpoint.class.getAnnotation(Target.class);
        assertThat(target).isNotNull();
        assertThat(target.value()).containsExactly(ElementType.TYPE);
    }

    @Test
    void isDocumented() {
        assertThat(Checkpoint.class.getAnnotation(Documented.class)).isNotNull();
    }
}
