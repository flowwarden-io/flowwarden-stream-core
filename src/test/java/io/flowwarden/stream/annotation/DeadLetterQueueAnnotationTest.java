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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static org.assertj.core.api.Assertions.assertThat;

class DeadLetterQueueAnnotationTest {

    @DeadLetterQueue
    static class DefaultDlqHandler {
    }

    @DeadLetterQueue(
            enabled = false,
            collection = "custom_dlq",
            ttlDays = 90,
            includeOriginalDocument = false,
            includeStackTrace = false
    )
    static class CustomDlqHandler {
    }

    @Test
    void defaultValues() {
        DeadLetterQueue ann = DefaultDlqHandler.class.getAnnotation(DeadLetterQueue.class);
        assertThat(ann).isNotNull();
        assertThat(ann.enabled()).isTrue();
        assertThat(ann.collection()).isEqualTo("_fw_dlq");
        assertThat(ann.ttlDays()).isEqualTo(30);
        assertThat(ann.includeOriginalDocument()).isTrue();
        assertThat(ann.includeStackTrace()).isTrue();
    }

    @Test
    void customValues() {
        DeadLetterQueue ann = CustomDlqHandler.class.getAnnotation(DeadLetterQueue.class);
        assertThat(ann).isNotNull();
        assertThat(ann.enabled()).isFalse();
        assertThat(ann.collection()).isEqualTo("custom_dlq");
        assertThat(ann.ttlDays()).isEqualTo(90);
        assertThat(ann.includeOriginalDocument()).isFalse();
        assertThat(ann.includeStackTrace()).isFalse();
    }

    @Test
    void retentionIsRuntime() {
        Retention retention = DeadLetterQueue.class.getAnnotation(Retention.class);
        assertThat(retention).isNotNull();
        assertThat(retention.value()).isEqualTo(RetentionPolicy.RUNTIME);
    }

    @Test
    void targetIsType() {
        Target target = DeadLetterQueue.class.getAnnotation(Target.class);
        assertThat(target).isNotNull();
        assertThat(target.value()).containsExactly(ElementType.TYPE);
    }

    @Test
    void isDocumented() {
        assertThat(DeadLetterQueue.class.getAnnotation(Documented.class)).isNotNull();
    }
}
