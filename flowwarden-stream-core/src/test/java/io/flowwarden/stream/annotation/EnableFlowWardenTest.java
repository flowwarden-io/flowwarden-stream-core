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

import java.lang.annotation.ElementType;
import java.lang.annotation.RetentionPolicy;

import static org.junit.jupiter.api.Assertions.*;

class EnableFlowWardenTest {

    @EnableFlowWarden
    static class DefaultConfig {}

    @EnableFlowWarden(enabled = false)
    static class CustomConfig {}

    @Test
    void isRuntimeRetention() {
        assertEquals(RetentionPolicy.RUNTIME,
                EnableFlowWarden.class.getAnnotation(java.lang.annotation.Retention.class).value());
    }

    @Test
    void targetsType() {
        var target = EnableFlowWarden.class.getAnnotation(java.lang.annotation.Target.class);
        assertArrayEquals(new ElementType[]{ElementType.TYPE}, target.value());
    }

    @Test
    void defaultValues() {
        EnableFlowWarden ann = DefaultConfig.class.getAnnotation(EnableFlowWarden.class);
        assertNotNull(ann);
        assertEquals("flowwarden.stream", ann.propertyPrefix());
        assertTrue(ann.enabled());
    }

    @Test
    void customValues() {
        EnableFlowWarden ann = CustomConfig.class.getAnnotation(EnableFlowWarden.class);
        assertNotNull(ann);
        assertFalse(ann.enabled());
    }
}
