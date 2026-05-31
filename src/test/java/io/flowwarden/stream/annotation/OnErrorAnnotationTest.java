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

import java.io.IOException;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

class OnErrorAnnotationTest {

    static class CatchAllHandler {
        @OnError
        void handle() {}
    }

    static class SpecificHandler {
        @OnError({NullPointerException.class, IOException.class})
        void handle() {}
    }

    @Test
    void isRuntimeRetention() {
        assertEquals(RetentionPolicy.RUNTIME,
                OnError.class.getAnnotation(java.lang.annotation.Retention.class).value());
    }

    @Test
    void targetsMethod() {
        var target = OnError.class.getAnnotation(java.lang.annotation.Target.class);
        assertArrayEquals(new ElementType[]{ElementType.METHOD}, target.value());
    }

    @Test
    void isDocumented() {
        assertNotNull(OnError.class.getAnnotation(Documented.class));
    }

    @Test
    void defaultValueIsEmptyArray() throws Exception {
        Method m = CatchAllHandler.class.getDeclaredMethod("handle");
        OnError ann = m.getAnnotation(OnError.class);
        assertNotNull(ann);
        assertEquals(0, ann.value().length);
    }

    @Test
    void customValueContainsExceptionClasses() throws Exception {
        Method m = SpecificHandler.class.getDeclaredMethod("handle");
        OnError ann = m.getAnnotation(OnError.class);
        assertNotNull(ann);
        assertEquals(2, ann.value().length);
        assertEquals(NullPointerException.class, ann.value()[0]);
        assertEquals(IOException.class, ann.value()[1]);
    }

    @Test
    void isPresentAtRuntime() throws Exception {
        Method m = CatchAllHandler.class.getDeclaredMethod("handle");
        assertNotNull(m.getAnnotation(OnError.class));
    }
}
