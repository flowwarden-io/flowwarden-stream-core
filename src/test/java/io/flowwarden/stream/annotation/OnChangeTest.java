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

import io.flowwarden.stream.OperationType;
import org.junit.jupiter.api.Test;

import java.lang.annotation.ElementType;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

class OnChangeTest {

    static class Handler {
        @OnChange
        void handleAll() {}

        @OnChange(operationTypes = {OperationType.INSERT, OperationType.UPDATE})
        void handleFiltered() {}
    }

    @Test
    void isRuntimeRetention() {
        assertEquals(RetentionPolicy.RUNTIME,
                OnChange.class.getAnnotation(java.lang.annotation.Retention.class).value());
    }

    @Test
    void targetsMethod() {
        var target = OnChange.class.getAnnotation(java.lang.annotation.Target.class);
        assertArrayEquals(new ElementType[]{ElementType.METHOD}, target.value());
    }

    @Test
    void defaultOperationTypesIsEmpty() throws Exception {
        Method m = Handler.class.getDeclaredMethod("handleAll");
        OnChange ann = m.getAnnotation(OnChange.class);
        assertNotNull(ann);
        assertArrayEquals(new OperationType[]{}, ann.operationTypes());
    }

    @Test
    void customOperationTypes() throws Exception {
        Method m = Handler.class.getDeclaredMethod("handleFiltered");
        OnChange ann = m.getAnnotation(OnChange.class);
        assertNotNull(ann);
        assertArrayEquals(
                new OperationType[]{OperationType.INSERT, OperationType.UPDATE},
                ann.operationTypes()
        );
    }
}
