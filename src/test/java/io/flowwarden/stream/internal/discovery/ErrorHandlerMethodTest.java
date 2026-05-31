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
package io.flowwarden.stream.internal.discovery;

import io.flowwarden.stream.ChangeStreamContext;
import io.flowwarden.stream.ErrorAction;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ErrorHandlerMethodTest {

    // --- Test fixtures ---

    @SuppressWarnings("unused")
    static class Handlers {
        ErrorAction handleNpe(Throwable ex, ChangeStreamContext<?> ctx) {
            return ErrorAction.SKIP;
        }

        ErrorAction handleRuntime(Throwable ex, ChangeStreamContext<?> ctx) {
            return ErrorAction.DLQ;
        }

        ErrorAction handleCatchAll(Throwable ex, ChangeStreamContext<?> ctx) {
            return ErrorAction.RETRY;
        }

        ErrorAction throwingHandler(Throwable ex, ChangeStreamContext<?> ctx) {
            throw new RuntimeException("handler error");
        }
    }

    private ErrorHandlerMethod create(String methodName, Set<Class<? extends Throwable>> exTypes) throws Exception {
        Method m = Handlers.class.getDeclaredMethod(methodName, Throwable.class, ChangeStreamContext.class);
        m.setAccessible(true);
        return new ErrorHandlerMethod(m, exTypes);
    }

    // --- matches() ---

    @Test
    void matchesExactType() throws Exception {
        ErrorHandlerMethod handler = create("handleNpe", Set.of(NullPointerException.class));
        assertTrue(handler.matches(new NullPointerException()));
    }

    @Test
    void matchesParentType() throws Exception {
        ErrorHandlerMethod handler = create("handleRuntime", Set.of(RuntimeException.class));
        assertTrue(handler.matches(new IllegalArgumentException()));
    }

    @Test
    void matchesCatchAll() throws Exception {
        ErrorHandlerMethod handler = create("handleCatchAll", Set.of());
        assertTrue(handler.matches(new IOException()));
        assertTrue(handler.matches(new NullPointerException()));
        assertTrue(handler.matches(new Error("test")));
    }

    @Test
    void doesNotMatchUnrelatedType() throws Exception {
        ErrorHandlerMethod handler = create("handleNpe", Set.of(NullPointerException.class));
        assertFalse(handler.matches(new IOException()));
    }

    // --- specificityFor() ---

    @Test
    void specificityForExactMatch() throws Exception {
        ErrorHandlerMethod handler = create("handleNpe", Set.of(NullPointerException.class));
        assertEquals(0, handler.specificityFor(new NullPointerException()));
    }

    @Test
    void specificityForParentType() throws Exception {
        ErrorHandlerMethod handler = create("handleRuntime", Set.of(RuntimeException.class));
        int distance = handler.specificityFor(new IllegalArgumentException());
        assertTrue(distance > 0);
        assertTrue(distance < Integer.MAX_VALUE);
    }

    @Test
    void specificityForCatchAll() throws Exception {
        ErrorHandlerMethod handler = create("handleCatchAll", Set.of());
        assertEquals(Integer.MAX_VALUE, handler.specificityFor(new NullPointerException()));
    }

    // --- invoke() ---

    @Test
    void invokeReturnsCorrectAction() throws Exception {
        ErrorHandlerMethod handler = create("handleNpe", Set.of(NullPointerException.class));
        ErrorAction action = handler.invoke(new Handlers(), new NullPointerException(), null);
        assertEquals(ErrorAction.SKIP, action);
    }

    @Test
    void invokeReturnsRethrowWhenHandlerThrows() throws Exception {
        ErrorHandlerMethod handler = create("throwingHandler", Set.of(RuntimeException.class));
        ErrorAction action = handler.invoke(new Handlers(), new RuntimeException(), null);
        assertEquals(ErrorAction.RETHROW, action);
    }
}
