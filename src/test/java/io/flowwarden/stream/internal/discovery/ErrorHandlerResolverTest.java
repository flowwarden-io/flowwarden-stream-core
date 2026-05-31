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
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ErrorHandlerResolverTest {

    @SuppressWarnings("unused")
    static class Handlers {
        ErrorAction handleNpe(Throwable ex, ChangeStreamContext<?> ctx) {
            return ErrorAction.SKIP;
        }

        ErrorAction handleRuntime(Throwable ex, ChangeStreamContext<?> ctx) {
            return ErrorAction.RETRY;
        }

        ErrorAction handleCatchAll(Throwable ex, ChangeStreamContext<?> ctx) {
            return ErrorAction.DLQ;
        }
    }

    private ErrorHandlerMethod create(String methodName, Set<Class<? extends Throwable>> exTypes) throws Exception {
        Method m = Handlers.class.getDeclaredMethod(methodName, Throwable.class, ChangeStreamContext.class);
        m.setAccessible(true);
        return new ErrorHandlerMethod(m, exTypes);
    }

    @Test
    void exactTypeWinsOverParentAndCatchAll() throws Exception {
        ErrorHandlerMethod npeHandler = create("handleNpe", Set.of(NullPointerException.class));
        ErrorHandlerMethod runtimeHandler = create("handleRuntime", Set.of(RuntimeException.class));
        ErrorHandlerMethod catchAllHandler = create("handleCatchAll", Set.of());

        ErrorHandlerResolver resolver = new ErrorHandlerResolver(
                List.of(catchAllHandler, runtimeHandler, npeHandler));

        Optional<ErrorHandlerMethod> result = resolver.resolve(new NullPointerException());
        assertTrue(result.isPresent());
        assertEquals("handleNpe", result.get().method().getName());
    }

    @Test
    void parentTypeWinsOverCatchAll() throws Exception {
        ErrorHandlerMethod runtimeHandler = create("handleRuntime", Set.of(RuntimeException.class));
        ErrorHandlerMethod catchAllHandler = create("handleCatchAll", Set.of());

        ErrorHandlerResolver resolver = new ErrorHandlerResolver(
                List.of(catchAllHandler, runtimeHandler));

        Optional<ErrorHandlerMethod> result = resolver.resolve(new IllegalArgumentException());
        assertTrue(result.isPresent());
        assertEquals("handleRuntime", result.get().method().getName());
    }

    @Test
    void catchAllMatchesWhenNoSpecificHandler() throws Exception {
        ErrorHandlerMethod npeHandler = create("handleNpe", Set.of(NullPointerException.class));
        ErrorHandlerMethod catchAllHandler = create("handleCatchAll", Set.of());

        ErrorHandlerResolver resolver = new ErrorHandlerResolver(
                List.of(npeHandler, catchAllHandler));

        Optional<ErrorHandlerMethod> result = resolver.resolve(new IOException());
        assertTrue(result.isPresent());
        assertEquals("handleCatchAll", result.get().method().getName());
    }

    @Test
    void noMatchReturnsEmpty() throws Exception {
        ErrorHandlerMethod npeHandler = create("handleNpe", Set.of(NullPointerException.class));

        ErrorHandlerResolver resolver = new ErrorHandlerResolver(List.of(npeHandler));

        Optional<ErrorHandlerMethod> result = resolver.resolve(new IOException());
        assertTrue(result.isEmpty());
    }

    @Test
    void emptyResolverReturnsEmpty() {
        ErrorHandlerResolver resolver = new ErrorHandlerResolver(List.of());
        assertTrue(resolver.isEmpty());
        assertTrue(resolver.resolve(new RuntimeException()).isEmpty());
    }

    @Test
    void resolveAndInvokeReturnsAction() throws Exception {
        ErrorHandlerMethod npeHandler = create("handleNpe", Set.of(NullPointerException.class));
        ErrorHandlerResolver resolver = new ErrorHandlerResolver(List.of(npeHandler));

        Optional<ErrorAction> action = resolver.resolveAndInvoke(
                new Handlers(), new NullPointerException(), null);
        assertTrue(action.isPresent());
        assertEquals(ErrorAction.SKIP, action.get());
    }

    @Test
    void resolveAndInvokeReturnsEmptyForNoMatch() throws Exception {
        ErrorHandlerMethod npeHandler = create("handleNpe", Set.of(NullPointerException.class));
        ErrorHandlerResolver resolver = new ErrorHandlerResolver(List.of(npeHandler));

        Optional<ErrorAction> action = resolver.resolveAndInvoke(
                new Handlers(), new IOException(), null);
        assertTrue(action.isEmpty());
    }
}
