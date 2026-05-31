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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates an {@code @OnError}-annotated method and its invocation strategy.
 *
 * <p>The method is invoked when a handler throws an exception, before the standard
 * retry/DLQ chain. It must return an {@link ErrorAction}.</p>
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public final class ErrorHandlerMethod {

    private static final Logger log = LoggerFactory.getLogger(ErrorHandlerMethod.class);

    private final Method method;
    private final Set<Class<? extends Throwable>> exceptionTypes;

    public ErrorHandlerMethod(Method method, Set<Class<? extends Throwable>> exceptionTypes) {
        this.method = Objects.requireNonNull(method, "method must not be null");
        this.exceptionTypes = Objects.requireNonNull(exceptionTypes, "exceptionTypes must not be null");
    }

    public Method method() {
        return method;
    }

    public Set<Class<? extends Throwable>> exceptionTypes() {
        return exceptionTypes;
    }

    /**
     * Returns {@code true} if this handler matches the given exception.
     * A catch-all (empty exceptionTypes) matches everything.
     */
    public boolean matches(Throwable ex) {
        if (exceptionTypes.isEmpty()) {
            return true; // catch-all
        }
        for (Class<? extends Throwable> type : exceptionTypes) {
            if (type.isInstance(ex)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns a specificity score for the given exception.
     * <ul>
     *   <li>0 = exact type match</li>
     *   <li>N = distance in the class hierarchy</li>
     *   <li>{@link Integer#MAX_VALUE} = catch-all</li>
     * </ul>
     */
    public int specificityFor(Throwable ex) {
        if (exceptionTypes.isEmpty()) {
            return Integer.MAX_VALUE; // catch-all is least specific
        }
        int best = Integer.MAX_VALUE;
        for (Class<? extends Throwable> type : exceptionTypes) {
            if (type.isInstance(ex)) {
                int distance = computeDistance(ex.getClass(), type);
                if (distance < best) {
                    best = distance;
                }
            }
        }
        return best;
    }

    /**
     * Invokes the error handler method.
     * If the handler itself throws, logs the error and returns {@link ErrorAction#RETHROW} as safe fallback.
     */
    public ErrorAction invoke(Object bean, Throwable ex, ChangeStreamContext<?> ctx) {
        try {
            return (ErrorAction) method.invoke(bean, ex, ctx);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            log.error("@OnError handler {}#{} threw an exception — falling back to RETHROW",
                    method.getDeclaringClass().getSimpleName(), method.getName(), cause);
            return ErrorAction.RETHROW;
        } catch (IllegalAccessException e) {
            log.error("Cannot invoke @OnError handler {}#{}",
                    method.getDeclaringClass().getSimpleName(), method.getName(), e);
            return ErrorAction.RETHROW;
        }
    }

    private static int computeDistance(Class<?> actual, Class<?> target) {
        int distance = 0;
        Class<?> current = actual;
        while (current != null) {
            if (current == target) {
                return distance;
            }
            current = current.getSuperclass();
            distance++;
        }
        // Should not happen if isInstance was true, but fallback
        return Integer.MAX_VALUE - 1;
    }

    @Override
    public String toString() {
        return method.getDeclaringClass().getSimpleName() + "#" + method.getName()
                + "(exceptions=" + (exceptionTypes.isEmpty() ? "catch-all" : exceptionTypes) + ")";
    }
}
