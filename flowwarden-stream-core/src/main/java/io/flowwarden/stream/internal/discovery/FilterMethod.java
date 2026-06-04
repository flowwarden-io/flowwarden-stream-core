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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Encapsulates a {@code @Filter}-annotated method and its invocation strategy.
 *
 * <p>The method is invoked at each event to decide whether the event should be
 * forwarded to handler methods. Two return styles are supported:</p>
 * <ul>
 *   <li>{@link ReturnStyle#PREDICATE} — method has no parameters and returns
 *       {@code Predicate<ChangeStreamContext<?>>}</li>
 *   <li>{@link ReturnStyle#BOOLEAN_METHOD} — method takes one {@code ChangeStreamContext<?>}
 *       parameter and returns {@code boolean}</li>
 * </ul>
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public final class FilterMethod {

    /**
     * Describes the signature style of the {@code @Filter} method.
     */
    public enum ReturnStyle {
        /** {@code Predicate<ChangeStreamContext<?>>} — no parameters, returns a predicate */
        PREDICATE,
        /** {@code boolean method(ChangeStreamContext<?>)} — direct boolean evaluation */
        BOOLEAN_METHOD
    }

    private final Method method;
    private final ReturnStyle returnStyle;

    public FilterMethod(Method method, ReturnStyle returnStyle) {
        this.method = Objects.requireNonNull(method, "method must not be null");
        this.returnStyle = Objects.requireNonNull(returnStyle, "returnStyle must not be null");
    }

    public Method method() {
        return method;
    }

    public ReturnStyle returnStyle() {
        return returnStyle;
    }

    /**
     * Evaluates the filter for the given event context.
     *
     * @param bean the target bean instance
     * @param ctx  the change stream context for the current event
     * @return {@code true} if the event passes the filter and should be processed,
     *         {@code false} if it should be skipped
     * @throws IllegalStateException if the filter method fails
     */
    @SuppressWarnings("unchecked")
    public boolean evaluate(Object bean, ChangeStreamContext<?> ctx) {
        try {
            return switch (returnStyle) {
                case PREDICATE -> {
                    Predicate<ChangeStreamContext<?>> predicate =
                            (Predicate<ChangeStreamContext<?>>) method.invoke(bean);
                    if (predicate == null) {
                        throw new IllegalStateException(
                                "@Filter method " + method.getDeclaringClass().getSimpleName()
                                        + "#" + method.getName() + " returned null");
                    }
                    yield predicate.test(ctx);
                }
                case BOOLEAN_METHOD -> (boolean) method.invoke(bean, ctx);
            };
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new IllegalStateException(
                    "@Filter method " + method.getDeclaringClass().getSimpleName()
                            + "#" + method.getName() + " threw an exception", cause);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(
                    "Cannot invoke @Filter method " + method.getDeclaringClass().getSimpleName()
                            + "#" + method.getName(), e);
        }
    }

    @Override
    public String toString() {
        return method.getDeclaringClass().getSimpleName() + "#" + method.getName()
                + "(" + returnStyle + ")";
    }
}
