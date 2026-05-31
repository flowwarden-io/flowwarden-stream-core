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

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Resolves the best {@link ErrorHandlerMethod} for a given exception.
 *
 * <p>Resolution priority: exact type match → parent type match → catch-all.</p>
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public final class ErrorHandlerResolver {

    private final List<ErrorHandlerMethod> handlers;

    public ErrorHandlerResolver(List<ErrorHandlerMethod> handlers) {
        this.handlers = Objects.requireNonNull(handlers, "handlers must not be null");
    }

    /**
     * Finds the most specific handler matching the given exception.
     */
    public Optional<ErrorHandlerMethod> resolve(Throwable ex) {
        return handlers.stream()
                .filter(h -> h.matches(ex))
                .min(Comparator.comparingInt(h -> h.specificityFor(ex)));
    }

    /**
     * Resolves the best handler and invokes it, returning the {@link ErrorAction}.
     */
    public Optional<ErrorAction> resolveAndInvoke(Object bean, Throwable ex, ChangeStreamContext<?> ctx) {
        return resolve(ex).map(handler -> handler.invoke(bean, ex, ctx));
    }

    /**
     * Returns {@code true} if no error handlers are registered.
     */
    public boolean isEmpty() {
        return handlers.isEmpty();
    }
}
