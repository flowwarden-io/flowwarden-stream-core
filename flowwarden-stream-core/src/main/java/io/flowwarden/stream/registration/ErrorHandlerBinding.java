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
package io.flowwarden.stream.registration;

import io.flowwarden.stream.core.ErrorHandler;

import java.util.Objects;
import java.util.Set;

/**
 * One {@code onError} registration on a {@link StreamSpec}: an {@link ErrorHandler} and the
 * exception types it handles — an empty set means catch-all, mirroring {@code @OnError}'s
 * {@code value()} attribute.
 */
public record ErrorHandlerBinding(Set<Class<? extends Throwable>> exceptionTypes, ErrorHandler handler) {

    public ErrorHandlerBinding {
        Objects.requireNonNull(exceptionTypes, "exceptionTypes must not be null");
        Objects.requireNonNull(handler, "handler must not be null");
        exceptionTypes = Set.copyOf(exceptionTypes);
    }

    /** {@code true} if this binding is a catch-all (no specific exception type). */
    public boolean isCatchAll() {
        return exceptionTypes.isEmpty();
    }
}
