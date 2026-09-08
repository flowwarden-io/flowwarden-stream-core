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
package io.flowwarden.stream.core;

import io.flowwarden.stream.ChangeStreamContext;
import io.flowwarden.stream.ErrorAction;

/**
 * Functional interface for handlers that decide how to react to a handler exception,
 * mirroring {@code @OnError}.
 */
@FunctionalInterface
public interface ErrorHandler {

    /**
     * Decides how FlowWarden should proceed after a handler threw {@code ex}.
     *
     * @param ex  the exception thrown by the handler
     * @param ctx the change stream context of the event being processed
     * @return the action to take
     */
    ErrorAction handle(Throwable ex, ChangeStreamContext<?> ctx);
}
