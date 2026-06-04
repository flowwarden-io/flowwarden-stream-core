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
import reactor.core.publisher.Mono;

/**
 * Functional interface for reactive handlers that receive
 * only the change stream context (no document).
 *
 * @param <T> the document type (used for type-safety at registration time)
 */
@FunctionalInterface
public interface ReactiveContextHandler<T> {

    /**
     * Handles a change stream event reactively.
     *
     * @param ctx the change stream context
     * @return a {@link Mono} that completes when the event is processed
     */
    Mono<Void> handle(ChangeStreamContext<?> ctx);
}
