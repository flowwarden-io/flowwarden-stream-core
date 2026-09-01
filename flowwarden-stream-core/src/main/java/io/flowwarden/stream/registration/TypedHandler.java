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

import io.flowwarden.stream.core.ContextHandler;
import io.flowwarden.stream.core.DocumentHandler;
import io.flowwarden.stream.core.ReactiveContextHandler;
import io.flowwarden.stream.core.ReactiveDocumentHandler;

import java.util.Objects;

/**
 * One of the four handler shapes a {@link StreamSpec} can bind to an operation: with or
 * without the deserialized document, imperative or reactive. Exactly one of the four
 * subtypes is ever produced by {@link StreamSpec.Builder}.
 *
 * <p>Each variant rejects a {@code null} handler in its compact constructor, so a mistake
 * fails immediately at the {@code StreamSpec.Builder} call site — not later, deep inside the
 * internal conversion to {@code HandlerMethod}.</p>
 *
 * @param <T> the document type
 */
public sealed interface TypedHandler<T> {

    record Context<T>(ContextHandler<T> handler) implements TypedHandler<T> {
        public Context {
            Objects.requireNonNull(handler, "handler must not be null");
        }
    }

    record Document<T>(DocumentHandler<T> handler) implements TypedHandler<T> {
        public Document {
            Objects.requireNonNull(handler, "handler must not be null");
        }
    }

    record ReactiveContext<T>(ReactiveContextHandler<T> handler) implements TypedHandler<T> {
        public ReactiveContext {
            Objects.requireNonNull(handler, "handler must not be null");
        }
    }

    record ReactiveDocument<T>(ReactiveDocumentHandler<T> handler) implements TypedHandler<T> {
        public ReactiveDocument {
            Objects.requireNonNull(handler, "handler must not be null");
        }
    }
}
