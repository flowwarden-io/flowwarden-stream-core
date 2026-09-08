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

import java.util.ArrayList;
import java.util.List;

/**
 * Collects the {@link StreamSpec} builders declared by a {@link StreamDefinitionContributor}.
 *
 * <p>One instance is passed to each contributor's {@code contribute} call; the FlowWarden
 * runtime reads back {@link #streams()} once every contributor has run.</p>
 */
public final class StreamRegistration {

    private final List<StreamSpec.Builder<?>> builders = new ArrayList<>();

    /**
     * Starts declaring a stream. The returned builder is tracked by this registration — no
     * separate call is needed to register it.
     *
     * @param name         unique stream name
     * @param documentType the Java type for document deserialization
     * @param <T>          the document type
     */
    public <T> StreamSpec.Builder<T> stream(String name, Class<T> documentType) {
        StreamSpec.Builder<T> builder = StreamSpec.builder(name, documentType);
        builders.add(builder);
        return builder;
    }

    /** Builds and returns every stream declared on this registration so far. */
    public List<StreamSpec<?>> streams() {
        return builders.stream().<StreamSpec<?>>map(StreamSpec.Builder::build).toList();
    }
}
