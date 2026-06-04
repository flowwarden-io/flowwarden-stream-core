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

import java.util.List;
import java.util.Optional;

/**
 * Registry of {@link ChangeStreamDefinition} instances.
 *
 * <p>Abstracts how definitions are discovered (annotation scanning, builder API, etc.)
 * so that stream managers depend on the registry interface rather than a concrete
 * discovery mechanism.</p>
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public interface StreamRegistry {

    /**
     * Returns all registered stream definitions (unmodifiable).
     */
    List<ChangeStreamDefinition> getDefinitions();

    /**
     * Registers a new stream definition.
     *
     * @param definition the definition to register
     */
    void register(ChangeStreamDefinition definition);

    /**
     * Finds a stream definition by its unique name.
     *
     * @param streamName the stream name
     * @return the definition, or empty if not found
     */
    Optional<ChangeStreamDefinition> findByName(String streamName);
}
