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

import io.flowwarden.stream.FullDocumentBeforeChangeMode;
import io.flowwarden.stream.FullDocumentMode;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class StreamRegistryTest {

    private ChangeStreamBeanPostProcessor registry;

    @BeforeEach
    void setUp() {
        registry = new ChangeStreamBeanPostProcessor();
    }

    @Test
    void registerAndGetDefinitions() {
        ChangeStreamDefinition def = createDefinition("test-stream");
        registry.register(def);

        assertEquals(1, registry.getDefinitions().size());
        assertSame(def, registry.getDefinitions().get(0));
    }

    @Test
    void findByNameReturnsPresent() {
        ChangeStreamDefinition def = createDefinition("my-stream");
        registry.register(def);

        Optional<ChangeStreamDefinition> found = registry.findByName("my-stream");
        assertTrue(found.isPresent());
        assertSame(def, found.get());
    }

    @Test
    void findByNameReturnsEmptyForUnknown() {
        ChangeStreamDefinition def = createDefinition("my-stream");
        registry.register(def);

        Optional<ChangeStreamDefinition> found = registry.findByName("unknown");
        assertTrue(found.isEmpty());
    }

    @Test
    void getStreamNamesFollowsRegistrationOrder() {
        registry.register(createDefinition("zeta"));
        registry.register(createDefinition("alpha"));
        registry.register(createDefinition("mid"));

        assertEquals(List.of("zeta", "alpha", "mid"), List.copyOf(registry.getStreamNames()));
    }

    @Test
    void getStreamNamesIsEmptyBeforeAnyRegistration() {
        assertTrue(registry.getStreamNames().isEmpty());
    }

    @Test
    void getStreamNamesIsAnUnmodifiableSnapshot() {
        registry.register(createDefinition("first"));
        Set<String> names = registry.getStreamNames();

        registry.register(createDefinition("second"));

        assertEquals(Set.of("first"), names, "a snapshot must not follow later registrations");
        assertThrows(UnsupportedOperationException.class, () -> names.add("intruder"));
        assertEquals(List.of("first", "second"), List.copyOf(registry.getStreamNames()));
    }

    private ChangeStreamDefinition createDefinition(String name) {
        return new ChangeStreamDefinition(
                name,
                "orders",
                "",
                "",
                new Object(),
                null,
                Collections.emptyMap(),
                new StreamConfig(true, true, Document.class, "",
                        FullDocumentMode.DEFAULT, FullDocumentBeforeChangeMode.OFF,
                        io.flowwarden.stream.DeploymentMode.ALL_INSTANCES),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                new ErrorHandlerResolver(List.of()),
                Collections.emptyMap());
    }
}
