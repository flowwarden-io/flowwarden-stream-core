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

import io.flowwarden.stream.DeploymentMode;
import io.flowwarden.stream.FullDocumentBeforeChangeMode;
import io.flowwarden.stream.FullDocumentMode;
import io.flowwarden.stream.internal.MongoTemplateRegistry;
import io.flowwarden.stream.internal.discovery.ChangeStreamBeanPostProcessor;
import io.flowwarden.stream.internal.discovery.ChangeStreamDefinition;
import io.flowwarden.stream.internal.discovery.ErrorHandlerResolver;
import io.flowwarden.stream.internal.discovery.StreamConfig;
import io.flowwarden.stream.internal.discovery.StreamRegistry;
import io.flowwarden.stream.internal.imperative.ImperativeStreamManager;
import io.flowwarden.stream.internal.reactive.ReactiveStreamManager;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.DlqStore;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * {@link FlowWardenStreamManager#getStreamNames()}: the registered names, in
 * registration order, readable from both managers without any stream having
 * been started — nothing here opens a cursor or publishes
 * {@code ApplicationReadyEvent}.
 */
class FlowWardenStreamManagerStreamNamesTest {

    @Test
    void defaultImplementation_returnsAnEmptySet() {
        FlowWardenStreamManager custom = new FlowWardenStreamManager() {
            @Override
            public void startStream(String streamName) {
            }

            @Override
            public void stopStream(String streamName) {
            }

            @Override
            public boolean isRunning(String streamName) {
                return false;
            }
        };

        assertThat(custom.getStreamNames()).isEmpty();
    }

    @Test
    void imperativeManager_listsRegisteredStreams_withoutStartingAny() {
        StreamRegistry registry = registryOf("orders", "payments", "audit");
        ImperativeStreamManager manager = new ImperativeStreamManager(
                mock(MongoTemplateRegistry.class), registry, CheckpointStore.noOp(), DlqStore.noOp(), null);
        try {
            assertListsRegisteredStreams(manager, registry);
        } finally {
            manager.shutdown();
        }
    }

    @Test
    void reactiveManager_listsRegisteredStreams_withoutStartingAny() {
        StreamRegistry registry = registryOf("orders", "payments", "audit");
        ReactiveStreamManager manager = new ReactiveStreamManager(
                mock(MongoTemplateRegistry.class), registry, CheckpointStore.noOp(), DlqStore.noOp(), null);
        try {
            assertListsRegisteredStreams(manager, registry);
        } finally {
            manager.shutdown();
        }
    }

    private static void assertListsRegisteredStreams(FlowWardenStreamManager manager, StreamRegistry registry) {
        Set<String> names = manager.getStreamNames();

        assertThat(names).containsExactly("orders", "payments", "audit");
        assertThat(names).noneMatch(manager::isRunning);
        assertThatThrownBy(() -> names.add("intruder")).isInstanceOf(UnsupportedOperationException.class);

        // A snapshot: a later registration shows in the next call, not in this one.
        registry.register(definition("late"));
        assertThat(names).containsExactly("orders", "payments", "audit");
        assertThat(manager.getStreamNames()).containsExactly("orders", "payments", "audit", "late");
    }

    private static StreamRegistry registryOf(String... names) {
        StreamRegistry registry = new ChangeStreamBeanPostProcessor();
        for (String name : names) {
            registry.register(definition(name));
        }
        return registry;
    }

    private static ChangeStreamDefinition definition(String name) {
        return new ChangeStreamDefinition(
                name, "orders", "", "", new Object(), null, Collections.emptyMap(),
                new StreamConfig(true, true, Document.class, "",
                        FullDocumentMode.DEFAULT, FullDocumentBeforeChangeMode.OFF,
                        DeploymentMode.ALL_INSTANCES),
                null, null, null, null, null, null, null,
                new ErrorHandlerResolver(List.of()), Collections.emptyMap());
    }
}
