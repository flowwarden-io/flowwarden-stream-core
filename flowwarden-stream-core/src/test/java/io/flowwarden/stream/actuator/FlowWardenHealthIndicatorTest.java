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
package io.flowwarden.stream.actuator;

import io.flowwarden.stream.FullDocumentBeforeChangeMode;
import io.flowwarden.stream.FullDocumentMode;
import io.flowwarden.stream.core.FlowWardenStreamManager;
import io.flowwarden.stream.internal.discovery.ChangeStreamDefinition;
import io.flowwarden.stream.internal.discovery.StreamConfig;
import io.flowwarden.stream.internal.discovery.StreamRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

class FlowWardenHealthIndicatorTest {

    private FlowWardenStreamManager streamManager;
    private StreamRegistry registry;
    private FlowWardenHealthIndicator indicator;

    @BeforeEach
    void setUp() {
        streamManager = Mockito.mock(FlowWardenStreamManager.class);
        registry = Mockito.mock(StreamRegistry.class);
        indicator = new FlowWardenHealthIndicator(streamManager, registry);
    }

    @Test
    void allStreamsRunningReportsUp() {
        ChangeStreamDefinition def1 = mockDefinition("s1", true, true);
        ChangeStreamDefinition def2 = mockDefinition("s2", true, true);
        when(registry.getDefinitions()).thenReturn(List.of(def1, def2));
        when(streamManager.isRunning("s1")).thenReturn(true);
        when(streamManager.isRunning("s2")).thenReturn(true);

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails().get("streams")).isEqualTo(2);
        assertThat(health.getDetails().get("healthy")).isEqualTo(2);
    }

    @Test
    void oneStreamDownReportsDown() {
        ChangeStreamDefinition def1 = mockDefinition("s1", true, true);
        ChangeStreamDefinition def2 = mockDefinition("s2", true, true);
        when(registry.getDefinitions()).thenReturn(List.of(def1, def2));
        when(streamManager.isRunning("s1")).thenReturn(true);
        when(streamManager.isRunning("s2")).thenReturn(false);

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails().get("streams")).isEqualTo(2);
        assertThat(health.getDetails().get("healthy")).isEqualTo(1);
    }

    @Test
    void noStreamsReportsUp() {
        when(registry.getDefinitions()).thenReturn(Collections.emptyList());

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails().get("streams")).isEqualTo(0);
        assertThat(health.getDetails().get("healthy")).isEqualTo(0);
    }

    @Test
    void disabledStreamIsNotCounted() {
        ChangeStreamDefinition def = mockDefinition("s1", false, true);
        when(registry.getDefinitions()).thenReturn(List.of(def));

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails().get("streams")).isEqualTo(0);
    }

    @Test
    void nonAutoStartStreamIsNotCounted() {
        ChangeStreamDefinition def = mockDefinition("s1", true, false);
        when(registry.getDefinitions()).thenReturn(List.of(def));
        when(streamManager.isRunning("s1")).thenReturn(false);

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails().get("streams")).isEqualTo(0);
    }

    private ChangeStreamDefinition mockDefinition(String name, boolean enabled, boolean autoStart) {
        ChangeStreamDefinition def = Mockito.mock(ChangeStreamDefinition.class);
        StreamConfig config = new StreamConfig(enabled, autoStart, Object.class, "",
                FullDocumentMode.DEFAULT, FullDocumentBeforeChangeMode.OFF,
                io.flowwarden.stream.DeploymentMode.ALL_INSTANCES);
        when(def.streamName()).thenReturn(name);
        when(def.config()).thenReturn(config);
        return def;
    }
}
