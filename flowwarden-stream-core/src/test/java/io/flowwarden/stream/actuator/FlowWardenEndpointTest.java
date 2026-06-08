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

import io.flowwarden.stream.DeploymentMode;
import io.flowwarden.stream.FullDocumentBeforeChangeMode;
import io.flowwarden.stream.FullDocumentMode;
import io.flowwarden.stream.core.FlowWardenStreamManager;
import io.flowwarden.stream.internal.discovery.ChangeStreamDefinition;
import io.flowwarden.stream.internal.discovery.StreamConfig;
import io.flowwarden.stream.internal.discovery.StreamRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
class FlowWardenEndpointTest {

    private FlowWardenStreamManager streamManager;
    private StreamRegistry registry;
    private FlowWardenEndpoint endpoint;

    @BeforeEach
    void setUp() {
        streamManager = Mockito.mock(FlowWardenStreamManager.class);
        registry = Mockito.mock(StreamRegistry.class);
        endpoint = new FlowWardenEndpoint(streamManager, registry);
    }

    @Test
    void statusReturnsStreamDetailsWithModeAndLastEventTime() {
        ChangeStreamDefinition def1 = mockDefinition("stream-a", true, true, DeploymentMode.SINGLE_LEADER);
        ChangeStreamDefinition def2 = mockDefinition("stream-b", true, true, DeploymentMode.ALL_INSTANCES);
        when(registry.getDefinitions()).thenReturn(List.of(def1, def2));
        when(streamManager.isRunning("stream-a")).thenReturn(true);
        when(streamManager.isRunning("stream-b")).thenReturn(false);
        Instant now = Instant.parse("2026-04-09T14:00:00Z");
        when(streamManager.getLastEventTime("stream-a")).thenReturn(now);
        when(streamManager.getLastEventTime("stream-b")).thenReturn(null);

        Map<String, Object> result = endpoint.status();

        assertThat(result.get("healthy")).isEqualTo(false);
        Map<String, Map<String, Object>> streams = (Map<String, Map<String, Object>>) result.get("streams");
        assertThat(streams.get("stream-a").get("status")).isEqualTo("UP");
        assertThat(streams.get("stream-a").get("mode")).isEqualTo("SINGLE_LEADER");
        assertThat(streams.get("stream-a").get("lastEventTime")).isEqualTo(now.toString());
        assertThat(streams.get("stream-b").get("status")).isEqualTo("DOWN");
        assertThat(streams.get("stream-b").get("mode")).isEqualTo("ALL_INSTANCES");
        assertThat(streams.get("stream-b").get("lastEventTime")).isNull();
    }

    @Test
    void statusReportsHealthyWhenAllAutoStartStreamsAreUp() {
        ChangeStreamDefinition def1 = mockDefinition("stream-a", true, true);
        ChangeStreamDefinition def2 = mockDefinition("stream-b", true, true);
        when(registry.getDefinitions()).thenReturn(List.of(def1, def2));
        when(streamManager.isRunning("stream-a")).thenReturn(true);
        when(streamManager.isRunning("stream-b")).thenReturn(true);

        Map<String, Object> result = endpoint.status();

        assertThat(result.get("healthy")).isEqualTo(true);
    }

    @Test
    void statusReportsUnhealthyWhenAutoStartStreamIsDown() {
        ChangeStreamDefinition def = mockDefinition("stream-a", true, true);
        when(registry.getDefinitions()).thenReturn(List.of(def));
        when(streamManager.isRunning("stream-a")).thenReturn(false);

        Map<String, Object> result = endpoint.status();

        assertThat(result.get("healthy")).isEqualTo(false);
    }

    @Test
    void statusReportsHealthyWhenNonAutoStartStreamIsDown() {
        ChangeStreamDefinition def = mockDefinition("stream-a", true, false);
        when(registry.getDefinitions()).thenReturn(List.of(def));
        when(streamManager.isRunning("stream-a")).thenReturn(false);

        Map<String, Object> result = endpoint.status();

        assertThat(result.get("healthy")).isEqualTo(true);
    }

    @Test
    void manageStopCallsStopStream() {
        when(streamManager.isRunning("my-stream")).thenReturn(false);

        Map<String, Object> result = endpoint.manage("my-stream", "stop");

        verify(streamManager).stopStream("my-stream");
        assertThat(result.get("action")).isEqualTo("stopped");
        assertThat(result.get("stream")).isEqualTo("my-stream");
    }

    @Test
    void manageStartCallsStartStream() {
        when(streamManager.isRunning("my-stream")).thenReturn(true);

        Map<String, Object> result = endpoint.manage("my-stream", "start");

        verify(streamManager).startStream("my-stream");
        assertThat(result.get("action")).isEqualTo("started");
        assertThat(result.get("running")).isEqualTo(true);
    }

    @Test
    void manageRestartCallsStopThenStart() {
        when(streamManager.isRunning("my-stream")).thenReturn(true);

        Map<String, Object> result = endpoint.manage("my-stream", "restart");

        var inOrder = Mockito.inOrder(streamManager);
        inOrder.verify(streamManager).stopStream("my-stream");
        inOrder.verify(streamManager).startStream("my-stream");
        assertThat(result.get("action")).isEqualTo("restarted");
        assertThat(result.get("running")).isEqualTo(true);
    }

    @Test
    void manageUnknownActionReturnsError() {
        Map<String, Object> result = endpoint.manage("my-stream", "pause");

        assertThat(result.get("error")).asString()
                .contains("Unsupported action")
                .contains("stop, start, restart");
    }

    private ChangeStreamDefinition mockDefinition(String name, boolean enabled, boolean autoStart) {
        return mockDefinition(name, enabled, autoStart, DeploymentMode.ALL_INSTANCES);
    }

    private ChangeStreamDefinition mockDefinition(String name, boolean enabled, boolean autoStart,
                                                   DeploymentMode deploymentMode) {
        ChangeStreamDefinition def = Mockito.mock(ChangeStreamDefinition.class);
        StreamConfig config = new StreamConfig(enabled, autoStart, Object.class, "",
                FullDocumentMode.DEFAULT, FullDocumentBeforeChangeMode.OFF,
                deploymentMode);
        when(def.streamName()).thenReturn(name);
        when(def.config()).thenReturn(config);
        return def;
    }
}
