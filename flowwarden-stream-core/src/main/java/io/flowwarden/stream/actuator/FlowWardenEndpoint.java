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

import io.flowwarden.stream.core.FlowWardenStreamManager;
import io.flowwarden.stream.internal.discovery.ChangeStreamDefinition;
import io.flowwarden.stream.internal.discovery.StreamRegistry;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Custom Actuator endpoint exposing FlowWarden Change Stream status and lifecycle actions.
 *
 * <ul>
 *   <li>{@code GET /actuator/flowwarden} — status of all streams (status, mode, lastEventTime)</li>
 *   <li>{@code POST /actuator/flowwarden/{streamName}/stop} — stop a stream</li>
 *   <li>{@code POST /actuator/flowwarden/{streamName}/start} — start a stream</li>
 *   <li>{@code POST /actuator/flowwarden/{streamName}/restart} — restart a stream</li>
 * </ul>
 */
@Endpoint(id = "flowwarden")
public class FlowWardenEndpoint {

    private final FlowWardenStreamManager streamManager;
    private final StreamRegistry registry;

    public FlowWardenEndpoint(FlowWardenStreamManager streamManager,
                              StreamRegistry registry) {
        this.streamManager = streamManager;
        this.registry = registry;
    }

    @ReadOperation
    public Map<String, Object> status() {
        Map<String, Object> streams = new LinkedHashMap<>();
        boolean allHealthy = true;

        for (ChangeStreamDefinition def : registry.getDefinitions()) {
            String name = def.streamName();
            boolean running = streamManager.isRunning(name);

            if (!running && def.config().enabled() && def.config().autoStart()) {
                allHealthy = false;
            }

            Map<String, Object> streamInfo = new LinkedHashMap<>();
            streamInfo.put("status", running ? "UP" : "DOWN");
            streamInfo.put("mode", def.config().deploymentMode().name());
            Instant lastEvent = streamManager.getLastEventTime(name);
            streamInfo.put("lastEventTime", lastEvent != null ? lastEvent.toString() : null);

            streams.put(name, streamInfo);
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("streams", streams);
        result.put("healthy", allHealthy);
        return result;
    }

    @WriteOperation
    public Map<String, Object> manage(@Selector String streamName, @Selector String action) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("stream", streamName);

        switch (action) {
            case "stop" -> {
                streamManager.stopStream(streamName);
                result.put("action", "stopped");
                result.put("running", streamManager.isRunning(streamName));
            }
            case "start" -> {
                streamManager.startStream(streamName);
                result.put("action", "started");
                result.put("running", streamManager.isRunning(streamName));
            }
            case "restart" -> {
                streamManager.stopStream(streamName);
                streamManager.startStream(streamName);
                result.put("action", "restarted");
                result.put("running", streamManager.isRunning(streamName));
            }
            default -> result.put("error", "Unsupported action '" + action
                    + "'. Available actions: stop, start, restart.");
        }

        return result;
    }
}
