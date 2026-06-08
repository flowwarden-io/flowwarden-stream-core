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
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

/**
 * Contributes FlowWarden Change Stream health to {@code /actuator/health}.
 *
 * <p>Reports UP if all registered streams that are both {@code enabled} and
 * {@code autoStart} are currently running. Otherwise reports DOWN.</p>
 */
public class FlowWardenHealthIndicator implements HealthIndicator {

    private final FlowWardenStreamManager streamManager;
    private final StreamRegistry registry;

    public FlowWardenHealthIndicator(FlowWardenStreamManager streamManager,
                                     StreamRegistry registry) {
        this.streamManager = streamManager;
        this.registry = registry;
    }

    @Override
    public Health health() {
        int total = 0;
        int healthy = 0;

        for (ChangeStreamDefinition def : registry.getDefinitions()) {
            if (def.config().enabled() && def.config().autoStart()) {
                total++;
                if (streamManager.isRunning(def.streamName())) {
                    healthy++;
                }
            }
        }

        Health.Builder builder = (healthy == total) ? Health.up() : Health.down();
        return builder
                .withDetail("streams", total)
                .withDetail("healthy", healthy)
                .build();
    }
}
