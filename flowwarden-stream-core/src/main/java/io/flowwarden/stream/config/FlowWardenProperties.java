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
package io.flowwarden.stream.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for FlowWarden.
 *
 * <p>The {@code default-mode} property defaults to {@code IMPERATIVE} when not
 * explicitly configured. Set it to {@code REACTIVE} for Spring WebFlux applications.</p>
 */
@ConfigurationProperties(prefix = "flowwarden")
public class FlowWardenProperties {

    private ExecutionMode defaultMode = ExecutionMode.IMPERATIVE;

    /**
     * Returns the configured execution mode (IMPERATIVE or REACTIVE).
     */
    public ExecutionMode getDefaultMode() {
        return defaultMode;
    }

    public void setDefaultMode(ExecutionMode defaultMode) {
        this.defaultMode = defaultMode;
    }
}
