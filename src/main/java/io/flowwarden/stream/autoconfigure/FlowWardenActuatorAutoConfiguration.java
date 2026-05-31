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
package io.flowwarden.stream.autoconfigure;

import io.flowwarden.stream.actuator.FlowWardenEndpoint;
import io.flowwarden.stream.actuator.FlowWardenHealthIndicator;
import io.flowwarden.stream.core.FlowWardenStreamManager;
import io.flowwarden.stream.internal.discovery.StreamRegistry;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.actuate.autoconfigure.health.ConditionalOnEnabledHealthIndicator;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configuration for FlowWarden Actuator support.
 *
 * <p>Only activates when Spring Boot Actuator is on the classpath
 * ({@code @ConditionalOnClass(Endpoint.class)}).</p>
 */
@AutoConfiguration(after = FlowWardenAutoConfiguration.class)
@ConditionalOnClass(Endpoint.class)
public class FlowWardenActuatorAutoConfiguration {

    @Bean
    public FlowWardenEndpoint flowWardenEndpoint(FlowWardenStreamManager streamManager,
                                                  StreamRegistry registry) {
        return new FlowWardenEndpoint(streamManager, registry);
    }

    @Bean
    @ConditionalOnEnabledHealthIndicator("flowwarden")
    public FlowWardenHealthIndicator flowWardenHealthIndicator(FlowWardenStreamManager streamManager,
                                                                StreamRegistry registry) {
        return new FlowWardenHealthIndicator(streamManager, registry);
    }
}
