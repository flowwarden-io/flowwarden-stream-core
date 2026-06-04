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

import io.flowwarden.stream.config.FlowWardenProperties;
import io.flowwarden.stream.internal.discovery.ChangeStreamBeanPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

/**
 * Common auto-configuration for FlowWarden (applies regardless of execution mode).
 *
 * <p>The mode-specific {@link io.flowwarden.stream.spi.CheckpointStore} bean is registered in
 * {@link ImperativeFlowWardenAutoConfiguration} or {@link ReactiveFlowWardenAutoConfiguration},
 * matching the configured {@code flowwarden.default-mode}.</p>
 */
@AutoConfiguration
@EnableConfigurationProperties(FlowWardenProperties.class)
public class FlowWardenAutoConfiguration {

    @Bean
    public static ChangeStreamBeanPostProcessor changeStreamBeanPostProcessor() {
        return new ChangeStreamBeanPostProcessor();
    }

    @Bean
    public FlowWardenConfigurationValidator flowWardenConfigurationValidator(
            FlowWardenProperties properties,
            ApplicationContext applicationContext) {
        return new FlowWardenConfigurationValidator(properties, applicationContext);
    }
}
