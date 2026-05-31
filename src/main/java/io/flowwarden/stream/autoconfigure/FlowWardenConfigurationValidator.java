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

import io.flowwarden.stream.config.ExecutionMode;
import io.flowwarden.stream.config.FlowWardenProperties;
import io.flowwarden.stream.internal.discovery.ChangeStreamBeanPostProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;

/**
 * Validates the FlowWarden configuration after all singleton beans are created.
 *
 * <p>Checks that {@code flowwarden.default-mode} is set and that the corresponding
 * MongoDB template bean is available. Throws {@link FlowWardenConfigurationException}
 * with clear messages when validation fails.</p>
 */
public class FlowWardenConfigurationValidator implements SmartInitializingSingleton {

    private static final Logger log = LoggerFactory.getLogger(FlowWardenConfigurationValidator.class);

    private final FlowWardenProperties properties;
    private final ApplicationContext applicationContext;

    public FlowWardenConfigurationValidator(FlowWardenProperties properties,
                                            ApplicationContext applicationContext) {
        this.properties = properties;
        this.applicationContext = applicationContext;
    }

    @Override
    public void afterSingletonsInstantiated() {
        // Only validate if at least one @ChangeStream bean was discovered
        if (!hasChangeStreamBeans()) {
            return;
        }

        ExecutionMode mode = properties.getDefaultMode();

        if (mode == null) {
            properties.setDefaultMode(ExecutionMode.IMPERATIVE);
            mode = ExecutionMode.IMPERATIVE;
            log.info("No 'flowwarden.default-mode' configured, defaulting to IMPERATIVE");
        }

        if (mode == ExecutionMode.IMPERATIVE
                && applicationContext.getBeanNamesForType(MongoTemplate.class).length == 0) {
            throw new FlowWardenConfigurationException(
                    "FlowWarden is configured in IMPERATIVE mode but MongoTemplate is not available.",
                    "Either:\n"
                            + "1. Add 'spring-boot-starter-data-mongodb' to your dependencies\n"
                            + "2. Change 'flowwarden.default-mode' to REACTIVE in your configuration"
            );
        }

        if (mode == ExecutionMode.REACTIVE
                && applicationContext.getBeanNamesForType(ReactiveMongoTemplate.class).length == 0) {
            throw new FlowWardenConfigurationException(
                    "FlowWarden is configured in REACTIVE mode but ReactiveMongoTemplate is not available.",
                    "Either:\n"
                            + "1. Add 'spring-boot-starter-data-mongodb-reactive' to your dependencies\n"
                            + "2. Change 'flowwarden.default-mode' to IMPERATIVE in your configuration"
            );
        }
    }

    private boolean hasChangeStreamBeans() {
        String[] bppNames = applicationContext.getBeanNamesForType(ChangeStreamBeanPostProcessor.class);
        for (String bppName : bppNames) {
            ChangeStreamBeanPostProcessor bpp = applicationContext.getBean(bppName, ChangeStreamBeanPostProcessor.class);
            if (!bpp.getDefinitions().isEmpty()) {
                return true;
            }
        }
        return false;
    }
}
