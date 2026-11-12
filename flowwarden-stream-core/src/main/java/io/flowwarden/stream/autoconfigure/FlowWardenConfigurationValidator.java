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
import io.flowwarden.stream.registration.StreamDefinitionContributor;
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
 *
 * <p>The checks run as soon as the application declares at least one stream, whether
 * through a {@code @ChangeStream} bean or a {@link StreamDefinitionContributor} bean —
 * the fail-fast exists for the application that has streams to run, not for a class
 * path that merely carries the library.</p>
 *
 * <p>In an application assembled by the mode auto-configuration this callback is rarely the
 * first to notice a missing template: the mode-specific defaults ({@code checkpointStore},
 * {@code lockService}, …) are built on that template and fail first, with Spring's
 * {@code NoSuchBeanDefinitionException}. {@link FlowWardenMissingTemplateFailureAnalyzer}
 * explains that failure with the same description and action, so the user sees one
 * diagnosis either way.</p>
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
        // Only validate when the application declares streams — annotated or contributed.
        if (!hasChangeStreamBeans() && !hasStreamContributors()) {
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
            throw FlowWardenConfigurationException.missingMongoTemplate();
        }

        if (mode == ExecutionMode.REACTIVE
                && applicationContext.getBeanNamesForType(ReactiveMongoTemplate.class).length == 0) {
            throw FlowWardenConfigurationException.missingReactiveMongoTemplate();
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

    /**
     * A contributor bean is enough: its streams are only materialized later by
     * {@code StreamContributorProcessor}, and the checks here depend on the
     * configured mode, not on what it will contribute.
     */
    private boolean hasStreamContributors() {
        return applicationContext.getBeanNamesForType(StreamDefinitionContributor.class).length > 0;
    }
}
