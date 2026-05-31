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

import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.DlqStore;
import io.flowwarden.stream.spi.FailedEvent;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Verifies that user-provided {@link DlqStore} and {@link CheckpointStore} beans
 * take precedence over the auto-configured MongoDB implementations.
 */
class CustomSpiBeansAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    FlowWardenAutoConfiguration.class,
                    ImperativeFlowWardenAutoConfiguration.class))
            .withUserConfiguration(MongoTemplateConfig.class);

    @Test
    void defaultDlqStoreIsAutoConfigured() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(DlqStore.class);
            assertThat(context.getBean(DlqStore.class))
                    .isNotInstanceOf(CustomDlqStore.class);
        });
    }

    @Test
    void customDlqStoreOverridesDefault() {
        contextRunner
                .withUserConfiguration(CustomDlqStoreConfig.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(DlqStore.class);
                    assertThat(context.getBean(DlqStore.class))
                            .isInstanceOf(CustomDlqStore.class);
                });
    }

    @Test
    void defaultCheckpointStoreIsAutoConfigured() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(CheckpointStore.class);
            assertThat(context.getBean(CheckpointStore.class))
                    .isNotInstanceOf(CustomCheckpointStore.class);
        });
    }

    @Test
    void customCheckpointStoreOverridesDefault() {
        contextRunner
                .withUserConfiguration(CustomCheckpointStoreConfig.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(CheckpointStore.class);
                    assertThat(context.getBean(CheckpointStore.class))
                            .isInstanceOf(CustomCheckpointStore.class);
                });
    }

    // --- Test configurations ---

    @Configuration(proxyBeanMethods = false)
    static class MongoTemplateConfig {
        @Bean
        MongoTemplate mongoTemplate() {
            return mock(MongoTemplate.class);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class CustomDlqStoreConfig {
        @Bean
        DlqStore dlqStore() {
            return new CustomDlqStore();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class CustomCheckpointStoreConfig {
        @Bean
        CheckpointStore checkpointStore() {
            return new CustomCheckpointStore();
        }
    }

    static class CustomDlqStore implements DlqStore {
        @Override
        public void save(FailedEvent event) {}

        @Override
        public Optional<FailedEvent> findById(String id) {
            return Optional.empty();
        }

        @Override
        public List<FailedEvent> findByStreamName(String streamName) {
            return List.of();
        }
    }

    static class CustomCheckpointStore implements CheckpointStore {
        @Override
        public void save(io.flowwarden.stream.spi.Checkpoint checkpoint) {}

        @Override
        public Optional<io.flowwarden.stream.spi.Checkpoint> findByStreamName(String streamName) {
            return Optional.empty();
        }

        @Override
        public void delete(String streamName) {}
    }
}
