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

import io.flowwarden.stream.ChangeStreamContext;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.OnChange;
import io.flowwarden.stream.config.ExecutionMode;
import io.flowwarden.stream.config.FlowWardenProperties;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class FlowWardenConfigurationValidatorTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(FlowWardenAutoConfiguration.class))
            .withUserConfiguration(ChangeStreamHandlerConfig.class);

    @Test
    void defaultsToImperativeWhenModeNotConfigured() {
        contextRunner
                .withUserConfiguration(MongoTemplateConfig.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    FlowWardenProperties props = context.getBean(FlowWardenProperties.class);
                    assertThat(props.getDefaultMode()).isEqualTo(ExecutionMode.IMPERATIVE);
                });
    }

    @Test
    void failsWhenImperativeModeWithoutMongoTemplate() {
        contextRunner
                .withPropertyValues("flowwarden.default-mode=IMPERATIVE")
                .run(context -> {
                    assertThat(context).hasFailed();
                    Throwable failure = findCause(context.getStartupFailure(),
                            FlowWardenConfigurationException.class);
                    assertThat(failure)
                            .isNotNull()
                            .hasMessageContaining("IMPERATIVE")
                            .hasMessageContaining("MongoTemplate");
                });
    }

    @Test
    void failsWhenReactiveModeWithoutReactiveMongoTemplate() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(FlowWardenAutoConfiguration.class))
                .withUserConfiguration(ReactiveChangeStreamHandlerConfig.class)
                .withPropertyValues("flowwarden.default-mode=REACTIVE")
                .run(context -> {
                    assertThat(context).hasFailed();
                    Throwable failure = findCause(context.getStartupFailure(),
                            FlowWardenConfigurationException.class);
                    assertThat(failure)
                            .isNotNull()
                            .hasMessageContaining("REACTIVE")
                            .hasMessageContaining("ReactiveMongoTemplate");
                });
    }

    private static Throwable findCause(Throwable throwable, Class<? extends Throwable> type) {
        Throwable current = throwable;
        while (current != null) {
            if (type.isInstance(current)) {
                return current;
            }
            current = current.getCause();
        }
        return null;
    }

    @Test
    void succeedsWhenImperativeModeWithMongoTemplate() {
        contextRunner
                .withPropertyValues("flowwarden.default-mode=IMPERATIVE")
                .withUserConfiguration(MongoTemplateConfig.class)
                .run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void succeedsWhenReactiveModeWithReactiveMongoTemplate() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(FlowWardenAutoConfiguration.class))
                .withUserConfiguration(ReactiveChangeStreamHandlerConfig.class, ReactiveMongoTemplateConfig.class)
                .withPropertyValues("flowwarden.default-mode=REACTIVE")
                .run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void succeedsWithoutModeWhenNoChangeStreamBeans() {
        // No @ChangeStream beans → validator should not complain about missing mode
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(FlowWardenAutoConfiguration.class))
                .run(context -> assertThat(context).hasNotFailed());
    }

    // --- Test configurations ---

    @Configuration(proxyBeanMethods = false)
    static class ChangeStreamHandlerConfig {
        @Bean
        TestHandler testHandler() {
            return new TestHandler();
        }
    }

    @ChangeStream(collection = "orders")
    static class TestHandler {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class MongoTemplateConfig {
        @Bean
        MongoTemplate mongoTemplate() {
            return mock(MongoTemplate.class);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ReactiveMongoTemplateConfig {
        @Bean
        ReactiveMongoTemplate reactiveMongoTemplate() {
            return mock(ReactiveMongoTemplate.class);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ReactiveChangeStreamHandlerConfig {
        @Bean
        ReactiveTestHandler reactiveTestHandler() {
            return new ReactiveTestHandler();
        }
    }

    @ChangeStream(collection = "orders")
    static class ReactiveTestHandler {
        @OnChange
        Mono<Void> handle(ChangeStreamContext<?> ctx) {
            return Mono.empty();
        }
    }
}
