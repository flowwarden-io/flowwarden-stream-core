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
import io.flowwarden.stream.core.ContextHandler;
import io.flowwarden.stream.core.ReactiveContextHandler;
import io.flowwarden.stream.registration.StreamDefinitionContributor;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.data.mongo.MongoReactiveDataAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoReactiveAutoConfiguration;
import org.springframework.boot.diagnostics.FailureAnalysis;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The fail-fast on {@code flowwarden.default-mode} versus the available Mongo template,
 * exercised the way a real application wires FlowWarden: Spring Boot's Mongo
 * auto-configurations plus the common and the two mode auto-configurations. The
 * mode-specific defaults ({@code checkpointStore(ReactiveMongoTemplate)}, …) are the first
 * beans to need the template, so the diagnosis has to be the failure the user sees — not
 * an analysis of a context that never got that far.
 */
class FlowWardenConfigurationValidatorTest {

    /** Nothing connects at bean creation; the URI only has to parse. */
    private static final String MONGO_URI = "spring.data.mongodb.uri=mongodb://localhost:27017/validator-test";

    private static final String SYNC_STARTER = "spring-boot-starter-data-mongodb'";
    private static final String REACTIVE_STARTER = "spring-boot-starter-data-mongodb-reactive'";

    // --- Runners -------------------------------------------------------------------------

    /** Both drivers auto-configured, as in an application carrying both starters. */
    private static ApplicationContextRunner fullStack() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        MongoAutoConfiguration.class,
                        MongoDataAutoConfiguration.class,
                        MongoReactiveAutoConfiguration.class,
                        MongoReactiveDataAutoConfiguration.class,
                        FlowWardenAutoConfiguration.class,
                        ImperativeFlowWardenAutoConfiguration.class,
                        ReactiveFlowWardenAutoConfiguration.class))
                .withPropertyValues(MONGO_URI);
    }

    /** Only the reactive starter: no {@code MongoTemplate} anywhere. */
    private static ApplicationContextRunner reactiveStarterOnly() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        MongoReactiveAutoConfiguration.class,
                        MongoReactiveDataAutoConfiguration.class,
                        FlowWardenAutoConfiguration.class,
                        ImperativeFlowWardenAutoConfiguration.class,
                        ReactiveFlowWardenAutoConfiguration.class))
                .withPropertyValues(MONGO_URI);
    }

    /** Only the synchronous starter: no {@code ReactiveMongoTemplate} anywhere. */
    private static ApplicationContextRunner syncStarterOnly() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        MongoAutoConfiguration.class,
                        MongoDataAutoConfiguration.class,
                        FlowWardenAutoConfiguration.class,
                        ImperativeFlowWardenAutoConfiguration.class,
                        ReactiveFlowWardenAutoConfiguration.class))
                .withPropertyValues(MONGO_URI);
    }

    // --- Streams declared with @ChangeStream ---------------------------------------------

    @Test
    void defaultsToImperativeWhenModeNotConfigured() {
        fullStack()
                .withUserConfiguration(ChangeStreamHandlerConfig.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(FlowWardenProperties.class).getDefaultMode())
                            .isEqualTo(ExecutionMode.IMPERATIVE);
                });
    }

    @Test
    void failsWhenImperativeModeWithoutMongoTemplate() {
        reactiveStarterOnly()
                .withUserConfiguration(ChangeStreamHandlerConfig.class)
                .withPropertyValues("flowwarden.default-mode=IMPERATIVE")
                .run(context -> assertFlowWardenDiagnosis(context, "IMPERATIVE", "MongoTemplate", SYNC_STARTER));
    }

    @Test
    void failsWhenReactiveModeWithoutReactiveMongoTemplate() {
        syncStarterOnly()
                .withUserConfiguration(ReactiveChangeStreamHandlerConfig.class)
                .withPropertyValues("flowwarden.default-mode=REACTIVE")
                .run(context -> assertFlowWardenDiagnosis(context, "REACTIVE", "ReactiveMongoTemplate", REACTIVE_STARTER));
    }

    @Test
    void succeedsWhenImperativeModeWithMongoTemplate() {
        fullStack()
                .withUserConfiguration(ChangeStreamHandlerConfig.class)
                .withPropertyValues("flowwarden.default-mode=IMPERATIVE")
                .run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void succeedsWhenReactiveModeWithReactiveMongoTemplate() {
        fullStack()
                .withUserConfiguration(ReactiveChangeStreamHandlerConfig.class)
                .withPropertyValues("flowwarden.default-mode=REACTIVE")
                .run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void modeValueIsCaseInsensitive() {
        syncStarterOnly()
                .withUserConfiguration(ReactiveChangeStreamHandlerConfig.class)
                .withPropertyValues("flowwarden.default-mode=reactive")
                .run(context -> assertFlowWardenDiagnosis(context, "REACTIVE", "ReactiveMongoTemplate", REACTIVE_STARTER));
    }

    // --- Streams declared only through StreamDefinitionContributor -----------------------

    @Test
    void contributorOnly_failsWhenReactiveModeWithoutReactiveMongoTemplate() {
        // Before: no @ChangeStream bean, so the validator returned early and the misconfiguration
        // surfaced as a raw NoSuchBeanDefinitionException on 'checkpointStore'.
        syncStarterOnly()
                .withUserConfiguration(ReactiveContributorConfig.class)
                .withPropertyValues("flowwarden.default-mode=REACTIVE")
                .run(context -> assertFlowWardenDiagnosis(context, "REACTIVE", "ReactiveMongoTemplate", REACTIVE_STARTER));
    }

    @Test
    void contributorOnly_failsWhenImperativeModeWithoutMongoTemplate() {
        reactiveStarterOnly()
                .withUserConfiguration(ImperativeContributorConfig.class)
                .withPropertyValues("flowwarden.default-mode=IMPERATIVE")
                .run(context -> assertFlowWardenDiagnosis(context, "IMPERATIVE", "MongoTemplate", SYNC_STARTER));
    }

    @Test
    void contributorOnly_succeedsWhenReactiveModeWithReactiveMongoTemplate() {
        fullStack()
                .withUserConfiguration(ReactiveContributorConfig.class)
                .withPropertyValues("flowwarden.default-mode=REACTIVE")
                .run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void contributorOnly_defaultsToImperativeWhenModeNotConfigured() {
        fullStack()
                .withUserConfiguration(ImperativeContributorConfig.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(FlowWardenProperties.class).getDefaultMode())
                            .isEqualTo(ExecutionMode.IMPERATIVE);
                });
    }

    // --- The late validator on its own ---------------------------------------------------

    @Test
    void succeedsWithoutModeWhenNoStreamsAtAll() {
        // Neither a @ChangeStream bean nor a contributor: nothing to validate against, even with
        // no template at all — a class path that merely carries the library is not misconfigured.
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(FlowWardenAutoConfiguration.class))
                .run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void lateValidator_contributorOnly_failsWhenReactiveModeWithoutReactiveMongoTemplate() {
        // Without the mode auto-configuration nothing needs the template before the validator
        // runs: this is the one place its own check is the failure, and it must count contributors.
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(FlowWardenAutoConfiguration.class))
                .withUserConfiguration(ReactiveContributorConfig.class)
                .withPropertyValues("flowwarden.default-mode=REACTIVE")
                .run(context -> {
                    assertThat(context).hasFailed();
                    Throwable failure = findCause(context.getStartupFailure(), FlowWardenConfigurationException.class);
                    assertThat(failure).isNotNull().hasMessageContaining("ReactiveMongoTemplate");
                    assertThat(new FlowWardenFailureAnalyzer().analyze(context.getStartupFailure())).isNotNull();
                });
    }

    // --- Helpers -------------------------------------------------------------------------

    /**
     * Spring detects the missing template ({@code NoSuchBeanDefinitionException} on a
     * mode-specific default), FlowWarden explains it: whichever exception ends the startup,
     * the analysis the user reads names the mode, the template, the starter and the way out.
     */
    private static void assertFlowWardenDiagnosis(AssertableApplicationContext context,
                                                  String mode, String template, String starter) {
        assertThat(context).hasFailed();
        Throwable startupFailure = context.getStartupFailure();
        assertThat(findCause(startupFailure, NoSuchBeanDefinitionException.class))
                .as("the template is what Spring reports missing")
                .isNotNull()
                .hasMessageContaining(template);

        FailureAnalysis analysis = new FlowWardenMissingTemplateFailureAnalyzer().analyze(startupFailure);
        assertThat(analysis).as("APPLICATION FAILED TO START carries the FlowWarden analysis").isNotNull();
        assertThat(analysis.getDescription()).contains(mode).contains(template);
        assertThat(analysis.getAction()).contains(starter).contains("flowwarden.default-mode");
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

    // --- Test configurations -------------------------------------------------------------

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

    @Configuration(proxyBeanMethods = false)
    static class ReactiveContributorConfig {
        @Bean
        StreamDefinitionContributor ordersContributor() {
            return registration -> registration.stream("orders-contributed", Object.class)
                    .collection("orders")
                    .onInsertReactive((ReactiveContextHandler<Object>) ctx -> Mono.empty());
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ImperativeContributorConfig {
        @Bean
        StreamDefinitionContributor ordersContributor() {
            return registration -> registration.stream("orders-contributed", Object.class)
                    .collection("orders")
                    .onInsert((ContextHandler<Object>) ctx -> { });
        }
    }

}
