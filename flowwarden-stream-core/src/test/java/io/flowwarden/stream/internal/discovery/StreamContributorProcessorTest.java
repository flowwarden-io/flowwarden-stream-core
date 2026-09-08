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
package io.flowwarden.stream.internal.discovery;

import io.flowwarden.stream.ChangeStreamContext;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.OnChange;
import io.flowwarden.stream.autoconfigure.FlowWardenAutoConfiguration;
import io.flowwarden.stream.registration.CheckpointSpec;
import io.flowwarden.stream.registration.RetryPolicySpec;
import io.flowwarden.stream.registration.StreamDefinitionContributor;
import io.flowwarden.stream.registration.StreamRegistration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class StreamContributorProcessorTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(FlowWardenAutoConfiguration.class))
            .withPropertyValues("flowwarden.default-mode=IMPERATIVE")
            .withUserConfiguration(MongoTemplateConfig.class);

    @Test
    void registersAContributedStream() {
        contextRunner
                .withUserConfiguration(ValidContributorConfig.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    StreamRegistry registry = context.getBean(StreamRegistry.class);
                    assertThat(registry.findByName("contributed-stream")).isPresent();
                });
    }

    @Test
    void failsOnDuplicateNameAgainstAnnotatedStream() {
        contextRunner
                .withUserConfiguration(AnnotatedOrderStreamConfig.class, DuplicateOfAnnotatedConfig.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasMessageContaining("order-stream")
                            .hasMessageContaining("already registered");
                });
    }

    @Test
    void failsOnDuplicateNameBetweenTwoContributors() {
        contextRunner
                .withUserConfiguration(ValidContributorConfig.class, DuplicateContributorConfig.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasMessageContaining("contributed-stream")
                            .hasMessageContaining("already registered");
                });
    }

    @Test
    void failsWithoutAtLeastOneHandler() {
        contextRunner
                .withUserConfiguration(NoHandlerContributorConfig.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasMessageContaining("at least one handler");
                });
    }

    @Test
    void failsOnInvalidRetryPolicySameAsAnnotationPath() {
        // Parity check: an invalid RetryPolicySpec fails for the same reason as the
        // equivalent invalid @RetryPolicy (see ChangeStreamBeanPostProcessorValidationTest).
        contextRunner
                .withUserConfiguration(InvalidRetryContributorConfig.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasMessageContaining("@RetryPolicy")
                            .hasMessageContaining("invalid multiplier");
                });
    }

    @Test
    void failsWhenDocumentTypeIsRawDocumentAndNoCollectionGivenSameAsAnnotationPath() {
        contextRunner
                .withUserConfiguration(RawDocumentNoCollectionContributorConfig.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasMessageContaining("must specify a collection or a documentType with @Document");
                });
    }

    @Test
    void failsWhenMongoTemplateRefNamesABeanThatIsNotAMongoTemplate() {
        // Same regression as ChangeStreamBeanPostProcessorValidationTest, contributor path.
        contextRunner
                .withUserConfiguration(BadMongoTemplateRefContributorConfig.class, NonMongoTemplateBeanConfig.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasMessageContaining("mongoTemplateRef='notATemplate'")
                            .hasMessageContaining("no bean");
                });
    }

    @Test
    void failsOnHandlerModeMismatch() {
        contextRunner
                .withUserConfiguration(ReactiveHandlerInImperativeModeContributorConfig.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasMessageContaining("Mono<Void>")
                            .hasMessageContaining("IMPERATIVE");
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
    static class AnnotatedOrderStreamConfig {
        @Bean
        AnnotatedOrderStream annotatedOrderStream() {
            return new AnnotatedOrderStream();
        }
    }

    @ChangeStream(name = "order-stream", collection = "orders")
    static class AnnotatedOrderStream {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class DuplicateOfAnnotatedConfig {
        @Bean
        StreamDefinitionContributor duplicateOfAnnotated() {
            return registration -> registration.stream("order-stream", Order.class)
                    .collection("orders")
                    .onChange(ctx -> { });
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ValidContributorConfig {
        @Bean
        StreamDefinitionContributor validContributor() {
            return registration -> registration.stream("contributed-stream", Order.class)
                    .collection("orders")
                    .checkpoint(CheckpointSpec.defaults())
                    .onInsert((order, ctx) -> { })
                    .onChange(ctx -> { });
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class DuplicateContributorConfig {
        @Bean
        StreamDefinitionContributor duplicateContributor() {
            return registration -> registration.stream("contributed-stream", Order.class)
                    .collection("other_orders")
                    .onChange(ctx -> { });
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class NoHandlerContributorConfig {
        @Bean
        StreamDefinitionContributor noHandlerContributor() {
            return registration -> registration.stream("no-handler-stream", Order.class)
                    .collection("orders");
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class InvalidRetryContributorConfig {
        @Bean
        StreamDefinitionContributor invalidRetryContributor() {
            return registration -> registration.stream("invalid-retry-stream", Order.class)
                    .collection("orders")
                    .retryPolicy(RetryPolicySpec.builder().multiplier(-1.0).build())
                    .onChange(ctx -> { });
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class NonMongoTemplateBeanConfig {
        @Bean
        String notATemplate() {
            return "not-a-mongo-template";
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class BadMongoTemplateRefContributorConfig {
        @Bean
        StreamDefinitionContributor badMongoTemplateRefContributor() {
            return registration -> registration.stream("bad-ref-stream", Order.class)
                    .collection("orders")
                    .mongoTemplateRef("notATemplate")
                    .onChange(ctx -> { });
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class RawDocumentNoCollectionContributorConfig {
        @Bean
        StreamDefinitionContributor rawDocumentNoCollectionContributor() {
            return registration -> registration.stream("raw-stream", org.bson.Document.class)
                    .onChange(ctx -> { });
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ReactiveHandlerInImperativeModeContributorConfig {
        @Bean
        StreamDefinitionContributor reactiveContributor() {
            return registration -> registration.stream("reactive-in-imperative-stream", Order.class)
                    .collection("orders")
                    .onChangeReactive(ctx -> reactor.core.publisher.Mono.empty());
        }
    }

    static class Order {
    }
}
