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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.flowwarden.stream.ChangeStreamContext;
import io.flowwarden.stream.FullDocumentBeforeChangeMode;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.OnChange;
import io.flowwarden.stream.annotation.OnDelete;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.annotation.OnReplace;
import io.flowwarden.stream.annotation.OnUpdate;
import io.flowwarden.stream.annotation.RetryPolicy;
import io.flowwarden.stream.autoconfigure.FlowWardenAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class ChangeStreamBeanPostProcessorValidationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(FlowWardenAutoConfiguration.class))
            .withPropertyValues("flowwarden.default-mode=IMPERATIVE")
            .withUserConfiguration(MongoTemplateConfig.class);

    @Test
    void failsWhenMongoTemplateRefDoesNotExist() {
        contextRunner
                .withUserConfiguration(MissingRefHandlerConfig.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasMessageContaining("mongoTemplateRef='missingTemplate'")
                            .hasMessageContaining("no bean");
                });
    }

    @Test
    void succeedsWhenMongoTemplateRefExists() {
        contextRunner
                .withUserConfiguration(ValidRefHandlerConfig.class, CustomMongoTemplateConfig.class)
                .run(context -> assertThat(context).hasNotFailed());
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
    static class CustomMongoTemplateConfig {
        @Bean
        MongoTemplate myTemplate() {
            return mock(MongoTemplate.class);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class MissingRefHandlerConfig {
        @Bean
        MissingRefHandler missingRefHandler() {
            return new MissingRefHandler();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ValidRefHandlerConfig {
        @Bean
        ValidRefHandler validRefHandler() {
            return new ValidRefHandler();
        }
    }

    @ChangeStream(collection = "orders", mongoTemplateRef = "missingTemplate")
    static class MissingRefHandler {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {
        }
    }

    @ChangeStream(collection = "orders", mongoTemplateRef = "myTemplate")
    static class ValidRefHandler {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {
        }
    }

    // --- Multiple typed annotation on same method tests ---

    @Test
    void succeedsWhenMethodHasMultipleTypedAnnotations() {
        contextRunner
                .withUserConfiguration(DualAnnotationHandlerConfig.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    // Both operation types should be registered with the same handler
                    ChangeStreamBeanPostProcessor bpp = context.getBean(ChangeStreamBeanPostProcessor.class);
                    assertThat(bpp.getDefinitions()).hasSize(1);
                    ChangeStreamDefinition def = bpp.getDefinitions().get(0);
                    assertThat(def.typedHandlers()).containsKey(io.flowwarden.stream.OperationType.UPDATE);
                    assertThat(def.typedHandlers()).containsKey(io.flowwarden.stream.OperationType.REPLACE);
                    // Both point to the same method
                    assertThat(def.typedHandlers().get(io.flowwarden.stream.OperationType.UPDATE).method())
                            .isEqualTo(def.typedHandlers().get(io.flowwarden.stream.OperationType.REPLACE).method());
                });
    }

    @Test
    void succeedsWhenMethodHasInsertAndUpdateAnnotations() {
        contextRunner
                .withUserConfiguration(InsertUpdateHandlerConfig.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    ChangeStreamBeanPostProcessor bpp = context.getBean(ChangeStreamBeanPostProcessor.class);
                    assertThat(bpp.getDefinitions()).hasSize(1);
                    ChangeStreamDefinition def = bpp.getDefinitions().get(0);
                    assertThat(def.typedHandlers()).containsKey(io.flowwarden.stream.OperationType.INSERT);
                    assertThat(def.typedHandlers()).containsKey(io.flowwarden.stream.OperationType.UPDATE);
                });
    }

    @Configuration(proxyBeanMethods = false)
    static class DualAnnotationHandlerConfig {
        @Bean
        DualAnnotationHandler dualAnnotationHandler() {
            return new DualAnnotationHandler();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class InsertUpdateHandlerConfig {
        @Bean
        InsertUpdateHandler insertUpdateHandler() {
            return new InsertUpdateHandler();
        }
    }

    @ChangeStream(collection = "orders")
    static class DualAnnotationHandler {
        @OnUpdate
        @OnReplace
        void handle(ChangeStreamContext<?> ctx) {
        }
    }

    @ChangeStream(collection = "orders")
    static class InsertUpdateHandler {
        @OnInsert
        @OnUpdate
        void handle(ChangeStreamContext<?> ctx) {
        }
    }

    // --- Mixed fullDocument warning tests ---

    @Test
    void warnsWhenMethodCombinesAnnotationsWithAndWithoutFullDocument() {
        Logger bppLogger = (Logger) LoggerFactory.getLogger(ChangeStreamBeanPostProcessor.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        bppLogger.addAppender(listAppender);

        try {
            contextRunner
                    .withUserConfiguration(MixedFullDocHandlerConfig.class)
                    .run(context -> {
                        assertThat(context).hasNotFailed();

                        assertThat(listAppender.list)
                                .anyMatch(event ->
                                        event.getLevel() == Level.WARN
                                                && event.getFormattedMessage().contains("@OnUpdate")
                                                && event.getFormattedMessage().contains("@OnDelete")
                                                && event.getFormattedMessage().contains("no fullDocument"));
                    });
        } finally {
            bppLogger.detachAppender(listAppender);
        }
    }

    @Test
    void noWarningWhenAllAnnotationsHaveFullDocument() {
        Logger bppLogger = (Logger) LoggerFactory.getLogger(ChangeStreamBeanPostProcessor.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        bppLogger.addAppender(listAppender);

        try {
            contextRunner
                    .withUserConfiguration(InsertUpdateHandlerConfig.class)
                    .run(context -> {
                        assertThat(context).hasNotFailed();

                        assertThat(listAppender.list)
                                .noneMatch(event ->
                                        event.getLevel() == Level.WARN
                                                && event.getFormattedMessage().contains("no fullDocument"));
                    });
        } finally {
            bppLogger.detachAppender(listAppender);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class MixedFullDocHandlerConfig {
        @Bean
        MixedFullDocHandler mixedFullDocHandler() {
            return new MixedFullDocHandler();
        }
    }

    @ChangeStream(collection = "orders")
    static class MixedFullDocHandler {
        @OnUpdate
        @OnDelete
        void handle(ChangeStreamContext<?> ctx) {
        }
    }

    // --- Mode/signature validation tests ---

    @Test
    void failsWhenReactiveHandlerInImperativeMode() {
        contextRunner
                .withPropertyValues("flowwarden.default-mode=IMPERATIVE")
                .withUserConfiguration(ReactiveHandlerConfig.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasMessageContaining("Mono<Void>")
                            .hasMessageContaining("IMPERATIVE");
                });
    }

    @Test
    void failsWhenVoidHandlerInReactiveMode() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(FlowWardenAutoConfiguration.class))
                .withPropertyValues("flowwarden.default-mode=REACTIVE")
                .withUserConfiguration(ReactiveMongoTemplateConfig.class, VoidHandlerConfig.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasMessageContaining("void")
                            .hasMessageContaining("REACTIVE");
                });
    }

    @Test
    void succeedsWhenReactiveHandlerInReactiveMode() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(FlowWardenAutoConfiguration.class))
                .withPropertyValues("flowwarden.default-mode=REACTIVE")
                .withUserConfiguration(ReactiveMongoTemplateConfig.class, ReactiveHandlerConfig.class)
                .run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void succeedsWhenVoidHandlerInImperativeMode() {
        contextRunner
                .withPropertyValues("flowwarden.default-mode=IMPERATIVE")
                .withUserConfiguration(VoidHandlerConfig.class)
                .run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void failsWhenHandlerReturnsUnsupportedType() {
        contextRunner
                .withUserConfiguration(UnsupportedReturnTypeConfig.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasMessageContaining("unsupported return type");
                });
    }

    // --- Configs for mode validation tests ---

    @Configuration(proxyBeanMethods = false)
    static class ReactiveMongoTemplateConfig {
        @Bean
        ReactiveMongoTemplate reactiveMongoTemplate() {
            return mock(ReactiveMongoTemplate.class);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ReactiveHandlerConfig {
        @Bean
        ReactiveHandler reactiveHandler() {
            return new ReactiveHandler();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class VoidHandlerConfig {
        @Bean
        VoidHandler voidHandler() {
            return new VoidHandler();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class UnsupportedReturnTypeConfig {
        @Bean
        UnsupportedReturnTypeHandler unsupportedReturnTypeHandler() {
            return new UnsupportedReturnTypeHandler();
        }
    }

    @ChangeStream(collection = "orders")
    static class ReactiveHandler {
        @OnChange
        Mono<Void> handle(ChangeStreamContext<?> ctx) {
            return Mono.empty();
        }
    }

    @ChangeStream(collection = "orders")
    static class VoidHandler {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {
        }
    }

    @ChangeStream(collection = "orders")
    static class UnsupportedReturnTypeHandler {
        @OnChange
        String handle(ChangeStreamContext<?> ctx) {
            return "bad";
        }
    }

    // --- @RetryPolicy validation tests ---

    @Test
    void failsWhenRetryPolicyMaxAttemptsLessThanOne() {
        contextRunner
                .withUserConfiguration(InvalidRetryMaxAttemptsConfig.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasMessageContaining("@RetryPolicy")
                            .hasMessageContaining("maxAttempts");
                });
    }

    @Test
    void failsWhenRetryPolicyMultiplierNotPositive() {
        contextRunner
                .withUserConfiguration(InvalidRetryMultiplierConfig.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasMessageContaining("@RetryPolicy")
                            .hasMessageContaining("multiplier");
                });
    }

    @Test
    void failsWhenRetryPolicyHasInvalidDelay() {
        contextRunner
                .withUserConfiguration(InvalidRetryDelayConfig.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasMessageContaining("@RetryPolicy")
                            .hasMessageContaining("initialDelay");
                });
    }

    @Test
    void succeedsWithValidRetryPolicy() {
        contextRunner
                .withUserConfiguration(ValidRetryHandlerConfig.class)
                .run(context -> assertThat(context).hasNotFailed());
    }

    @Configuration(proxyBeanMethods = false)
    static class InvalidRetryMaxAttemptsConfig {
        @Bean
        InvalidRetryMaxAttemptsHandler invalidRetryMaxAttemptsHandler() {
            return new InvalidRetryMaxAttemptsHandler();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class InvalidRetryMultiplierConfig {
        @Bean
        InvalidRetryMultiplierHandler invalidRetryMultiplierHandler() {
            return new InvalidRetryMultiplierHandler();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class InvalidRetryDelayConfig {
        @Bean
        InvalidRetryDelayHandler invalidRetryDelayHandler() {
            return new InvalidRetryDelayHandler();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ValidRetryHandlerConfig {
        @Bean
        ValidRetryHandler validRetryHandler() {
            return new ValidRetryHandler();
        }
    }

    @ChangeStream(collection = "orders")
    @RetryPolicy(maxAttempts = 0)
    static class InvalidRetryMaxAttemptsHandler {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {
        }
    }

    @ChangeStream(collection = "orders")
    @RetryPolicy(multiplier = -1.0)
    static class InvalidRetryMultiplierHandler {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {
        }
    }

    @ChangeStream(collection = "orders")
    @RetryPolicy(initialDelay = "invalid")
    static class InvalidRetryDelayHandler {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {
        }
    }

    @ChangeStream(collection = "orders")
    @RetryPolicy(maxAttempts = 5, initialDelay = "1s", maxDelay = "30s")
    static class ValidRetryHandler {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {
        }
    }

    // --- fullDocumentBeforeChange validation tests ---

    @Test
    void warnsWhenFullDocumentBeforeChangeWithOnlyInsertHandlers() {
        // Should not throw — just warn (we verify it doesn't fail the context)
        contextRunner
                .withUserConfiguration(BeforeChangeInsertOnlyConfig.class)
                .run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void succeedsWhenFullDocumentBeforeChangeWithUpdateHandlers() {
        contextRunner
                .withUserConfiguration(BeforeChangeWithUpdateConfig.class)
                .run(context -> assertThat(context).hasNotFailed());
    }

    @Configuration(proxyBeanMethods = false)
    static class BeforeChangeInsertOnlyConfig {
        @Bean
        BeforeChangeInsertOnlyHandler beforeChangeInsertOnlyHandler() {
            return new BeforeChangeInsertOnlyHandler();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class BeforeChangeWithUpdateConfig {
        @Bean
        BeforeChangeWithUpdateHandler beforeChangeWithUpdateHandler() {
            return new BeforeChangeWithUpdateHandler();
        }
    }

    @ChangeStream(collection = "orders", fullDocumentBeforeChange = FullDocumentBeforeChangeMode.WHEN_AVAILABLE)
    static class BeforeChangeInsertOnlyHandler {
        @OnInsert
        void onInsert(ChangeStreamContext<?> ctx) {
        }
    }

    @ChangeStream(collection = "orders", fullDocumentBeforeChange = FullDocumentBeforeChangeMode.WHEN_AVAILABLE)
    static class BeforeChangeWithUpdateHandler {
        @OnInsert
        void onInsert(ChangeStreamContext<?> ctx) {
        }

        @OnUpdate
        void onUpdate(ChangeStreamContext<?> ctx) {
        }
    }
}
