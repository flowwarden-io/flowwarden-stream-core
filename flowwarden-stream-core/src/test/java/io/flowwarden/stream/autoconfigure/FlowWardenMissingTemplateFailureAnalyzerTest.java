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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.InjectionPoint;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.beans.factory.UnsatisfiedDependencyException;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.diagnostics.FailureAnalysis;
import org.springframework.boot.diagnostics.FailureAnalyzer;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.MethodParameter;
import org.springframework.core.annotation.OrderUtils;
import org.springframework.core.io.UrlResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Spring detects a missing template, FlowWarden explains it. The analyzer must speak only
 * for failures that are provably FlowWarden's, be discovered through
 * {@code META-INF/spring.factories}, and win over Spring Boot's generic missing-bean
 * analyzer on a real startup failure.
 */
@ExtendWith(OutputCaptureExtension.class)
class FlowWardenMissingTemplateFailureAnalyzerTest {

    private final FlowWardenMissingTemplateFailureAnalyzer analyzer = new FlowWardenMissingTemplateFailureAnalyzer();

    // --- What it explains ----------------------------------------------------------------

    @Test
    void reactiveTemplateMissingForAFlowWardenBean_isExplained() throws Exception {
        Throwable failure = unsatisfied(
                ReactiveFlowWardenAutoConfiguration.class, "checkpointStore", ReactiveMongoTemplate.class,
                new NoSuchBeanDefinitionException(ReactiveMongoTemplate.class));

        FailureAnalysis analysis = analyzer.analyze(failure);

        assertThat(analysis).isNotNull();
        assertThat(analysis.getDescription()).contains("REACTIVE").contains("ReactiveMongoTemplate");
        assertThat(analysis.getAction())
                .contains("spring-boot-starter-data-mongodb-reactive'")
                .contains("flowwarden.default-mode");
        assertThat(analysis.getCause()).isInstanceOf(NoSuchBeanDefinitionException.class);
    }

    @Test
    void mongoTemplateMissingForAFlowWardenBean_isExplained() throws Exception {
        Throwable failure = unsatisfied(
                ImperativeFlowWardenAutoConfiguration.class, "checkpointStore", MongoTemplate.class,
                new NoSuchBeanDefinitionException(MongoTemplate.class));

        FailureAnalysis analysis = analyzer.analyze(failure);

        assertThat(analysis).isNotNull();
        assertThat(analysis.getDescription()).contains("IMPERATIVE").contains("MongoTemplate");
        assertThat(analysis.getAction()).contains("spring-boot-starter-data-mongodb'");
    }

    @Test
    void nestedBehindAUserBean_isStillExplained() throws Exception {
        // A user bean depends on the stream manager, which depends on the checkpoint store, which
        // needs the template: the innermost unsatisfied dependency is FlowWarden's.
        Throwable flowWardenFailure = unsatisfied(
                ReactiveFlowWardenAutoConfiguration.class, "checkpointStore", ReactiveMongoTemplate.class,
                new NoSuchBeanDefinitionException(ReactiveMongoTemplate.class));
        Throwable failure = new UnsatisfiedDependencyException(
                "user config", "orderService",
                new InjectionPoint(parameter(UserConfig.class, "orderService", String.class)),
                new BeanCreationException("reactiveStreamManager", "nested", flowWardenFailure));

        assertThat(analyzer.analyze(failure)).isNotNull();
    }

    // --- What it leaves to Spring Boot ---------------------------------------------------

    @Test
    void templateMissingForAUserBean_isNotOurs() throws Exception {
        Throwable failure = unsatisfied(
                UserConfig.class, "orderService", String.class,
                new NoSuchBeanDefinitionException(ReactiveMongoTemplate.class));

        assertThat(analyzer.analyze(failure)).isNull();
    }

    @Test
    void anotherTypeMissingForAFlowWardenBean_isNotOurs() throws Exception {
        Throwable failure = unsatisfied(
                ReactiveFlowWardenAutoConfiguration.class, "checkpointStore", ReactiveMongoTemplate.class,
                new NoSuchBeanDefinitionException(String.class));

        assertThat(analyzer.analyze(failure)).isNull();
    }

    @Test
    void templateOfTheOtherMode_isNotOurs() throws Exception {
        // The exceptions must agree on the mode; a mismatch is some other wiring problem.
        Throwable failure = unsatisfied(
                ReactiveFlowWardenAutoConfiguration.class, "checkpointStore", ReactiveMongoTemplate.class,
                new NoSuchBeanDefinitionException(MongoTemplate.class));

        assertThat(analyzer.analyze(failure)).isNull();
    }

    @Test
    void severalTemplates_isAnAmbiguityNotAMissingStarter() throws Exception {
        Throwable failure = unsatisfied(
                ReactiveFlowWardenAutoConfiguration.class, "checkpointStore", ReactiveMongoTemplate.class,
                new NoUniqueBeanDefinitionException(ReactiveMongoTemplate.class, "primaryTemplate", "secondaryTemplate"));

        assertThat(analyzer.analyze(failure)).isNull();
    }

    @Test
    void missingBeanWithoutAnInjectionPoint_isNotOurs() {
        Throwable failure = new BeanCreationException("checkpointStore", "no injection point",
                new NoSuchBeanDefinitionException(ReactiveMongoTemplate.class));

        assertThat(analyzer.analyze(failure)).isNull();
    }

    @Test
    void unrelatedFailure_isNotOurs() {
        assertThat(analyzer.analyze(new IllegalStateException("boom"))).isNull();
    }

    // --- Discovery and priority ----------------------------------------------------------

    @Test
    void isRegisteredInSpringFactories() throws IOException {
        List<String> registered = new ArrayList<>();
        for (URL factories : Collections.list(getClass().getClassLoader().getResources("META-INF/spring.factories"))) {
            Properties properties = PropertiesLoaderUtils.loadProperties(new UrlResource(factories));
            registered.addAll(List.of(StringUtils.commaDelimitedListToStringArray(
                    properties.getProperty(FailureAnalyzer.class.getName(), ""))));
        }

        assertThat(registered.stream().map(String::trim))
                .contains(FlowWardenMissingTemplateFailureAnalyzer.class.getName());
    }

    @Test
    void isOrderedAheadOfBootsGenericAnalyzer_whateverTheClassPathOrder() throws ClassNotFoundException {
        // Boot sorts the analyzers it loads (AnnotationAwareOrderComparator) and keeps the first
        // non-null analysis. Among unordered analyzers the class-path order decides — which is why
        // the real-startup test below cannot tell @Order apart from luck. The generic missing-bean
        // analyzer carries no order; ours does, so it ranks first on any class path.
        Class<?> bootGeneric = Class.forName(
                "org.springframework.boot.autoconfigure.diagnostics.analyzer.NoSuchBeanDefinitionFailureAnalyzer");

        assertThat(OrderUtils.getOrder(FlowWardenMissingTemplateFailureAnalyzer.class)).isEqualTo(0);
        assertThat(OrderUtils.getOrder(bootGeneric)).isNull();
    }

    @Test
    void onARealStartupFailure_bootReportsTheFlowWardenAnalysis(CapturedOutput output) {
        // The real path: SpringApplication fails to start (synchronous starter only, REACTIVE mode →
        // 'checkpointStore' cannot get a ReactiveMongoTemplate) and hands the failure to its
        // analyzers — loaded from spring.factories, sorted by order, first non-null analysis wins.
        // What Boot logs under APPLICATION FAILED TO START must be ours, not the generic one.
        SpringApplication application = new SpringApplication(
                ReactiveHandlerConfig.class,
                MongoAutoConfiguration.class,
                MongoDataAutoConfiguration.class,
                FlowWardenAutoConfiguration.class,
                ImperativeFlowWardenAutoConfiguration.class,
                ReactiveFlowWardenAutoConfiguration.class);
        application.setWebApplicationType(WebApplicationType.NONE);
        application.setBannerMode(Banner.Mode.OFF);
        application.setRegisterShutdownHook(false);
        application.setDefaultProperties(Map.of(
                "flowwarden.default-mode", "REACTIVE",
                "spring.data.mongodb.uri", "mongodb://localhost:27017/analyzer-test"));

        assertThatThrownBy(application::run).isInstanceOf(BeansException.class);

        // Captured through Boot's own output capture: SpringApplication re-initializes the logging
        // system on start, so an appender attached beforehand would not survive to see the report.
        assertThat(output.getOut())
                .contains("APPLICATION FAILED TO START")
                .contains(FlowWardenConfigurationException.MISSING_REACTIVE_MONGO_TEMPLATE_DESCRIPTION)
                .contains("spring-boot-starter-data-mongodb-reactive")
                .doesNotContain("required a bean of type");
    }

    // --- Helpers -------------------------------------------------------------------------

    /** A bean creation that failed on parameter 0 of {@code declaringClass.method(parameterType)}. */
    private static Throwable unsatisfied(Class<?> declaringClass, String method, Class<?> parameterType,
                                         NoSuchBeanDefinitionException cause) throws NoSuchMethodException {
        return new UnsatisfiedDependencyException(
                declaringClass.getName(), method,
                new InjectionPoint(parameter(declaringClass, method, parameterType)), cause);
    }

    private static MethodParameter parameter(Class<?> declaringClass, String method, Class<?> parameterType)
            throws NoSuchMethodException {
        Method m = declaringClass.getMethod(method, parameterType);
        return MethodParameter.forExecutable(m, 0);
    }

    @Configuration(proxyBeanMethods = false)
    static class ReactiveHandlerConfig {
        @Bean
        ReactiveHandler reactiveHandler() {
            return new ReactiveHandler();
        }
    }

    @ChangeStream(collection = "orders")
    static class ReactiveHandler {
        @OnChange
        Mono<Void> handle(ChangeStreamContext<?> ctx) {
            return Mono.empty();
        }
    }

    /** Stands in for application code with a {@code @Bean} method of its own. */
    public static class UserConfig {
        public Object orderService(String template) {
            return template;
        }
    }
}
