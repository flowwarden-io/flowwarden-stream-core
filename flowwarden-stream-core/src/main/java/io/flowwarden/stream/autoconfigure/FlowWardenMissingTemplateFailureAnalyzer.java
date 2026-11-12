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

import org.springframework.beans.factory.InjectionPoint;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.beans.factory.UnsatisfiedDependencyException;
import org.springframework.boot.diagnostics.AbstractFailureAnalyzer;
import org.springframework.boot.diagnostics.FailureAnalysis;
import org.springframework.core.MethodParameter;
import org.springframework.core.annotation.Order;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;

/**
 * Explains the startup failure of an application whose execution mode has no Mongo
 * template to run on.
 *
 * <p>The mode-specific auto-configuration builds its defaults on the template of that
 * mode ({@code checkpointStore(ReactiveMongoTemplate)}, …). Without the template those
 * beans fail first, with Spring's {@code NoSuchBeanDefinitionException}, before any
 * FlowWarden check gets to run. Spring detects the failure; this analyzer explains it,
 * with the same description and action {@link FlowWardenConfigurationValidator} would
 * have given: add the starter of the mode, or switch mode.</p>
 *
 * <p>It only speaks for failures that are provably FlowWarden's: the missing type is
 * exactly {@link MongoTemplate} or {@link ReactiveMongoTemplate}, the injection point
 * that asked for it is a {@code @Bean} method of {@link ImperativeFlowWardenAutoConfiguration}
 * or {@link ReactiveFlowWardenAutoConfiguration}, and the two agree on the mode. Any other
 * missing bean, and an ambiguity between several templates
 * ({@link NoUniqueBeanDefinitionException}), is left to Spring Boot's own analyzers.
 * Structured exceptions only — never the message text or a bean name.</p>
 *
 * <p>Ordered before Spring Boot's generic missing-bean analyzer, which has no explicit
 * order: the first analysis found wins.</p>
 */
@Order(0)
public class FlowWardenMissingTemplateFailureAnalyzer
        extends AbstractFailureAnalyzer<NoSuchBeanDefinitionException> {

    @Override
    protected FailureAnalysis analyze(Throwable rootFailure, NoSuchBeanDefinitionException cause) {
        if (cause instanceof NoUniqueBeanDefinitionException) {
            return null; // several templates: a real ambiguity, not a missing starter
        }
        Class<?> missing = cause.getBeanType();
        Class<?> requestedBy = flowWardenInjectionPointClass(rootFailure, cause);
        if (missing == null || requestedBy == null) {
            return null;
        }
        if (missing == MongoTemplate.class && requestedBy == ImperativeFlowWardenAutoConfiguration.class) {
            return new FailureAnalysis(
                    FlowWardenConfigurationException.MISSING_MONGO_TEMPLATE_DESCRIPTION,
                    FlowWardenConfigurationException.MISSING_MONGO_TEMPLATE_ACTION, cause);
        }
        if (missing == ReactiveMongoTemplate.class && requestedBy == ReactiveFlowWardenAutoConfiguration.class) {
            return new FailureAnalysis(
                    FlowWardenConfigurationException.MISSING_REACTIVE_MONGO_TEMPLATE_DESCRIPTION,
                    FlowWardenConfigurationException.MISSING_REACTIVE_MONGO_TEMPLATE_ACTION, cause);
        }
        return null;
    }

    /**
     * The declaring class of the {@code @Bean} method whose parameter could not be
     * satisfied — the innermost unsatisfied dependency above the cause, since a user bean
     * depending on a FlowWarden bean nests its own unsatisfied dependency further out.
     */
    private static Class<?> flowWardenInjectionPointClass(Throwable rootFailure,
                                                          NoSuchBeanDefinitionException cause) {
        UnsatisfiedDependencyException innermost = null;
        for (Throwable current = rootFailure; current != null && current != cause; current = current.getCause()) {
            if (current instanceof UnsatisfiedDependencyException unsatisfied) {
                innermost = unsatisfied;
            }
        }
        if (innermost == null || innermost.getInjectionPoint() == null) {
            return null;
        }
        InjectionPoint injectionPoint = innermost.getInjectionPoint();
        MethodParameter parameter = injectionPoint.getMethodParameter();
        return parameter != null ? parameter.getDeclaringClass() : null;
    }
}
