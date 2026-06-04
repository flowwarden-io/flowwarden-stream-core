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
package io.flowwarden.stream.internal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MongoTemplateRegistryTest {

    private ApplicationContext applicationContext;
    private MongoTemplate defaultTemplate;
    private ReactiveMongoTemplate defaultReactiveTemplate;
    private MongoTemplateRegistry registry;

    @BeforeEach
    void setUp() {
        applicationContext = mock(ApplicationContext.class);
        defaultTemplate = mock(MongoTemplate.class);
        defaultReactiveTemplate = mock(ReactiveMongoTemplate.class);
        registry = new MongoTemplateRegistry(applicationContext, defaultTemplate, defaultReactiveTemplate);
    }

    @Test
    void resolveReturnsDefaultWhenRefIsNull() {
        assertThat(registry.resolve(null)).isSameAs(defaultTemplate);
    }

    @Test
    void resolveReturnsDefaultWhenRefIsEmpty() {
        assertThat(registry.resolve("")).isSameAs(defaultTemplate);
    }

    @Test
    void resolveReturnsNamedBean() {
        MongoTemplate custom = mock(MongoTemplate.class);
        when(applicationContext.getBean("customTemplate", MongoTemplate.class)).thenReturn(custom);

        assertThat(registry.resolve("customTemplate")).isSameAs(custom);
    }

    @Test
    void resolveCachesResult() {
        MongoTemplate custom = mock(MongoTemplate.class);
        when(applicationContext.getBean("cached", MongoTemplate.class)).thenReturn(custom);

        registry.resolve("cached");
        registry.resolve("cached");

        verify(applicationContext, times(1)).getBean("cached", MongoTemplate.class);
    }

    @Test
    void resolveThrowsWhenBeanNotFound() {
        when(applicationContext.getBean("missing", MongoTemplate.class))
                .thenThrow(new NoSuchBeanDefinitionException("missing"));

        assertThatThrownBy(() -> registry.resolve("missing"))
                .isInstanceOf(NoSuchBeanDefinitionException.class);
    }

    @Test
    void resolveReactiveReturnsDefaultWhenRefIsEmpty() {
        assertThat(registry.resolveReactive("")).isSameAs(defaultReactiveTemplate);
    }

    @Test
    void resolveReactiveReturnsNamedBean() {
        ReactiveMongoTemplate custom = mock(ReactiveMongoTemplate.class);
        when(applicationContext.getBean("reactiveCustom", ReactiveMongoTemplate.class)).thenReturn(custom);

        assertThat(registry.resolveReactive("reactiveCustom")).isSameAs(custom);
    }

    @Test
    void resolveReactiveCachesResult() {
        ReactiveMongoTemplate custom = mock(ReactiveMongoTemplate.class);
        when(applicationContext.getBean("reactiveCached", ReactiveMongoTemplate.class)).thenReturn(custom);

        registry.resolveReactive("reactiveCached");
        registry.resolveReactive("reactiveCached");

        verify(applicationContext, times(1)).getBean("reactiveCached", ReactiveMongoTemplate.class);
    }

    @Test
    void getDefaultTemplateReturnsDefault() {
        assertThat(registry.getDefaultTemplate()).isSameAs(defaultTemplate);
    }

    @Test
    void getDefaultReactiveTemplateReturnsDefault() {
        assertThat(registry.getDefaultReactiveTemplate()).isSameAs(defaultReactiveTemplate);
    }
}
