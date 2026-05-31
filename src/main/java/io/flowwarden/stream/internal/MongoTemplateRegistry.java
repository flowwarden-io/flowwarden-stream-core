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

import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves {@link MongoTemplate} or {@link ReactiveMongoTemplate} beans by name.
 * Used internally to support {@code @ChangeStream(mongoTemplateRef = "...")}.
 *
 * <p>When the ref is empty, the default template is returned.
 * Resolved beans are cached for the lifetime of the application context.</p>
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public class MongoTemplateRegistry {

    private final ApplicationContext applicationContext;
    private final MongoTemplate defaultTemplate;             // nullable in reactive mode
    private final ReactiveMongoTemplate defaultReactiveTemplate; // nullable in imperative mode
    private final Map<String, MongoTemplate> cache = new ConcurrentHashMap<>();
    private final Map<String, ReactiveMongoTemplate> reactiveCache = new ConcurrentHashMap<>();

    public MongoTemplateRegistry(ApplicationContext applicationContext,
                                 MongoTemplate defaultTemplate,
                                 ReactiveMongoTemplate defaultReactiveTemplate) {
        this.applicationContext = applicationContext;
        this.defaultTemplate = defaultTemplate;
        this.defaultReactiveTemplate = defaultReactiveTemplate;
    }

    /**
     * Resolves a {@link MongoTemplate} by bean name.
     *
     * @param ref the bean name, or empty/null for the default template
     * @return the resolved template
     */
    public MongoTemplate resolve(String ref) {
        if (ref == null || ref.isEmpty()) {
            return defaultTemplate;
        }
        return cache.computeIfAbsent(ref,
                name -> applicationContext.getBean(name, MongoTemplate.class));
    }

    /**
     * Resolves a {@link ReactiveMongoTemplate} by bean name.
     *
     * @param ref the bean name, or empty/null for the default template
     * @return the resolved template
     */
    public ReactiveMongoTemplate resolveReactive(String ref) {
        if (ref == null || ref.isEmpty()) {
            return defaultReactiveTemplate;
        }
        return reactiveCache.computeIfAbsent(ref,
                name -> applicationContext.getBean(name, ReactiveMongoTemplate.class));
    }

    /** Returns the default (non-custom) template. Used for infrastructure operations (oplog stats). */
    public MongoTemplate getDefaultTemplate() {
        return defaultTemplate;
    }

    /** Returns the default reactive template. Used for infrastructure operations (oplog stats). */
    public ReactiveMongoTemplate getDefaultReactiveTemplate() {
        return defaultReactiveTemplate;
    }
}
