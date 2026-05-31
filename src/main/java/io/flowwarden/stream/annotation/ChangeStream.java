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
package io.flowwarden.stream.annotation;

import io.flowwarden.stream.DeploymentMode;
import io.flowwarden.stream.FullDocumentBeforeChangeMode;
import io.flowwarden.stream.FullDocumentMode;

import org.bson.Document;

import org.springframework.stereotype.Component;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares a MongoDB Change Stream handler.
 *
 * <p>The annotated class becomes a Spring-managed bean whose lifecycle
 * is controlled by the FlowWarden runtime. This annotation is
 * meta-annotated with {@link Component @Component}, so there is no need
 * to add {@code @Component} separately.</p>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface ChangeStream {

    // --- Identification ---

    /** Unique stream name. Alias for {@link #name()}. */
    String value() default "";

    /** Unique stream name. Defaults to kebab-case class name if empty. */
    String name() default "";

    /** Human-readable description. */
    String description() default "";

    /** Grouping zone (discovered by Console). */
    String zone() default "";

    // --- MongoDB target ---

    /** Collection to watch. Required unless inferred from {@link #documentType()}. */
    String collection() default "";

    /** Target database. Defaults to the Spring-configured database. */
    String database() default "";

    /** Java type for deserialization. */
    Class<?> documentType() default Document.class;

    // --- Operations ---

    /** Full document inclusion mode. */
    FullDocumentMode fullDocument() default FullDocumentMode.DEFAULT;

    /** Pre-image inclusion mode (MongoDB 6.0+). */
    FullDocumentBeforeChangeMode fullDocumentBeforeChange() default FullDocumentBeforeChangeMode.OFF;

    // --- Deployment ---

    /** Multi-instance deployment strategy. */
    DeploymentMode deploymentMode() default DeploymentMode.ALL_INSTANCES;

    // --- Behaviour ---

    /** Enable/disable this stream. */
    boolean enabled() default true;

    /** Automatically start the stream on context refresh. */
    boolean autoStart() default true;

    /** Name of the MongoTemplate/ReactiveMongoTemplate bean to use. */
    String mongoTemplateRef() default "";
}
