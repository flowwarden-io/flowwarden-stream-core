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
 *
 * <p><strong>Inheritance.</strong> The method-level annotations ({@link Pipeline},
 * {@link Filter}, {@link OnChange}, {@link OnInsert}, {@link OnUpdate}, {@link OnDelete},
 * {@link OnReplace}, {@link Reconcile}, {@link OnError}) are looked up through the whole
 * type hierarchy of the class: its superclasses and the interfaces it implements, default
 * methods included, whatever the visibility of the method. Streams that only differ by
 * their document type can therefore share their pipeline and handlers in a generic
 * superclass:</p>
 *
 * <pre>{@code
 * abstract class BaseStream<T> {
 *
 *     @Pipeline
 *     Aggregation pipeline() {
 *         return newAggregation(match(where("fullDocument.status").is("OPEN")));
 *     }
 *
 *     @OnInsert
 *     void onInsert(T document) { ... }
 * }
 *
 * @ChangeStream(name = "orders-stream", documentType = Order.class)
 * class OrdersStream extends BaseStream<Order> {
 * }
 * }</pre>
 *
 * <ul>
 *   <li>A method overridden in a subclass counts once, and the override is the one that
 *       runs. It keeps the role of the method it overrides without repeating the
 *       annotation, so an inherited handler is replaced, or neutralised with an empty
 *       body, by overriding it.</li>
 *   <li>The "at most one" rules apply to the hierarchy as a whole: an {@code @OnInsert}
 *       method in the superclass and another one, with a different name, in the subclass
 *       are two handlers and fail the startup like two in the same class.</li>
 *   <li>A parameter declared as a type variable ({@code T document}) is checked against
 *       {@link #documentType()} with the type the subclass binds it to.</li>
 * </ul>
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

    /** Automatically start the stream on {@code ApplicationReadyEvent}. */
    boolean autoStart() default true;

    /** Name of the MongoTemplate/ReactiveMongoTemplate bean to use. */
    String mongoTemplateRef() default "";
}
