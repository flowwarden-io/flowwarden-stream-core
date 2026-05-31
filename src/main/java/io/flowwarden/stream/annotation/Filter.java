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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares an application-side filter for a Change Stream.
 *
 * <p>The annotated method is invoked <strong>at each event</strong> to decide whether
 * the event should be forwarded to the handler methods. Unlike {@link Pipeline} which
 * runs server-side at startup, {@code @Filter} executes in Java and can use complex
 * logic, Spring beans, and service calls.</p>
 *
 * <p>Supported signatures:</p>
 * <ul>
 *   <li>{@code Predicate<ChangeStreamContext<?>> myFilter()} — no parameters, returns a predicate</li>
 *   <li>{@code boolean myFilter(ChangeStreamContext<?> ctx)} — takes the context, returns boolean</li>
 * </ul>
 *
 * <p>At most one {@code @Filter} method is allowed per {@link ChangeStream} class.</p>
 *
 * <p>Can be combined with {@link Pipeline} for a double-filtering funnel:
 * MongoDB pre-filters server-side, then Java refines application-side.</p>
 *
 * @see ChangeStream
 * @see Pipeline
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Filter {
}
