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
 * Typed handler for REPLACE Change Stream events.
 *
 * <p>Supported signatures:</p>
 * <ul>
 *   <li>{@code void handle(ChangeStreamContext ctx)}</li>
 *   <li>{@code void handle(T doc)} — requires {@code documentType} on {@link ChangeStream}</li>
 *   <li>{@code void handle(T doc, ChangeStreamContext ctx)}</li>
 * </ul>
 *
 * <p>Only one {@code @OnReplace} method is allowed per {@link ChangeStream} class.</p>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface OnReplace {
}
