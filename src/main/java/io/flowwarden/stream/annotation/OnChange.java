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
 * Catch-all handler for Change Stream events.
 *
 * <p>Fires on every operation that is not covered by a typed handler
 * ({@code @OnInsert}, {@code @OnUpdate}, {@code @OnDelete}, {@code @OnReplace})
 * in the same class. This includes {@code DROP} and {@code INVALIDATE}, which
 * have no dedicated typed handler.</p>
 *
 * <p>Only one {@code @OnChange} method is allowed per {@link ChangeStream} class.</p>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface OnChange {
}
