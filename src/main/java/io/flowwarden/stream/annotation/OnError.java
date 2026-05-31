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
 * Marks a method as an error handler for a {@link ChangeStream} class.
 *
 * <p>When a handler method throws an exception, FlowWarden consults {@code @OnError}
 * methods <em>before</em> the standard retry/DLQ chain. The method must return an
 * {@link io.flowwarden.stream.ErrorAction} to decide what happens next.</p>
 *
 * <p>Supported signature:</p>
 * <pre>{@code
 * ErrorAction method(Throwable ex, ChangeStreamContext<?> ctx)
 * }</pre>
 *
 * <p>Multiple {@code @OnError} methods are allowed on different exception types.
 * Resolution order: exact type match → parent type match → catch-all (empty {@code value}).</p>
 *
 * @see io.flowwarden.stream.ErrorAction
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface OnError {
    /** Types of exceptions handled. Empty = catch-all. */
    Class<? extends Throwable>[] value() default {};
}
