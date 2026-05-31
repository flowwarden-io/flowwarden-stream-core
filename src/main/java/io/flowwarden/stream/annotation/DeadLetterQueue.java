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
 * Enables automatic Dead Letter Queue (DLQ) for failed handler invocations.
 *
 * <p>Place this annotation on a class that is also annotated with
 * {@link ChangeStream}. When a handler throws an exception and all retries
 * (if {@link RetryPolicy} is present) are exhausted, the event is
 * automatically sent to the DLQ instead of being lost.</p>
 *
 * <p>Works with or without {@link RetryPolicy}:
 * <ul>
 *   <li>With {@code @RetryPolicy}: event is sent to DLQ after all retries are exhausted</li>
 *   <li>Without {@code @RetryPolicy}: event is sent to DLQ after the first failure</li>
 * </ul>
 * </p>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface DeadLetterQueue {

    /** Whether DLQ is enabled. */
    boolean enabled() default true;

    /** MongoDB collection name for the DLQ. */
    String collection() default "_fw_dlq";

    /** Time-to-live in days. 0 means permanent (no expiry). */
    int ttlDays() default 30;

    /** Whether to include the original document in the DLQ entry. */
    boolean includeOriginalDocument() default true;

    /** Whether to include the full stack trace in the DLQ entry. */
    boolean includeStackTrace() default true;
}
