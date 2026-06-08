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
 * Enables automatic retry with exponential backoff for failed handler invocations.
 *
 * <p>Place this annotation on a class that is also annotated with
 * {@link ChangeStream}. When a handler throws an exception, the framework
 * will retry the invocation up to {@link #maxAttempts()} times with
 * exponential backoff between attempts.</p>
 *
 * <p>Backoff formula:</p>
 * <pre>
 * baseDelay = initialDelay * (multiplier ^ (attemptNumber - 1))
 * cappedDelay = min(baseDelay, maxDelay)
 * if jitter: finalDelay = cappedDelay +/- random(20%)
 * else: finalDelay = cappedDelay
 * </pre>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RetryPolicy {

    /** Maximum number of attempts (including the initial one). */
    int maxAttempts() default 3;

    /** Initial delay before the first retry. Supports "500ms", "1s", "1m" format. */
    String initialDelay() default "500ms";

    /** Maximum delay cap. Supports "500ms", "1s", "1m" format. */
    String maxDelay() default "30s";

    /** Multiplier applied to the delay between each retry. */
    double multiplier() default 2.0;

    /**
     * Exception types that should trigger a retry. Empty means all exceptions
     * (except those listed in {@link #noRetryOn()}).
     */
    Class<? extends Throwable>[] retryOn() default {};

    /**
     * Exception types that should NOT trigger a retry (immediate failure).
     * Takes precedence over {@link #retryOn()}.
     */
    Class<? extends Throwable>[] noRetryOn() default {
            IllegalArgumentException.class,
            NullPointerException.class,
            ClassCastException.class
    };

    /** Whether to add random jitter (+/- 20%) to the computed delay. */
    boolean jitter() default true;
}
