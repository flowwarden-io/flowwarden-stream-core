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
 * Tunes the managed restart loop of a stream: how it backs off between
 * resubscription attempts after a <em>runtime</em> death (cursor death —
 * network outage, primary stepdown, non-resumable server error, collection
 * invalidation) and whether it ever gives up.
 *
 * <p>Place this annotation on a class that is also annotated with
 * {@link ChangeStream}. Without it, every stream uses the defaults below —
 * exponential backoff from 1s doubling up to 60s, no jitter, unlimited
 * attempts — which is exactly the behaviour of a stream without the
 * annotation. Each attempt re-enters the full startup path (resume cascade
 * included); the attempt counter resets on the first successful
 * resubscription.</p>
 *
 * <p>Backoff formula (same shape as {@link RetryPolicy}):</p>
 * <pre>
 * baseDelay = initialDelay * (multiplier ^ (attemptNumber - 1))
 * cappedDelay = min(baseDelay, maxDelay)
 * if jitter: finalDelay = cappedDelay +/- random(20%)
 * else: finalDelay = cappedDelay
 * </pre>
 *
 * <p><strong>Giving up.</strong> A {@link io.flowwarden.stream.HistoryLostException}
 * escaping a restart attempt is always terminal, whatever {@link #maxAttempts()}
 * says. Exhausting {@link #maxAttempts()} takes the same terminal path: the
 * stream is reported stopped ({@code CRASHED}, with the last failure as cause),
 * the loop stops for that stream, and under {@code SINGLE_LEADER} the lease
 * is released so a standby can take over. An operator {@code startStream}
 * restarts it.</p>
 *
 * <p><strong>Differences from {@code @RetryPolicy}</strong>, on purpose:
 * {@link #maxAttempts()} counts resubscription attempts (there is no initial
 * attempt to exclude) and {@code 0} means unlimited, the default; and
 * {@link #jitter()} defaults to {@code false}, so that a stream without the
 * annotation keeps the deterministic cadence it had before the annotation
 * existed. Turn jitter on when many streams share one cluster — they would
 * otherwise all resubscribe inside the same window when it comes back.</p>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RestartPolicy {

    /** Resubscription attempts before giving up. {@code 0} = unlimited (the default). */
    int maxAttempts() default 0;

    /** Delay before the first resubscription attempt. Supports "500ms", "1s", "1m" format. Must be &gt; 0. */
    String initialDelay() default "1s";

    /** Maximum delay cap. Supports "500ms", "1s", "1m" format. Must be &gt; 0. */
    String maxDelay() default "60s";

    /** Multiplier applied to the delay between each attempt. Must be &gt;= 1 (the loop never speeds up). */
    double multiplier() default 2.0;

    /**
     * Whether to add random jitter (+/- 20%) to the computed delay — avoids a
     * thundering herd when a shared cluster returns. Off by default.
     */
    boolean jitter() default false;
}
