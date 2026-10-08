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
package io.flowwarden.stream.internal.restart;

import io.flowwarden.stream.annotation.RestartPolicy;
import io.flowwarden.stream.internal.retry.RetryPolicyConfig;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Immutable configuration parsed from a {@link RestartPolicy} annotation —
 * the per-stream policy driving {@link io.flowwarden.stream.internal.StreamRestarter}.
 *
 * <p>{@link #DEFAULTS} is the policy of a stream without the annotation:
 * 1s doubling up to 60s, no jitter, unlimited attempts.</p>
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public record RestartPolicyConfig(
        int maxAttempts,
        Duration initialDelay,
        Duration maxDelay,
        double multiplier,
        boolean jitter) {

    /** Same values as {@link RestartPolicy}'s unspecified attributes. */
    public static final RestartPolicyConfig DEFAULTS = new RestartPolicyConfig(
            0, Duration.ofSeconds(1), Duration.ofSeconds(60), 2.0, false);

    /**
     * Creates a {@link RestartPolicyConfig} from the given annotation.
     */
    public static RestartPolicyConfig fromAnnotation(RestartPolicy ann) {
        return new RestartPolicyConfig(
                ann.maxAttempts(),
                RetryPolicyConfig.parseDuration(ann.initialDelay()),
                RetryPolicyConfig.parseDuration(ann.maxDelay()),
                ann.multiplier(),
                ann.jitter());
    }

    /** Whether attempts are capped ({@code maxAttempts > 0}). */
    public boolean isBounded() {
        return maxAttempts > 0;
    }

    /**
     * Returns {@code true} when the given attempt number lies beyond the
     * configured ceiling — the loop must give up instead of running it.
     *
     * @param attemptNumber the attempt about to be scheduled (1-based)
     */
    public boolean isExhausted(int attemptNumber) {
        return isBounded() && attemptNumber > maxAttempts;
    }

    /**
     * Computes the delay in milliseconds before the given attempt.
     *
     * <p>Formula: {@code min(initialDelay * multiplier^(attempt-1), maxDelay)} with optional jitter.</p>
     *
     * @param attemptNumber the attempt about to be scheduled (1-based)
     * @return delay in milliseconds before that attempt runs
     */
    public long computeDelayMillis(int attemptNumber) {
        double baseDelay = initialDelay.toMillis() * Math.pow(multiplier, attemptNumber - 1);
        long cappedDelay = Double.isFinite(baseDelay)
                ? Math.min((long) baseDelay, maxDelay.toMillis())
                : maxDelay.toMillis();
        if (jitter) {
            double jitterFactor = 0.8 + ThreadLocalRandom.current().nextDouble() * 0.4; // ±20%
            // Floor at 1ms, never 0: the delays are validated strictly positive
            // so the loop never hot-loops, and a sub-1.0 factor on a 1ms delay
            // would truncate to zero and defeat that guarantee.
            return Math.max(1L, (long) (cappedDelay * jitterFactor));
        }
        return cappedDelay;
    }
}
