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
package io.flowwarden.stream.internal.retry;

import io.flowwarden.stream.annotation.RetryPolicy;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Immutable configuration parsed from a {@link RetryPolicy} annotation.
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public record RetryPolicyConfig(
        int maxAttempts,
        Duration initialDelay,
        Duration maxDelay,
        double multiplier,
        Set<Class<? extends Throwable>> retryOn,
        Set<Class<? extends Throwable>> noRetryOn,
        boolean jitter) {

    private static final Pattern DURATION_PATTERN = Pattern.compile("(\\d+)(ms|s|m)");

    /**
     * Creates a {@link RetryPolicyConfig} from the given annotation.
     */
    public static RetryPolicyConfig fromAnnotation(RetryPolicy ann) {
        return new RetryPolicyConfig(
                ann.maxAttempts(),
                parseDuration(ann.initialDelay()),
                parseDuration(ann.maxDelay()),
                ann.multiplier(),
                Set.of(ann.retryOn()),
                Set.of(ann.noRetryOn()),
                ann.jitter());
    }

    /**
     * Returns {@code true} if the given exception should be retried at the current attempt.
     *
     * @param ex             the exception thrown by the handler
     * @param currentAttempt the attempt number that just failed (1-based)
     * @return true if retry should be attempted
     */
    public boolean shouldRetry(Throwable ex, int currentAttempt) {
        if (currentAttempt >= maxAttempts) {
            return false;
        }

        // noRetryOn takes precedence
        for (Class<? extends Throwable> noRetry : noRetryOn) {
            if (noRetry.isInstance(ex)) {
                return false;
            }
        }

        // If retryOn is specified, only retry those
        if (!retryOn.isEmpty()) {
            for (Class<? extends Throwable> retry : retryOn) {
                if (retry.isInstance(ex)) {
                    return true;
                }
            }
            return false;
        }

        // Empty retryOn = retry all (except noRetryOn already checked)
        return true;
    }

    /**
     * Computes the delay in milliseconds for the given attempt number.
     *
     * <p>Formula: {@code min(initialDelay * multiplier^(attempt-1), maxDelay)} with optional jitter.</p>
     *
     * @param attemptNumber the attempt number that just failed (1-based)
     * @return delay in milliseconds before the next retry
     */
    public long computeDelayMillis(int attemptNumber) {
        double baseDelay = initialDelay.toMillis() * Math.pow(multiplier, attemptNumber - 1);
        long cappedDelay = Math.min((long) baseDelay, maxDelay.toMillis());
        if (jitter) {
            double jitterFactor = 0.8 + ThreadLocalRandom.current().nextDouble() * 0.4; // ±20%
            return Math.max(0, (long) (cappedDelay * jitterFactor));
        }
        return cappedDelay;
    }

    /**
     * Parses a duration string like "500ms", "30s", "1m".
     */
    public static Duration parseDuration(String value) {
        Matcher matcher = DURATION_PATTERN.matcher(value.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid duration format: '" + value
                    + "'. Expected format: <number><unit> where unit is ms, s, or m");
        }
        long amount = Long.parseLong(matcher.group(1));
        String unit = matcher.group(2);
        return switch (unit) {
            case "ms" -> Duration.ofMillis(amount);
            case "s" -> Duration.ofSeconds(amount);
            case "m" -> Duration.ofMinutes(amount);
            default -> throw new IllegalArgumentException("Unsupported duration unit: " + unit);
        };
    }
}
