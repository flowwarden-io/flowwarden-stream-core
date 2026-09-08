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
package io.flowwarden.stream.registration;

import io.flowwarden.stream.annotation.RetryPolicy;

import java.util.List;
import java.util.Objects;

/**
 * Plain-value equivalent of {@link RetryPolicy}, for streams contributed via
 * {@link StreamDefinitionContributor} instead of annotated.
 *
 * <p>See {@link RetryPolicy} for the meaning of each attribute. Built only through
 * {@link #builder()}, not a canonical constructor, so a future attribute addition doesn't
 * break existing callers. {@code retryOn}/{@code noRetryOn} are defensively copied.</p>
 */
public final class RetryPolicySpec {

    private final int maxAttempts;
    private final String initialDelay;
    private final String maxDelay;
    private final double multiplier;
    private final List<Class<? extends Throwable>> retryOn;
    private final List<Class<? extends Throwable>> noRetryOn;
    private final boolean jitter;

    private RetryPolicySpec(Builder builder) {
        this.maxAttempts = builder.maxAttempts;
        this.initialDelay = builder.initialDelay;
        this.maxDelay = builder.maxDelay;
        this.multiplier = builder.multiplier;
        this.retryOn = List.copyOf(builder.retryOn);
        this.noRetryOn = List.copyOf(builder.noRetryOn);
        this.jitter = builder.jitter;
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    public String initialDelay() {
        return initialDelay;
    }

    public String maxDelay() {
        return maxDelay;
    }

    public double multiplier() {
        return multiplier;
    }

    /** Exception types that should trigger a retry (unmodifiable). Empty means all exceptions. */
    public List<Class<? extends Throwable>> retryOn() {
        return retryOn;
    }

    /** Exception types that should NOT trigger a retry (unmodifiable). Takes precedence over {@link #retryOn()}. */
    public List<Class<? extends Throwable>> noRetryOn() {
        return noRetryOn;
    }

    public boolean jitter() {
        return jitter;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Same defaults as {@link RetryPolicy}'s unspecified attributes. */
    public static RetryPolicySpec defaults() {
        return builder().build();
    }

    public static final class Builder {

        private int maxAttempts = 3;
        private String initialDelay = "500ms";
        private String maxDelay = "30s";
        private double multiplier = 2.0;
        private List<Class<? extends Throwable>> retryOn = List.of();
        private List<Class<? extends Throwable>> noRetryOn =
                List.of(IllegalArgumentException.class, NullPointerException.class, ClassCastException.class);
        private boolean jitter = true;

        private Builder() {
        }

        public Builder maxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
            return this;
        }

        public Builder initialDelay(String initialDelay) {
            this.initialDelay = Objects.requireNonNull(initialDelay, "initialDelay must not be null");
            return this;
        }

        public Builder maxDelay(String maxDelay) {
            this.maxDelay = Objects.requireNonNull(maxDelay, "maxDelay must not be null");
            return this;
        }

        public Builder multiplier(double multiplier) {
            this.multiplier = multiplier;
            return this;
        }

        public Builder retryOn(List<Class<? extends Throwable>> retryOn) {
            this.retryOn = List.copyOf(Objects.requireNonNull(retryOn, "retryOn must not be null"));
            return this;
        }

        public Builder noRetryOn(List<Class<? extends Throwable>> noRetryOn) {
            this.noRetryOn = List.copyOf(Objects.requireNonNull(noRetryOn, "noRetryOn must not be null"));
            return this;
        }

        public Builder jitter(boolean jitter) {
            this.jitter = jitter;
            return this;
        }

        public RetryPolicySpec build() {
            return new RetryPolicySpec(this);
        }
    }
}
