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

import io.flowwarden.stream.annotation.RestartPolicy;

import java.util.Objects;

/**
 * Plain-value equivalent of {@link RestartPolicy}, for streams contributed via
 * {@link StreamDefinitionContributor} instead of annotated.
 *
 * <p>See {@link RestartPolicy} for the meaning of each attribute. Built only through
 * {@link #builder()}, not a canonical constructor, so a future attribute addition doesn't
 * break existing callers.</p>
 */
public final class RestartPolicySpec {

    private final int maxAttempts;
    private final String initialDelay;
    private final String maxDelay;
    private final double multiplier;
    private final boolean jitter;

    private RestartPolicySpec(Builder builder) {
        this.maxAttempts = builder.maxAttempts;
        this.initialDelay = builder.initialDelay;
        this.maxDelay = builder.maxDelay;
        this.multiplier = builder.multiplier;
        this.jitter = builder.jitter;
    }

    /** Resubscription attempts before giving up; {@code 0} = unlimited. */
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

    public boolean jitter() {
        return jitter;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Same defaults as {@link RestartPolicy}'s unspecified attributes. */
    public static RestartPolicySpec defaults() {
        return builder().build();
    }

    public static final class Builder {

        private int maxAttempts = 0;
        private String initialDelay = "1s";
        private String maxDelay = "60s";
        private double multiplier = 2.0;
        private boolean jitter = false;

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

        public Builder jitter(boolean jitter) {
            this.jitter = jitter;
            return this;
        }

        public RestartPolicySpec build() {
            return new RestartPolicySpec(this);
        }
    }
}
