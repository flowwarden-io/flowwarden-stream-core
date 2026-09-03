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

import io.flowwarden.stream.annotation.DeadLetterQueue;

/**
 * Plain-value equivalent of {@link DeadLetterQueue}, for streams contributed via
 * {@link StreamDefinitionContributor} instead of annotated.
 *
 * <p>See {@link DeadLetterQueue} for the meaning of each attribute. Built only through
 * {@link #builder()}, not a canonical constructor, so a future attribute addition doesn't
 * break existing callers.</p>
 */
public final class DeadLetterQueueSpec {

    private final boolean enabled;
    private final int retentionDays;
    private final boolean includeOriginalDocument;
    private final boolean includeStackTrace;

    private DeadLetterQueueSpec(Builder builder) {
        this.enabled = builder.enabled;
        this.retentionDays = builder.retentionDays;
        this.includeOriginalDocument = builder.includeOriginalDocument;
        this.includeStackTrace = builder.includeStackTrace;
    }

    public boolean enabled() {
        return enabled;
    }

    public int retentionDays() {
        return retentionDays;
    }

    public boolean includeOriginalDocument() {
        return includeOriginalDocument;
    }

    public boolean includeStackTrace() {
        return includeStackTrace;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Same defaults as {@link DeadLetterQueue}'s unspecified attributes. */
    public static DeadLetterQueueSpec defaults() {
        return builder().build();
    }

    public static final class Builder {

        private boolean enabled = true;
        private int retentionDays = 30;
        private boolean includeOriginalDocument = true;
        private boolean includeStackTrace = true;

        private Builder() {
        }

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder retentionDays(int retentionDays) {
            this.retentionDays = retentionDays;
            return this;
        }

        public Builder includeOriginalDocument(boolean includeOriginalDocument) {
            this.includeOriginalDocument = includeOriginalDocument;
            return this;
        }

        public Builder includeStackTrace(boolean includeStackTrace) {
            this.includeStackTrace = includeStackTrace;
            return this;
        }

        public DeadLetterQueueSpec build() {
            return new DeadLetterQueueSpec(this);
        }
    }
}
