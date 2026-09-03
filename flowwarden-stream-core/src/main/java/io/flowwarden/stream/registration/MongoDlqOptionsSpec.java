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

import io.flowwarden.stream.annotation.MongoDlqOptions;

import java.util.Objects;

/**
 * Plain-value equivalent of {@link MongoDlqOptions}, for streams contributed via
 * {@link StreamDefinitionContributor} instead of annotated.
 *
 * <p>Built only through {@link #builder()}, not a canonical constructor, so a future
 * attribute addition doesn't break existing callers.</p>
 */
public final class MongoDlqOptionsSpec {

    private final String collection;

    private MongoDlqOptionsSpec(Builder builder) {
        this.collection = builder.collection;
    }

    /** MongoDB collection name for this stream's DLQ entries; empty means the global default applies. */
    public String collection() {
        return collection;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Same default as {@link MongoDlqOptions}'s unspecified attribute. */
    public static MongoDlqOptionsSpec defaults() {
        return builder().build();
    }

    public static final class Builder {

        private String collection = "";

        private Builder() {
        }

        public Builder collection(String collection) {
            this.collection = Objects.requireNonNull(collection, "collection must not be null");
            return this;
        }

        public MongoDlqOptionsSpec build() {
            return new MongoDlqOptionsSpec(this);
        }
    }
}
