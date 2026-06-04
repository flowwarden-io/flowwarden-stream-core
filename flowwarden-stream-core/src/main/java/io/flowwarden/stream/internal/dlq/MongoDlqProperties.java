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
package io.flowwarden.stream.internal.dlq;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Backend-level defaults for the MongoDB-backed {@code DlqStore}.
 *
 * <p>Bound to {@code flowwarden.dlq.mongo.*}. Per-stream overrides live on the
 * stream class via {@code @MongoDlqOptions} — there is no per-stream YAML
 * configuration.</p>
 *
 * <p>This class is internal and not part of the public API.</p>
 */
@ConfigurationProperties("flowwarden.dlq.mongo")
public class MongoDlqProperties {

    /**
     * Global default collection name for DLQ entries. Streams without
     * {@code @MongoDlqOptions(collection = "...")} write here.
     */
    private String collection = "_fw_dlq";

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }
}
