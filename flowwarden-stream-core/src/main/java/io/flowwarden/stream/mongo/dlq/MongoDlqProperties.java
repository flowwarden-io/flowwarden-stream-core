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
package io.flowwarden.stream.mongo.dlq;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Backend-level defaults for the MongoDB-backed {@code DlqStore}.
 *
 * <p>Bound to {@code flowwarden.dlq.mongo.*}. Per-stream overrides live on the
 * stream class via {@code @MongoDlqOptions} — there is no per-stream YAML
 * configuration.</p>
 *
 * <p>Public API: the auto-configuration registers it as a bean, and it is the
 * second constructor argument of {@link MongoDlqStore} and
 * {@link ReactiveMongoDlqStore} for an application that redeclares the
 * {@code DlqStore} bean on another template.</p>
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
