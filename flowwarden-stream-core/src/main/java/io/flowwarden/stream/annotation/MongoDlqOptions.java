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
 * Backend-specific overrides for the MongoDB-backed {@code DlqStore}.
 *
 * <p>Used together with {@link DeadLetterQueue} when the user wants to route
 * failed events for a given stream to a non-default MongoDB collection. If
 * absent, the collection name falls back to the global default configured via
 * {@code flowwarden.dlq.mongo.collection} (defaults to {@code _fw_dlq}).</p>
 *
 * <p>This annotation has no effect when the configured {@code DlqStore}
 * implementation is non-Mongo (e.g. Kafka, Rabbit, JDBC) — each backend ships
 * its own companion annotation.</p>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface MongoDlqOptions {

    /**
     * MongoDB collection name to use for this stream's DLQ entries.
     * Empty (default) means the global default from
     * {@code flowwarden.dlq.mongo.collection} applies.
     */
    String collection() default "";
}
