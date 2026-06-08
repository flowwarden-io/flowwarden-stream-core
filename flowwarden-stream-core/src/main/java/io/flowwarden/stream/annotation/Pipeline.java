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
 * Declares a server-side aggregation pipeline for a Change Stream.
 *
 * <p>The annotated method is invoked <strong>once</strong> at stream startup to obtain
 * the pipeline stages that MongoDB will apply server-side. Only events matching
 * the pipeline are transmitted to the client, reducing network traffic.</p>
 *
 * <p>Supported return types:</p>
 * <ul>
 *   <li>{@code List<Bson>} — native BSON pipeline</li>
 *   <li>{@code List<AggregationOperation>} — Spring Data MongoDB operations</li>
 *   <li>{@code Aggregation} — Spring Data MongoDB aggregation object</li>
 * </ul>
 *
 * <p>At most one {@code @Pipeline} method is allowed per {@link ChangeStream} class.</p>
 *
 * <p>When a {@code @Pipeline} is present, FlowWarden automatically advances a separate
 * {@code lastSeenToken} via periodic oplog sampling, independently of which events
 * matched the pipeline. This ensures fast restart even when the pipeline filters out
 * most events. See {@link Checkpoint} for the dual-token model.</p>
 *
 * @see ChangeStream
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Pipeline {
}
