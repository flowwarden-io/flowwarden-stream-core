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
 * <p>When a {@code @Pipeline} filters out most events, the stream may settle
 * nothing for long stretches: the idle heartbeat then certifies a fresh
 * position into {@code lastSeenToken} (a bounded probe replicating this
 * exact pipeline confirms the interval empty server-side), so the resume
 * point keeps tracking the oplog head instead of aging out. See
 * {@link Checkpoint} for the dual-anchor model.</p>
 *
 * <p><strong>Do not filter out collection-lifecycle events.</strong> The
 * invalidate detection (collection drop, database drop, rename — see
 * {@code onStreamInvalidated}) relies on the stream <em>delivering</em>
 * those events; the pipeline runs server-side, before FlowWarden sees
 * anything. A {@code $match} that excludes {@code drop}, {@code dropDatabase},
 * {@code rename} or {@code invalidate} disables the detection entirely: the
 * imperative stream stalls silently until the heartbeat's failure signal,
 * and the reactive stream restarts without classifying the cause (a rename
 * would be self-healed as if it were a drop). Keep lifecycle operation
 * types out of exclusion filters.</p>
 *
 * @see ChangeStream
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Pipeline {
}
