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

import io.flowwarden.stream.PipelineScope;

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
 * <p><strong>The stages apply to the change event.</strong> MongoDB runs the pipeline on the <em>change event</em>, not on the document.
 * The document sits under {@code fullDocument}; the top level of an event only
 * holds {@code operationType}, {@code fullDocument}, {@code documentKey},
 * {@code updateDescription}, {@code ns}, {@code clusterTime} and a few others. A
 * condition on a field of the document is therefore written with the
 * {@code fullDocument.} prefix:</p>
 *
 * <pre>{@code
 * @Pipeline
 * Aggregation pipeline() {
 *     return newAggregation(match(where("fullDocument.status").is("OPEN")));
 * }
 * }</pre>
 *
 * <p>{@code match(where("status").is("OPEN"))} would be sent as written and select
 * nothing: a change event has no top-level {@code status}. FlowWarden reports such
 * a condition with a WARN when the stream starts, but the stream still runs.</p>
 *
 * <p><strong>Coming from Spring Data's change stream API.</strong>
 * {@code ReactiveMongoTemplate.changeStream(...)} and a
 * {@code MessageListenerContainer} with a {@code ChangeStreamRequest} prefix the
 * fields of an {@code Aggregation} filter with {@code fullDocument.} themselves.
 * FlowWarden does not, by default: a filter moved as is keeps compiling and stops
 * matching. Either add the prefix, or declare {@link #scope() scope =
 * PipelineScope.DOCUMENT} to keep writing the filter against the document.</p>
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

    /**
     * What the field names of the pipeline are written against.
     *
     * <p>{@link PipelineScope#EVENT} (default): the stages are sent to MongoDB
     * exactly as written.</p>
     *
     * <p>{@link PipelineScope#DOCUMENT}: the field names of the {@code $match}
     * stages are prefixed with {@code fullDocument.} before the pipeline is sent,
     * so the filter is written against the document:</p>
     *
     * <pre>{@code
     * @Pipeline(scope = PipelineScope.DOCUMENT)
     * Aggregation pipeline() {
     *     return newAggregation(match(where("status").is("OPEN")));
     *     // sent as { "$match": { "fullDocument.status": "OPEN" } }
     * }
     * }</pre>
     *
     * <p>What the rewrite does, and does not:</p>
     * <ul>
     *   <li>The top-level fields of a change event are never prefixed
     *       ({@code operationType}, {@code documentKey}, {@code updateDescription},
     *       {@code ns}, {@code clusterTime}, …), nor is a name already written
     *       {@code fullDocument.…}: event conditions and document conditions can be
     *       mixed in one {@code $match}.</li>
     *   <li>{@code _id} is a field of the event — its resume token — and stays
     *       one. The document's id is written {@code documentKey._id} (present on
     *       deletes too) or {@code fullDocument._id}. Spring Data's API prefixes
     *       {@code _id}; a filter on it must be adapted, and a WARN at start says
     *       so.</li>
     *   <li>The rewrite descends into {@code $and} / {@code $or} / {@code $nor}.
     *       {@code $expr} is left alone: a {@code "$status"} expression inside it
     *       must be written {@code "$fullDocument.status"}.</li>
     *   <li>Only {@code $match} stages are rewritten, and only up to the first
     *       stage that reshapes the event ({@code $project}, {@code $addFields},
     *       {@code $set}, {@code $unset}, {@code $replaceRoot},
     *       {@code $replaceWith}): those stages, and whatever follows them, are
     *       sent as written.</li>
     *   <li>A document-scoped condition can only select events that carry a
     *       {@code fullDocument}: deletes never do, and updates only with
     *       {@code fullDocument = UPDATE_LOOKUP} (or {@code WHEN_AVAILABLE} /
     *       {@code REQUIRED}).</li>
     * </ul>
     *
     * <p>Only for a method returning {@code Aggregation} or
     * {@code List<AggregationOperation>}. A {@code List<Bson>} is raw BSON and is
     * never rewritten: {@code DOCUMENT} on such a method is rejected at startup.</p>
     */
    PipelineScope scope() default PipelineScope.EVENT;
}
