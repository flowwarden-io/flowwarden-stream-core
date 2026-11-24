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
package io.flowwarden.stream;

/**
 * What the field names of a server-side pipeline are written against.
 *
 * <p>MongoDB applies the stages of a change stream pipeline to the
 * <em>change event</em>, in which the document sits under
 * {@code fullDocument}. The scope chooses whether FlowWarden sends the stages
 * as written or rewrites them so they can be written against the document.</p>
 *
 * @see io.flowwarden.stream.annotation.Pipeline#scope()
 */
public enum PipelineScope {

    /**
     * The stages are sent to MongoDB exactly as written: a field name
     * designates a field of the change event ({@code operationType},
     * {@code fullDocument.status}, {@code documentKey._id}, …). Default.
     */
    EVENT,

    /**
     * The {@code $match} stages are written against the document: a field
     * name that is not a top-level field of the change event is prefixed with
     * {@code fullDocument.} before the pipeline is sent — {@code status}
     * becomes {@code fullDocument.status}. This is how Spring Data's own
     * change stream API reads an {@code Aggregation} filter.
     *
     * <p>Only for pipelines declared as an {@code Aggregation} or a
     * {@code List<AggregationOperation>}; a raw {@code List<Bson>} pipeline
     * is never rewritten and is rejected at startup with this scope.</p>
     */
    DOCUMENT
}
