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
package io.flowwarden.stream.internal.imperative;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import io.flowwarden.stream.FullDocumentBeforeChangeMode;
import io.flowwarden.stream.FullDocumentMode;
import io.flowwarden.stream.internal.checkpoint.HeartbeatProbe;
import io.flowwarden.stream.internal.checkpoint.ProbeOutcome;
import io.flowwarden.stream.internal.discovery.ChangeStreamDefinition;
import org.bson.BsonDocument;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Driver-level {@link HeartbeatProbe} for IMPERATIVE mode: opens an ephemeral
 * cursor via {@code MongoCollection.watch(...)}, forces a bounded server read
 * with {@code tryNext()}, and reads the post-batch resume token from
 * {@code MongoChangeStreamCursor.getResumeToken()}.
 *
 * <p>This class is internal and not part of the public API.</p>
 */
final class ImperativeHeartbeatProbe implements HeartbeatProbe {

    static final long MAX_AWAIT_MILLIS = 1_000;

    private final MongoTemplate template;
    private final ChangeStreamDefinition def;
    private final List<Document> pipeline;

    ImperativeHeartbeatProbe(MongoTemplate template,
                             ChangeStreamDefinition def,
                             List<Document> pipeline) {
        this.template = template;
        this.def = def;
        this.pipeline = pipeline == null ? List.of() : pipeline;
    }

    @Override
    public ProbeOutcome probe(BsonDocument resumeAfter) {
        try {
            ChangeStreamIterable<Document> iterable = template.getCollection(def.collection())
                    .watch(pipeline)
                    .batchSize(1)
                    .maxAwaitTime(MAX_AWAIT_MILLIS, TimeUnit.MILLISECONDS)
                    .comment("flowwarden:heartbeat:" + def.streamName());
            if (resumeAfter != null) {
                iterable = iterable.resumeAfter(resumeAfter);
            }
            if (def.config().fullDocument() != FullDocumentMode.DEFAULT) {
                iterable = iterable.fullDocument(
                        FullDocument.valueOf(def.config().fullDocument().name()));
            }
            if (def.config().fullDocumentBeforeChange() != FullDocumentBeforeChangeMode.OFF) {
                iterable = iterable.fullDocumentBeforeChange(
                        FullDocumentBeforeChange.valueOf(def.config().fullDocumentBeforeChange().name()));
            }
            try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = iterable.cursor()) {
                ChangeStreamDocument<Document> event = cursor.tryNext();
                if (event != null) {
                    return ProbeOutcome.eventPending();
                }
                BsonDocument pbrt = cursor.getResumeToken();
                if (pbrt == null) {
                    return ProbeOutcome.failed(new IllegalStateException(
                            "Change stream reply carried no postBatchResumeToken"));
                }
                return ProbeOutcome.empty(pbrt);
            }
        } catch (Exception e) {
            return ProbeOutcome.failed(e);
        }
    }
}
