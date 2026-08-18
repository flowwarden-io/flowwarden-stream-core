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
import io.flowwarden.stream.internal.checkpoint.ChangeStreamProbeCommands;
import io.flowwarden.stream.internal.checkpoint.HeartbeatProbe;
import io.flowwarden.stream.internal.checkpoint.ProbeOutcome;
import io.flowwarden.stream.internal.discovery.ChangeStreamDefinition;
import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.List;
import java.util.Objects;
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

    private static final Logger log = LoggerFactory.getLogger(ImperativeHeartbeatProbe.class);

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
        Objects.requireNonNull(resumeAfter, "resumeAfter must not be null — use initialPosition()");
        try {
            try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor =
                         buildIterable().resumeAfter(resumeAfter).cursor()) {
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

    @Override
    public BsonDocument initialPosition() {
        // The initial aggregate reply carries a PBRT at cursor-open time,
        // before any event can be returned — so there is no probe result to
        // discard and no unprotected hand-off window. The sync driver's cursor
        // only caches the PBRT after an iteration (which could return an
        // event), so the reply is read via a raw command instead.
        Document reply = template.executeCommand(
                ChangeStreamProbeCommands.aggregateCommand(def, pipeline, null));
        long cursorId = ChangeStreamProbeCommands.cursorId(reply);
        try {
            return ChangeStreamProbeCommands.initialPbrt(reply, def);
        } finally {
            if (cursorId != 0) {
                try {
                    template.executeCommand(
                            ChangeStreamProbeCommands.killCursorsCommand(def, cursorId));
                } catch (Exception e) {
                    log.debug("Failed to kill bootstrap probe cursor for stream '{}': {}",
                            def.streamName(), e.getMessage());
                }
            }
        }
    }

    private ChangeStreamIterable<Document> buildIterable() {
        ChangeStreamIterable<Document> iterable = template.getCollection(def.collection())
                .watch(pipeline)
                .batchSize(1)
                .maxAwaitTime(MAX_AWAIT_MILLIS, TimeUnit.MILLISECONDS)
                .comment("flowwarden:heartbeat:" + def.streamName());
        if (def.config().fullDocument() != FullDocumentMode.DEFAULT) {
            iterable = iterable.fullDocument(
                    FullDocument.valueOf(def.config().fullDocument().name()));
        }
        if (def.config().fullDocumentBeforeChange() != FullDocumentBeforeChangeMode.OFF) {
            iterable = iterable.fullDocumentBeforeChange(
                    FullDocumentBeforeChange.valueOf(def.config().fullDocumentBeforeChange().name()));
        }
        return iterable;
    }
}
