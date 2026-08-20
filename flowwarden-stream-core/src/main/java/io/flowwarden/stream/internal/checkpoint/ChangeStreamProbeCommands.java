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
package io.flowwarden.stream.internal.checkpoint;

import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import io.flowwarden.stream.FullDocumentBeforeChangeMode;
import io.flowwarden.stream.FullDocumentMode;
import io.flowwarden.stream.internal.discovery.ChangeStreamDefinition;
import org.bson.BsonDocument;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * Raw command plumbing shared by the heartbeat probes.
 *
 * <p>Rationale: neither high-level driver API exposes the post-batch resume
 * token of a change stream's <em>initial</em> aggregate reply (the sync
 * cursor only caches it after an iteration; the reactive publisher never
 * exposes it), yet the server sends it — so the initial-position capture
 * reads the reply of a hand-built {@code aggregate} command.</p>
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public final class ChangeStreamProbeCommands {

    private ChangeStreamProbeCommands() {
    }

    /**
     * Builds the change stream {@code aggregate} command replicating the
     * stream's pipeline and {@code fullDocument} options, comment-stamped for
     * cursor attribution.
     */
    public static Document aggregateCommand(ChangeStreamDefinition def,
                                            List<Document> pipeline,
                                            BsonDocument resumeAfter) {
        return aggregateCommand(def, pipeline, resumeAfter, null);
    }

    /**
     * Variant with an optional {@code startAtOperationTime} (mutually
     * exclusive with {@code resumeAfter}) for the
     * {@code RESUME_FROM_OPLOG_START} recovery probe.
     */
    public static Document aggregateCommand(ChangeStreamDefinition def,
                                            List<Document> pipeline,
                                            BsonDocument resumeAfter,
                                            org.bson.BsonTimestamp startAtOperationTime) {
        Document changeStreamStage = new Document();
        if (resumeAfter != null) {
            changeStreamStage.append("resumeAfter", Document.parse(resumeAfter.toJson()));
        }
        if (startAtOperationTime != null) {
            changeStreamStage.append("startAtOperationTime", startAtOperationTime);
        }
        if (def.config().fullDocument() != FullDocumentMode.DEFAULT) {
            changeStreamStage.append("fullDocument",
                    FullDocument.valueOf(def.config().fullDocument().name()).getValue());
        }
        if (def.config().fullDocumentBeforeChange() != FullDocumentBeforeChangeMode.OFF) {
            changeStreamStage.append("fullDocumentBeforeChange",
                    FullDocumentBeforeChange.valueOf(def.config().fullDocumentBeforeChange().name())
                            .getValue());
        }
        List<Document> stages = new ArrayList<>();
        stages.add(new Document("$changeStream", changeStreamStage));
        if (pipeline != null) {
            stages.addAll(pipeline);
        }
        return new Document("aggregate", def.collection())
                .append("pipeline", stages)
                .append("cursor", new Document("batchSize", 1))
                // Bound the cursor CREATION on the server: probe aggregates
                // run on lifecycle paths (bootstrap, post-invalidate repair)
                // and must not execute open-endedly server-side. This is a
                // server-side bound ONLY — it caps neither server selection
                // nor a client socket read; a strict wall-clock bound has to
                // come from the Mongo client's own timeouts. The subsequent
                // getMore waits are bounded separately by the probes'
                // maxAwaitTime.
                .append("maxTimeMS", AGGREGATE_MAX_TIME_MS)
                .append("comment", comment(def));
    }

    /** Server-side bound for probe cursor creation. */
    public static final long AGGREGATE_MAX_TIME_MS = 5_000;

    public static Document killCursorsCommand(ChangeStreamDefinition def, long cursorId) {
        return new Document("killCursors", def.collection())
                .append("cursors", List.of(cursorId));
    }

    public static String comment(ChangeStreamDefinition def) {
        return "flowwarden:heartbeat:" + def.streamName();
    }

    public static long cursorId(Document reply) {
        return ((Number) reply.get("cursor", Document.class).get("id")).longValue();
    }

    /**
     * Extracts the post-batch resume token of the initial {@code aggregate}
     * reply — the server's position at cursor-open time, available before any
     * event can be returned.
     *
     * @throws IllegalStateException if the reply carries none
     */
    public static BsonDocument initialPbrt(Document reply, ChangeStreamDefinition def) {
        Document pbrt = reply.get("cursor", Document.class)
                .get("postBatchResumeToken", Document.class);
        if (pbrt == null) {
            throw new IllegalStateException(
                    "Initial change stream reply carried no postBatchResumeToken for stream '"
                            + def.streamName() + "'");
        }
        return BsonDocument.parse(pbrt.toJson());
    }
}
