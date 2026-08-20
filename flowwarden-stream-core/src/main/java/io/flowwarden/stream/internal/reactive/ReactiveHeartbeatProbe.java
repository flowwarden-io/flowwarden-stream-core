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
package io.flowwarden.stream.internal.reactive;

import com.mongodb.reactivestreams.client.MongoDatabase;
import io.flowwarden.stream.internal.checkpoint.ChangeStreamProbeCommands;
import io.flowwarden.stream.internal.checkpoint.HeartbeatProbe;
import io.flowwarden.stream.internal.checkpoint.ProbeOutcome;
import io.flowwarden.stream.internal.discovery.ChangeStreamDefinition;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Raw-command {@link HeartbeatProbe} for REACTIVE mode.
 *
 * <p>{@code ChangeStreamPublisher} exposes neither the cursor nor its
 * post-batch resume token, so the probe drives the change stream via
 * {@code runCommand}: an {@code aggregate} with a {@code $changeStream} stage
 * (carrying {@code resumeAfter} and the replicated {@code fullDocument}
 * options) followed by the user's pipeline stages verbatim, then — when the
 * first batch is empty — one bounded {@code getMore} whose reply carries the
 * PBRT in {@code cursor.postBatchResumeToken}. The server-side cursor is
 * always killed.</p>
 *
 * <p>Blocking with a timeout is acceptable here: the probe runs on
 * stream-core's own daemon scheduler thread, never on a Reactor thread.</p>
 *
 * <p>This class is internal and not part of the public API.</p>
 */
final class ReactiveHeartbeatProbe implements HeartbeatProbe {

    private static final Logger log = LoggerFactory.getLogger(ReactiveHeartbeatProbe.class);

    static final long MAX_AWAIT_MILLIS = 1_000;
    private static final Duration COMMAND_TIMEOUT = Duration.ofSeconds(5);

    private final ReactiveMongoTemplate template;
    private final ChangeStreamDefinition def;
    private final List<Document> pipeline;

    ReactiveHeartbeatProbe(ReactiveMongoTemplate template,
                           ChangeStreamDefinition def,
                           List<Document> pipeline) {
        this.template = template;
        this.def = def;
        this.pipeline = pipeline == null ? List.of() : pipeline;
    }

    @Override
    public ProbeOutcome probe(BsonDocument resumeAfter) {
        Objects.requireNonNull(resumeAfter, "resumeAfter must not be null — use initialPosition()");
        return boundedRead(
                ChangeStreamProbeCommands.aggregateCommand(def, pipeline, resumeAfter));
    }

    @Override
    public ProbeOutcome probeFromOperationTime(BsonTimestamp operationTime) {
        Objects.requireNonNull(operationTime, "operationTime must not be null");
        return boundedRead(
                ChangeStreamProbeCommands.aggregateCommand(def, pipeline, null, operationTime));
    }

    private ProbeOutcome boundedRead(Document aggregateCommand) {
        MongoDatabase db;
        try {
            db = database();
        } catch (Exception e) {
            return ProbeOutcome.failed(e);
        }

        long cursorId = 0;
        try {
            Document reply = runCommand(db, aggregateCommand);
            Document cursor = reply.get("cursor", Document.class);
            cursorId = ((Number) cursor.get("id")).longValue();
            List<Document> firstBatch = cursor.getList("firstBatch", Document.class);
            if (!firstBatch.isEmpty()) {
                return ProbeOutcome.eventPending();
            }

            // The initial reply's PBRT is typically the resume point itself —
            // the actual oplog read happens on getMore.
            Document getMoreReply = runCommand(db, new Document("getMore", cursorId)
                    .append("collection", def.collection())
                    .append("batchSize", 1)
                    .append("maxTimeMS", MAX_AWAIT_MILLIS));
            Document getMoreCursor = getMoreReply.get("cursor", Document.class);
            cursorId = ((Number) getMoreCursor.get("id")).longValue();
            List<Document> nextBatch = getMoreCursor.getList("nextBatch", Document.class);
            if (!nextBatch.isEmpty()) {
                return ProbeOutcome.eventPending();
            }
            Document pbrtDoc = getMoreCursor.get("postBatchResumeToken", Document.class);
            if (pbrtDoc == null) {
                return ProbeOutcome.failed(new IllegalStateException(
                        "getMore reply carried no postBatchResumeToken"));
            }
            return ProbeOutcome.empty(BsonDocument.parse(pbrtDoc.toJson()));
        } catch (Exception e) {
            return ProbeOutcome.failed(e);
        } finally {
            killCursor(db, cursorId);
        }
    }

    @Override
    public BsonDocument initialPosition() {
        // No getMore: the initial aggregate reply already carries a PBRT at
        // cursor-open time, before any event can be returned — so there is no
        // probe result to discard and no unprotected hand-off window.
        MongoDatabase db = database();
        long cursorId = 0;
        try {
            Document reply = runCommand(db,
                    ChangeStreamProbeCommands.aggregateCommand(def, pipeline, null));
            cursorId = ChangeStreamProbeCommands.cursorId(reply);
            return ChangeStreamProbeCommands.initialPbrt(reply, def);
        } finally {
            killCursor(db, cursorId);
        }
    }

    private MongoDatabase database() {
        MongoDatabase db = template.getMongoDatabaseFactory().getMongoDatabase()
                .block(COMMAND_TIMEOUT);
        if (db == null) {
            throw new IllegalStateException("MongoDatabase unavailable");
        }
        return db;
    }

    private static Document runCommand(MongoDatabase db, Document command) {
        Document reply = Mono.from(db.runCommand(command)).block(COMMAND_TIMEOUT);
        if (reply == null) {
            throw new IllegalStateException("No reply for command: " + command.keySet());
        }
        return reply;
    }

    private void killCursor(MongoDatabase db, long cursorId) {
        if (cursorId == 0) {
            return;
        }
        try {
            Mono.from(db.runCommand(
                    ChangeStreamProbeCommands.killCursorsCommand(def, cursorId)))
                    .block(COMMAND_TIMEOUT);
        } catch (Exception e) {
            log.debug("Failed to kill heartbeat probe cursor for stream '{}': {}",
                    def.streamName(), e.getMessage());
        }
    }
}
