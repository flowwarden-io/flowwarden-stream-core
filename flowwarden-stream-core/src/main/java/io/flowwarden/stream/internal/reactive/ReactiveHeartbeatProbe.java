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

import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import com.mongodb.reactivestreams.client.MongoDatabase;
import io.flowwarden.stream.FullDocumentBeforeChangeMode;
import io.flowwarden.stream.FullDocumentMode;
import io.flowwarden.stream.internal.checkpoint.HeartbeatProbe;
import io.flowwarden.stream.internal.checkpoint.ProbeOutcome;
import io.flowwarden.stream.internal.discovery.ChangeStreamDefinition;
import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

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
        MongoDatabase db;
        try {
            db = template.getMongoDatabaseFactory().getMongoDatabase().block(COMMAND_TIMEOUT);
            if (db == null) {
                return ProbeOutcome.failed(new IllegalStateException("MongoDatabase unavailable"));
            }
        } catch (Exception e) {
            return ProbeOutcome.failed(e);
        }

        long cursorId = 0;
        try {
            Document reply = runCommand(db, aggregateCommand(resumeAfter));
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

    private Document aggregateCommand(BsonDocument resumeAfter) {
        Document changeStreamStage = new Document();
        if (resumeAfter != null) {
            changeStreamStage.append("resumeAfter", Document.parse(resumeAfter.toJson()));
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
        stages.addAll(pipeline);
        return new Document("aggregate", def.collection())
                .append("pipeline", stages)
                .append("cursor", new Document("batchSize", 1))
                .append("comment", "flowwarden:heartbeat:" + def.streamName());
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
            Mono.from(db.runCommand(new Document("killCursors", def.collection())
                    .append("cursors", List.of(cursorId)))).block(COMMAND_TIMEOUT);
        } catch (Exception e) {
            log.debug("Failed to kill heartbeat probe cursor for stream '{}': {}",
                    def.streamName(), e.getMessage());
        }
    }
}
