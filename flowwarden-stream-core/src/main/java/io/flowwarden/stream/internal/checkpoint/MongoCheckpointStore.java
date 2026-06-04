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

import io.flowwarden.stream.spi.Checkpoint;
import io.flowwarden.stream.spi.CheckpointStore;
import org.bson.BsonDocument;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Optional;

/**
 * MongoDB-backed implementation of {@link CheckpointStore}.
 * Stores checkpoints in the {@code _fw_checkpoints} collection.
 */
public class MongoCheckpointStore implements CheckpointStore {

    static final String COLLECTION = "_fw_checkpoints";

    private final MongoTemplate mongoTemplate;

    public MongoCheckpointStore(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public void save(Checkpoint checkpoint) {
        Document doc = toDocument(checkpoint);
        mongoTemplate.save(doc, COLLECTION);
    }

    @Override
    public Optional<Checkpoint> findByStreamName(String streamName) {
        Document doc = mongoTemplate.findById(streamName, Document.class, COLLECTION);
        return Optional.ofNullable(doc).map(MongoCheckpointStore::fromDocument);
    }

    @Override
    public void saveSeen(String streamName, BsonDocument token, Instant timestamp) {
        Update update = new Update()
                .set("lastSeenToken", bsonToDocument(token))
                .set("lastSeenTimestamp", timestamp);
        mongoTemplate.upsert(
                Query.query(Criteria.where("_id").is(streamName)),
                update,
                COLLECTION
        );
    }

    @Override
    public void saveProcessed(String streamName, BsonDocument token, Instant timestamp) {
        Update update = new Update()
                .set("lastProcessedToken", bsonToDocument(token))
                .set("lastProcessedTimestamp", timestamp);
        mongoTemplate.upsert(
                Query.query(Criteria.where("_id").is(streamName)),
                update,
                COLLECTION
        );
    }

    @Override
    public void delete(String streamName) {
        mongoTemplate.remove(
                Query.query(Criteria.where("_id").is(streamName)),
                COLLECTION
        );
    }

    private static Document toDocument(Checkpoint cp) {
        Document doc = new Document();
        doc.put("_id", cp.streamName());
        doc.put("instanceId", cp.instanceId());
        doc.put("lastSeenToken", bsonToDocument(cp.lastSeenToken()));
        doc.put("lastSeenTimestamp", cp.lastSeenTimestamp());
        doc.put("lastProcessedToken", bsonToDocument(cp.lastProcessedToken()));
        doc.put("lastProcessedTimestamp", cp.lastProcessedTimestamp());
        doc.put("metadata", cp.metadata() != null ? new Document(cp.metadata()) : null);
        return doc;
    }

    private static Checkpoint fromDocument(Document doc) {
        return new Checkpoint(
                doc.getString("_id"),
                doc.getString("instanceId"),
                documentToBson(doc.get("lastSeenToken", Document.class)),
                dateToInstant(doc.get("lastSeenTimestamp", Date.class)),
                documentToBson(doc.get("lastProcessedToken", Document.class)),
                dateToInstant(doc.get("lastProcessedTimestamp", Date.class)),
                toMetadataMap(doc.get("metadata", Document.class))
        );
    }

    private static Document bsonToDocument(BsonDocument bson) {
        if (bson == null) return null;
        return Document.parse(bson.toJson());
    }

    private static BsonDocument documentToBson(Document doc) {
        if (doc == null) return null;
        return BsonDocument.parse(doc.toJson());
    }

    private static Instant dateToInstant(Date date) {
        if (date == null) return null;
        return date.toInstant();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> toMetadataMap(Document doc) {
        if (doc == null) return Collections.emptyMap();
        return (Map<String, Object>) (Map<?, ?>) doc;
    }
}
