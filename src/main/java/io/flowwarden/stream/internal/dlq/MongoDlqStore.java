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
package io.flowwarden.stream.internal.dlq;

import io.flowwarden.stream.spi.DlqStore;
import io.flowwarden.stream.spi.FailedEvent;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * MongoDB-backed implementation of {@link DlqStore}.
 * Stores failed events in the {@code _fw_dlq} collection.
 */
public class MongoDlqStore implements DlqStore {

    static final String COLLECTION = "_fw_dlq";

    private final MongoTemplate mongoTemplate;

    public MongoDlqStore(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public void save(FailedEvent event) {
        Document doc = toDocument(event);
        mongoTemplate.save(doc, COLLECTION);
    }

    @Override
    public Optional<FailedEvent> findById(String id) {
        Document doc = mongoTemplate.findById(id, Document.class, COLLECTION);
        return Optional.ofNullable(doc).map(MongoDlqStore::fromDocument);
    }

    @Override
    public List<FailedEvent> findByStreamName(String streamName) {
        Query query = Query.query(Criteria.where("streamName").is(streamName));
        return mongoTemplate.find(query, Document.class, COLLECTION)
                .stream()
                .map(MongoDlqStore::fromDocument)
                .toList();
    }

    static Document toDocument(FailedEvent event) {
        Document doc = new Document();
        doc.put("_id", event.id());
        doc.put("streamName", event.streamName());
        doc.put("operationType", event.operationType());
        doc.put("documentKey", bsonValueToDocument(event.documentKey()));
        doc.put("fullDocument", event.fullDocument());
        doc.put("resumeToken", bsonToDocument(event.resumeToken()));
        doc.put("error", errorToDocument(event.error()));
        doc.put("attempts", event.attempts());
        doc.put("status", event.status());
        doc.put("firstAttemptAt", instantToDate(event.firstAttemptAt()));
        doc.put("lastAttemptAt", instantToDate(event.lastAttemptAt()));
        doc.put("createdAt", instantToDate(event.createdAt()));
        doc.put("expiresAt", instantToDate(event.expiresAt()));
        doc.put("metadata", event.metadata() != null ? new Document(event.metadata()) : null);
        return doc;
    }

    static FailedEvent fromDocument(Document doc) {
        return new FailedEvent(
                doc.getString("_id"),
                doc.getString("streamName"),
                doc.getString("operationType"),
                documentToBsonValue(doc.get("documentKey", Document.class)),
                doc.get("fullDocument", Document.class),
                documentToBson(doc.get("resumeToken", Document.class)),
                errorFromDocument(doc.get("error", Document.class)),
                doc.getInteger("attempts", 0),
                doc.getString("status"),
                dateToInstant(doc.get("firstAttemptAt", Date.class)),
                dateToInstant(doc.get("lastAttemptAt", Date.class)),
                dateToInstant(doc.get("createdAt", Date.class)),
                dateToInstant(doc.get("expiresAt", Date.class)),
                toMetadataMap(doc.get("metadata", Document.class))
        );
    }

    private static Document bsonValueToDocument(BsonValue bsonValue) {
        if (bsonValue == null) return null;
        if (bsonValue.isDocument()) {
            return Document.parse(bsonValue.asDocument().toJson());
        }
        // Wrap non-document BsonValues (e.g. ObjectId) in a document
        Document wrapper = new Document();
        wrapper.put("_id", bsonValue.isObjectId()
                ? bsonValue.asObjectId().getValue()
                : bsonValue.toString());
        return wrapper;
    }

    private static BsonValue documentToBsonValue(Document doc) {
        if (doc == null) return null;
        return BsonDocument.parse(doc.toJson());
    }

    private static Document bsonToDocument(BsonDocument bson) {
        if (bson == null) return null;
        return Document.parse(bson.toJson());
    }

    private static BsonDocument documentToBson(Document doc) {
        if (doc == null) return null;
        return BsonDocument.parse(doc.toJson());
    }

    private static Document errorToDocument(FailedEvent.ErrorInfo error) {
        if (error == null) return null;
        Document doc = new Document();
        doc.put("type", error.type());
        doc.put("message", error.message());
        doc.put("stackTrace", error.stackTrace());
        return doc;
    }

    private static FailedEvent.ErrorInfo errorFromDocument(Document doc) {
        if (doc == null) return null;
        return new FailedEvent.ErrorInfo(
                doc.getString("type"),
                doc.getString("message"),
                doc.getString("stackTrace")
        );
    }

    private static Date instantToDate(Instant instant) {
        if (instant == null) return null;
        return Date.from(instant);
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
