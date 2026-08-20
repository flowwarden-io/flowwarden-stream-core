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

import io.flowwarden.stream.spi.DlqPolicy;
import io.flowwarden.stream.spi.DlqStore;
import io.flowwarden.stream.spi.FailedEvent;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * MongoDB-backed implementation of {@link DlqStore}.
 *
 * <p>Each stream may write to its own collection. The mapping is populated at
 * startup via {@link #registerStream(String, String)} (called by the stream
 * manager once per discovered {@code @ChangeStream} bean), with a default
 * collection name resolved from {@link MongoDlqProperties}. Registering a
 * stream also creates the TTL index on {@code expiresAt} for that
 * collection.</p>
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public class MongoDlqStore implements DlqStore {

    private static final Logger log = LoggerFactory.getLogger(MongoDlqStore.class);

    private final MongoTemplate mongoTemplate;
    private final String defaultCollection;
    private final ConcurrentMap<String, String> streamToCollection = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Boolean> indexesCreated = new ConcurrentHashMap<>();

    public MongoDlqStore(MongoTemplate mongoTemplate, MongoDlqProperties properties) {
        this.mongoTemplate = mongoTemplate;
        this.defaultCollection = properties.getCollection();
    }

    /**
     * Registers a stream → collection binding. Called once per discovered
     * {@code @ChangeStream} at startup. Creates the TTL index on
     * {@code expiresAt} for the resolved collection on first registration.
     *
     * @param streamName  stream identifier
     * @param collection  resolved Mongo collection name (empty/null → default)
     */
    public void registerStream(String streamName, String collection) {
        String resolved = (collection == null || collection.isEmpty())
                ? defaultCollection : collection;
        streamToCollection.put(streamName, resolved);
        ensureIndexes(resolved);
    }

    String collectionFor(String streamName) {
        return streamToCollection.getOrDefault(streamName, defaultCollection);
    }

    private void ensureIndexes(String collection) {
        indexesCreated.computeIfAbsent(collection, c -> {
            try {
                mongoTemplate.indexOps(c)
                        .ensureIndex(new Index().on("expiresAt", org.springframework.data.domain.Sort.Direction.ASC)
                                .expire(0));
                // Covers the backlog count filter (streamName + status) —
                // without it every count would collection-scan.
                mongoTemplate.indexOps(c)
                        .ensureIndex(new Index()
                                .on("streamName", org.springframework.data.domain.Sort.Direction.ASC)
                                .on("status", org.springframework.data.domain.Sort.Direction.ASC));
                log.debug("Ensured DLQ indexes (TTL, streamName+status) on collection '{}'", c);
            } catch (Exception e) {
                log.warn("Failed to create DLQ indexes on '{}': {}", c, e.getMessage());
            }
            return Boolean.TRUE;
        });
    }

    @Override
    public void save(FailedEvent event, DlqPolicy policy) {
        Document doc = toDocument(event);
        mongoTemplate.save(doc, collectionFor(event.streamName()));
    }

    @Override
    public Optional<FailedEvent> findById(String id) {
        for (String collection : distinctCollections()) {
            Document doc = mongoTemplate.findById(id, Document.class, collection);
            if (doc != null) {
                return Optional.of(fromDocument(doc));
            }
        }
        return Optional.empty();
    }

    @Override
    public List<FailedEvent> findByStreamName(String streamName) {
        Query query = Query.query(Criteria.where("streamName").is(streamName));
        return mongoTemplate.find(query, Document.class, collectionFor(streamName))
                .stream()
                .map(MongoDlqStore::fromDocument)
                .toList();
    }

    @Override
    public long count(String streamName) {
        // Always filtered — even a "dedicated" collection is only dedicated
        // by convention (two streams may register the same one), and only
        // PENDING entries are backlog. Rules out estimatedDocumentCount
        // (collection-level, unfilterable) by construction.
        Query query = Query.query(Criteria.where("streamName").is(streamName)
                .and("status").is(FailedEvent.STATUS_PENDING));
        return mongoTemplate.count(query, collectionFor(streamName));
    }

    private java.util.Set<String> distinctCollections() {
        java.util.Set<String> all = new java.util.HashSet<>(streamToCollection.values());
        all.add(defaultCollection);
        return all;
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
