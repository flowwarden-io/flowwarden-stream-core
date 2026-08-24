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
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * {@link ReactiveMongoTemplate}-backed implementation of {@link DlqStore}.
 *
 * <p>Uses blocking {@code .block()} calls because the {@link DlqStore} SPI
 * is synchronous. This is acceptable since DLQ saves are lightweight
 * single-document operations.</p>
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public class ReactiveMongoDlqStore implements DlqStore {

    private static final Logger log = LoggerFactory.getLogger(ReactiveMongoDlqStore.class);

    private final ReactiveMongoTemplate reactiveMongoTemplate;
    private final String defaultCollection;
    private final ConcurrentMap<String, String> streamToCollection = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Boolean> indexesCreated = new ConcurrentHashMap<>();

    public ReactiveMongoDlqStore(ReactiveMongoTemplate reactiveMongoTemplate,
                                 MongoDlqProperties properties) {
        this.reactiveMongoTemplate = reactiveMongoTemplate;
        this.defaultCollection = properties.getCollection();
    }

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
                reactiveMongoTemplate.indexOps(c)
                        .ensureIndex(new Index().on("expiresAt", Sort.Direction.ASC).expire(0))
                        .block();
                // Covers the backlog count filter (streamName + status) —
                // without it every count would collection-scan.
                reactiveMongoTemplate.indexOps(c)
                        .ensureIndex(new Index()
                                .on("streamName", Sort.Direction.ASC)
                                .on("status", Sort.Direction.ASC))
                        .block();
                log.debug("Ensured DLQ indexes (TTL, streamName+status) on collection '{}'", c);
            } catch (Exception e) {
                log.warn("Failed to create DLQ indexes on '{}': {}", c, e.getMessage());
            }
            return Boolean.TRUE;
        });
    }

    @Override
    public void save(FailedEvent event, DlqPolicy policy) {
        Document doc = MongoDlqStore.toDocument(event);
        reactiveMongoTemplate.save(doc, collectionFor(event.streamName())).block();
    }

    @Override
    public Optional<FailedEvent> findById(String id) {
        for (String collection : distinctCollections()) {
            Document doc = reactiveMongoTemplate.findById(id, Document.class, collection).block();
            if (doc != null) {
                return Optional.of(MongoDlqStore.fromDocument(doc));
            }
        }
        return Optional.empty();
    }

    @Override
    public List<FailedEvent> findByStreamName(String streamName) {
        Query query = Query.query(Criteria.where("streamName").is(streamName));
        List<Document> docs = reactiveMongoTemplate.find(query, Document.class,
                collectionFor(streamName)).collectList().block();
        if (docs == null) return List.of();
        return docs.stream()
                .map(MongoDlqStore::fromDocument)
                .toList();
    }

    @Override
    public long count(String streamName) {
        // Same contract as the sync store: always filtered (shared default
        // collection; "dedicated" is only a convention), PENDING only.
        Query query = Query.query(Criteria.where("streamName").is(streamName)
                .and("status").is(FailedEvent.STATUS_PENDING));
        Long count = reactiveMongoTemplate.count(query, collectionFor(streamName)).block();
        return count != null ? count : -1;
    }

    private Set<String> distinctCollections() {
        Set<String> all = new HashSet<>(streamToCollection.values());
        all.add(defaultCollection);
        return all;
    }
}
