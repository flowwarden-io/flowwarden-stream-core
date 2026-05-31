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
import org.bson.Document;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;
import java.util.Optional;

/**
 * {@link ReactiveMongoTemplate}-backed implementation of {@link DlqStore}.
 *
 * <p>Uses blocking {@code .block()} calls because the {@link DlqStore} SPI
 * is synchronous. This is acceptable since DLQ saves are lightweight
 * single-document operations.</p>
 *
 * <p>Stores failed events in the {@value MongoDlqStore#COLLECTION} collection
 * (same collection and format as the imperative variant).</p>
 */
public class ReactiveMongoDlqStore implements DlqStore {

    private final ReactiveMongoTemplate reactiveMongoTemplate;

    public ReactiveMongoDlqStore(ReactiveMongoTemplate reactiveMongoTemplate) {
        this.reactiveMongoTemplate = reactiveMongoTemplate;
    }

    @Override
    public void save(FailedEvent event) {
        Document doc = MongoDlqStore.toDocument(event);
        reactiveMongoTemplate.save(doc, MongoDlqStore.COLLECTION).block();
    }

    @Override
    public Optional<FailedEvent> findById(String id) {
        Document doc = reactiveMongoTemplate.findById(id, Document.class,
                MongoDlqStore.COLLECTION).block();
        return Optional.ofNullable(doc).map(MongoDlqStore::fromDocument);
    }

    @Override
    public List<FailedEvent> findByStreamName(String streamName) {
        Query query = Query.query(Criteria.where("streamName").is(streamName));
        List<Document> docs = reactiveMongoTemplate.find(query, Document.class,
                MongoDlqStore.COLLECTION).collectList().block();
        if (docs == null) return List.of();
        return docs.stream()
                .map(MongoDlqStore::fromDocument)
                .toList();
    }
}
