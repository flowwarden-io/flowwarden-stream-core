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

import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import io.flowwarden.stream.internal.CursorCommentStamping;
import io.flowwarden.stream.internal.MongoTemplateRegistry;
import io.flowwarden.stream.internal.discovery.StreamRegistry;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.DlqStore;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * The reactive resume-token validation cursor must carry the
 * {@code flowwarden:resume-validation:<stream>} comment — this pins the
 * actual wiring in {@code isTokenValid} (which routes through a stamped
 * template), not just the helper's string. The publisher mock never emits,
 * so the probe resolves through its timeout path — which counts as a valid
 * token, exercised here with the comment already applied.
 */
class ReactiveTokenValidationCommentTest {

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void validationCursor_carriesTheValidationComment() {
        MongoTemplateRegistry templateRegistry = mock(MongoTemplateRegistry.class);
        ReactiveStreamManager manager = new ReactiveStreamManager(
                templateRegistry, mock(StreamRegistry.class),
                mock(CheckpointStore.class), DlqStore.noOp(), null);

        ReactiveMongoDatabaseFactory delegateFactory = mock(ReactiveMongoDatabaseFactory.class);
        MongoDatabase db = mock(MongoDatabase.class);
        MongoCollection<Document> collection = mock(MongoCollection.class);
        ChangeStreamPublisher<Document> publisher =
                mock(ChangeStreamPublisher.class, withSettings().defaultAnswer(RETURNS_SELF));
        when(delegateFactory.getMongoDatabase()).thenReturn(Mono.just(db));
        when(db.getCollection(anyString())).thenReturn((MongoCollection) collection);
        when(collection.watch(eq(Document.class))).thenReturn(publisher);

        ReactiveMongoTemplate template = mock(ReactiveMongoTemplate.class);
        when(template.getMongoDatabaseFactory()).thenReturn(delegateFactory);
        when(template.getConverter()).thenReturn(null);

        boolean valid = manager.isTokenValid("orders",
                BsonDocument.parse("{\"_data\": \"t\"}"), template, "my-stream");

        assertThat(valid).isTrue();
        verify(publisher).comment(CursorCommentStamping.validationCommentFor("my-stream"));
    }
}
