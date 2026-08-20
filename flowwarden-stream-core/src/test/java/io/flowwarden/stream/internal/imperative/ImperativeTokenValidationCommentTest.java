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
import com.mongodb.client.MongoCollection;
import io.flowwarden.stream.internal.CursorCommentStamping;
import io.flowwarden.stream.internal.MongoTemplateRegistry;
import io.flowwarden.stream.internal.discovery.StreamRegistry;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.DlqStore;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * The resume-token validation cursor must carry the
 * {@code flowwarden:resume-validation:<stream>} comment — this pins the
 * actual wiring in {@code isTokenValid}, not just the helper's string.
 */
class ImperativeTokenValidationCommentTest {

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void validationCursor_carriesTheValidationComment() {
        MongoTemplateRegistry templateRegistry = mock(MongoTemplateRegistry.class);
        ImperativeStreamManager manager = new ImperativeStreamManager(
                templateRegistry, mock(StreamRegistry.class),
                mock(CheckpointStore.class), DlqStore.noOp(), null);

        MongoTemplate template = mock(MongoTemplate.class);
        MongoCollection<Document> collection = mock(MongoCollection.class);
        ChangeStreamIterable<Document> iterable =
                mock(ChangeStreamIterable.class, withSettings().defaultAnswer(RETURNS_SELF));
        MongoChangeStreamCursor<Document> cursor = mock(MongoChangeStreamCursor.class);
        when(template.getCollection("orders")).thenReturn((MongoCollection) collection);
        when(collection.watch()).thenReturn(iterable);
        when(iterable.cursor()).thenReturn((MongoChangeStreamCursor) cursor);
        when(cursor.tryNext()).thenReturn(null);

        boolean valid = manager.isTokenValid("orders",
                BsonDocument.parse("{\"_data\": \"t\"}"), template, "my-stream");

        assertThat(valid).isTrue();
        verify(iterable).comment(CursorCommentStamping.validationCommentFor("my-stream"));
    }
}
