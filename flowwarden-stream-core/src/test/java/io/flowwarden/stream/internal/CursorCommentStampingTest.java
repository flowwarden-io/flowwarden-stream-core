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
package io.flowwarden.stream.internal;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.flowwarden.stream.FlowWarden;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import reactor.core.publisher.Mono;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CursorCommentStampingTest {

    private static final String STREAM = "orders-stream";
    private static final String EXPECTED = FlowWarden.CURSOR_COMMENT_PREFIX + STREAM;

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void collectionWatch_getsTheComment() {
        MongoDatabaseFactory delegate = mock(MongoDatabaseFactory.class);
        MongoDatabase db = mock(MongoDatabase.class);
        MongoCollection<Document> collection = mock(MongoCollection.class);
        ChangeStreamIterable<Document> iterable = mock(ChangeStreamIterable.class);
        ChangeStreamIterable<Document> commented = mock(ChangeStreamIterable.class);
        when(delegate.getMongoDatabase()).thenReturn(db);
        when(db.getCollection(anyString())).thenReturn((MongoCollection) collection);
        when(collection.watch(any(List.class), eq(Document.class))).thenReturn(iterable);
        when(iterable.comment(anyString())).thenReturn(commented);

        MongoDatabaseFactory stamped = CursorCommentStamping.stamp(delegate, CursorCommentStamping.commentFor(STREAM));
        Object result = stamped.getMongoDatabase()
                .getCollection("orders")
                .watch(List.<Bson>of(), Document.class);

        assertThat(result).isSameAs(commented);
        verify(iterable).comment(EXPECTED);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void databaseLevelWatch_getsTheComment_andOtherCallsDelegate() {
        MongoDatabaseFactory delegate = mock(MongoDatabaseFactory.class);
        MongoDatabase db = mock(MongoDatabase.class);
        ChangeStreamIterable<Document> iterable = mock(ChangeStreamIterable.class);
        ChangeStreamIterable<Document> commented = mock(ChangeStreamIterable.class);
        when(delegate.getMongoDatabase(anyString())).thenReturn(db);
        when(db.watch(eq(Document.class))).thenReturn(iterable);
        when(iterable.comment(anyString())).thenReturn(commented);
        when(db.getName()).thenReturn("shop");

        MongoDatabase stampedDb = CursorCommentStamping.stamp(delegate, CursorCommentStamping.commentFor(STREAM))
                .getMongoDatabase("shop");

        assertThat(stampedDb.watch(Document.class)).isSameAs(commented);
        verify(iterable).comment(EXPECTED);
        assertThat(stampedDb.getName()).isEqualTo("shop");
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void reactiveCollectionWatch_getsTheComment() {
        ReactiveMongoDatabaseFactory delegate = mock(ReactiveMongoDatabaseFactory.class);
        com.mongodb.reactivestreams.client.MongoDatabase db =
                mock(com.mongodb.reactivestreams.client.MongoDatabase.class);
        com.mongodb.reactivestreams.client.MongoCollection<Document> collection =
                mock(com.mongodb.reactivestreams.client.MongoCollection.class);
        com.mongodb.reactivestreams.client.ChangeStreamPublisher<Document> publisher =
                mock(com.mongodb.reactivestreams.client.ChangeStreamPublisher.class);
        com.mongodb.reactivestreams.client.ChangeStreamPublisher<Document> commented =
                mock(com.mongodb.reactivestreams.client.ChangeStreamPublisher.class);
        when(delegate.getMongoDatabase()).thenReturn(Mono.just(db));
        when(db.getCollection(anyString())).thenReturn(
                (com.mongodb.reactivestreams.client.MongoCollection) collection);
        when(collection.watch(any(List.class), eq(Document.class))).thenReturn(publisher);
        when(publisher.comment(anyString())).thenReturn(commented);

        Object result = CursorCommentStamping.stamp(delegate, CursorCommentStamping.commentFor(STREAM))
                .getMongoDatabase().block()
                .getCollection("orders")
                .watch(List.<Bson>of(), Document.class);

        assertThat(result).isSameAs(commented);
        verify(publisher).comment(EXPECTED);
    }

    @Test
    void commentFamily_producesTheDocumentedValues() {
        // The single source of truth for every cursor comment FlowWarden
        // stamps — the values documented on FlowWarden.CURSOR_COMMENT_PREFIX.
        assertThat(CursorCommentStamping.commentFor("orders"))
                .isEqualTo("flowwarden:orders");
        assertThat(CursorCommentStamping.heartbeatCommentFor("orders"))
                .isEqualTo("flowwarden:heartbeat:orders");
        assertThat(CursorCommentStamping.validationCommentFor("orders"))
                .isEqualTo("flowwarden:resume-validation:orders");
    }

    @Test
    void delegateExceptions_surfaceUnwrapped() {
        MongoDatabaseFactory delegate = mock(MongoDatabaseFactory.class);
        MongoDatabase db = mock(MongoDatabase.class);
        when(delegate.getMongoDatabase()).thenReturn(db);
        when(db.getName()).thenThrow(new IllegalStateException("boom"));

        MongoDatabase stampedDb = CursorCommentStamping.stamp(delegate, CursorCommentStamping.commentFor(STREAM)).getMongoDatabase();

        // The proxy must rethrow the original exception, not an
        // UndeclaredThrowableException/InvocationTargetException wrapper.
        assertThatThrownBy(stampedDb::getName)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("boom");
    }
}
