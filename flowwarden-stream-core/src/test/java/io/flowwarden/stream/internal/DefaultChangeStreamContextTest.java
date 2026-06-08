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

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.flowwarden.stream.OperationType;
import io.flowwarden.stream.TransactionInfo;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DefaultChangeStreamContextTest {

    private ChangeStreamDocument<Document> raw;
    private DefaultChangeStreamContext<Document> ctx;
    private DefaultChangeStreamContext.ContextActions actions;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        raw = mock(ChangeStreamDocument.class);
        actions = mock(DefaultChangeStreamContext.ContextActions.class);

        when(raw.getOperationType())
                .thenReturn(com.mongodb.client.model.changestream.OperationType.INSERT);
        when(raw.getNamespace())
                .thenReturn(new MongoNamespace("testdb", "orders"));
        when(raw.getClusterTime())
                .thenReturn(new BsonTimestamp(1700000000, 1));
        when(raw.getResumeToken())
                .thenReturn(new BsonDocument("_data", new BsonString("token123")));
        when(raw.getDocumentKey())
                .thenReturn(new BsonDocument("_id", new BsonString("abc")));

        ctx = new DefaultChangeStreamContext<>(raw, "order-stream", actions, null);
    }

    @Test
    void eventIdIsGenerated() {
        assertNotNull(ctx.getEventId());
        assertFalse(ctx.getEventId().isEmpty());
    }

    @Test
    void eventIdIsStable() {
        assertEquals(ctx.getEventId(), ctx.getEventId());
    }

    @Test
    void mapsOperationType() {
        assertEquals(OperationType.INSERT, ctx.getOperationType());
    }

    @Test
    void mapsAllOperationTypes() {
        var mappings = Map.of(
                com.mongodb.client.model.changestream.OperationType.INSERT, OperationType.INSERT,
                com.mongodb.client.model.changestream.OperationType.UPDATE, OperationType.UPDATE,
                com.mongodb.client.model.changestream.OperationType.REPLACE, OperationType.REPLACE,
                com.mongodb.client.model.changestream.OperationType.DELETE, OperationType.DELETE,
                com.mongodb.client.model.changestream.OperationType.DROP, OperationType.DROP,
                com.mongodb.client.model.changestream.OperationType.INVALIDATE, OperationType.INVALIDATE
        );

        for (var entry : mappings.entrySet()) {
            when(raw.getOperationType()).thenReturn(entry.getKey());
            var freshCtx = new DefaultChangeStreamContext<>(raw, "test", DefaultChangeStreamContext.NOOP_ACTIONS, null);
            assertEquals(entry.getValue(), freshCtx.getOperationType(),
                    "Mapping failed for " + entry.getKey());
        }
    }

    @Test
    void returnsStreamName() {
        assertEquals("order-stream", ctx.getStreamName());
    }

    @Test
    void returnsCollectionName() {
        assertEquals("orders", ctx.getCollectionName());
    }

    @Test
    void returnsDatabaseName() {
        assertEquals("testdb", ctx.getDatabaseName());
    }

    @Test
    void mapsClusterTime() {
        Instant expected = Instant.ofEpochSecond(1700000000);
        assertEquals(expected, ctx.getClusterTime());
    }

    @Test
    void clusterTimeNullWhenAbsent() {
        when(raw.getClusterTime()).thenReturn(null);
        assertNull(ctx.getClusterTime());
    }

    @Test
    void returnsResumeToken() {
        BsonDocument token = ctx.getResumeToken();
        assertNotNull(token);
        assertEquals("token123", token.getString("_data").getValue());
    }

    @Test
    void attemptNumberIsAlwaysOne() {
        assertEquals(1, ctx.getAttemptNumber());
    }

    @Test
    void returnsDocumentKey() {
        assertNotNull(ctx.getDocumentKey());
    }

    // ---- Transaction info ----

    @Test
    void transactionInfoPresentWhenBothLsidAndTxnNumberAvailable() {
        BsonDocument lsid = new BsonDocument("id", new BsonString("session-1"));
        when(raw.getLsid()).thenReturn(lsid);
        when(raw.getTxnNumber()).thenReturn(new BsonInt64(42L));

        Optional<TransactionInfo> info = ctx.getTransactionInfo();

        assertTrue(info.isPresent());
        assertEquals(lsid, info.get().lsid());
        assertEquals(42L, info.get().txnNumber());
    }

    @Test
    void transactionInfoEmptyWhenBothNull() {
        when(raw.getLsid()).thenReturn(null);
        when(raw.getTxnNumber()).thenReturn(null);

        assertTrue(ctx.getTransactionInfo().isEmpty());
    }

    @Test
    void transactionInfoEmptyWhenLsidNullOnly() {
        when(raw.getLsid()).thenReturn(null);
        when(raw.getTxnNumber()).thenReturn(new BsonInt64(42L));

        assertTrue(ctx.getTransactionInfo().isEmpty());
    }

    @Test
    void transactionInfoEmptyWhenTxnNumberNullOnly() {
        when(raw.getLsid()).thenReturn(new BsonDocument("id", new BsonString("session-1")));
        when(raw.getTxnNumber()).thenReturn(null);

        assertTrue(ctx.getTransactionInfo().isEmpty());
    }

    // ---- Full document ----

    @Test
    void fullDocumentPresentWhenAvailable() {
        Document doc = new Document("status", "PAID");
        when(raw.getFullDocument()).thenReturn(doc);

        var result = ctx.getFullDocument(Document.class);
        assertTrue(result.isPresent());
        assertEquals("PAID", result.get().getString("status"));
    }

    @Test
    void fullDocumentEmptyWhenNull() {
        when(raw.getFullDocument()).thenReturn(null);
        assertTrue(ctx.getFullDocument(Document.class).isEmpty());
    }

    @Test
    void fullDocumentBeforeChangePresentWhenAvailable() {
        Document doc = new Document("status", "PENDING");
        when(raw.getFullDocumentBeforeChange()).thenReturn(doc);

        var result = ctx.getFullDocumentBeforeChange(Document.class);
        assertTrue(result.isPresent());
        assertEquals("PENDING", result.get().getString("status"));
    }

    @Test
    void fullDocumentBeforeChangeEmptyWhenNull() {
        when(raw.getFullDocumentBeforeChange()).thenReturn(null);
        assertTrue(ctx.getFullDocumentBeforeChange(Document.class).isEmpty());
    }

    // ---- Update description ----

    @Test
    void updateDescriptionPresentWhenAvailable() {
        var driverDesc = mock(com.mongodb.client.model.changestream.UpdateDescription.class);
        when(driverDesc.getUpdatedFields()).thenReturn(new BsonDocument("status", new BsonString("PAID")));
        when(driverDesc.getRemovedFields()).thenReturn(null);
        when(driverDesc.getTruncatedArrays()).thenReturn(null);
        when(raw.getUpdateDescription()).thenReturn(driverDesc);

        var result = ctx.getUpdateDescription();
        assertTrue(result.isPresent());
        assertEquals("PAID", result.get().getUpdatedFields().get("status"));
    }

    @Test
    void updateDescriptionEmptyWhenNull() {
        when(raw.getUpdateDescription()).thenReturn(null);
        assertTrue(ctx.getUpdateDescription().isEmpty());
    }

    // ---- Actions ----

    @Test
    void sendToDlqDelegatesToActions() {
        ctx.sendToDlq("test reason");
        verify(actions).sendToDlq("test reason");
    }

    @Test
    void saveCheckpointNowDelegatesToActions() {
        ctx.saveCheckpointNow();
        verify(actions).saveCheckpointNow();
    }

    @Test
    void noopActionsDoNotThrow() {
        var noopCtx = new DefaultChangeStreamContext<>(raw, "test", DefaultChangeStreamContext.NOOP_ACTIONS, null);
        assertDoesNotThrow(() -> noopCtx.sendToDlq("reason"));
        assertDoesNotThrow(noopCtx::saveCheckpointNow);
    }

    // ---- Metadata ----

    @Test
    void metadataRoundTrip() {
        ctx.addMetadata("key1", "value1");
        var result = ctx.getMetadata("key1", String.class);
        assertTrue(result.isPresent());
        assertEquals("value1", result.get());
    }

    @Test
    void metadataEmptyForMissingKey() {
        assertTrue(ctx.getMetadata("missing", String.class).isEmpty());
    }

    @Test
    void getAllMetadataReturnsAllEntries() {
        ctx.addMetadata("a", 1);
        ctx.addMetadata("b", 2);

        Map<String, Object> all = ctx.getAllMetadata();
        assertEquals(2, all.size());
        assertEquals(1, all.get("a"));
        assertEquals(2, all.get("b"));
    }

    @Test
    void getAllMetadataIsUnmodifiable() {
        ctx.addMetadata("key", "val");
        assertThrows(UnsupportedOperationException.class,
                () -> ctx.getAllMetadata().put("new", "val"));
    }
}
