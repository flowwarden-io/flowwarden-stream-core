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
package io.flowwarden.stream.spi;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FailedEventTest {

    @Test
    void recordFieldsAreAccessible() {
        var now = Instant.now();
        var docKey = new BsonDocument("_id", new BsonString("abc"));
        var fullDoc = new Document("status", "NEW");
        var resumeToken = BsonDocument.parse("{\"_data\": \"token-1\"}");
        var error = new FailedEvent.ErrorInfo("RuntimeException", "boom", "stack...");
        var metadata = Map.<String, Object>of("key", "value");

        var event = new FailedEvent(
                "evt-1", "my-stream", "INSERT",
                docKey, fullDoc, resumeToken,
                error, 3, FailedEvent.STATUS_PENDING,
                now, now, now, null, metadata
        );

        assertEquals("evt-1", event.id());
        assertEquals("my-stream", event.streamName());
        assertEquals("INSERT", event.operationType());
        assertEquals(docKey, event.documentKey());
        assertEquals(fullDoc, event.fullDocument());
        assertEquals(resumeToken, event.resumeToken());
        assertEquals(error, event.error());
        assertEquals(3, event.attempts());
        assertEquals("PENDING", event.status());
        assertEquals(now, event.firstAttemptAt());
        assertEquals(now, event.lastAttemptAt());
        assertEquals(now, event.createdAt());
        assertNull(event.expiresAt());
        assertEquals("value", event.metadata().get("key"));
    }

    @Test
    void statusPendingConstant() {
        assertEquals("PENDING", FailedEvent.STATUS_PENDING);
    }

    @Test
    void errorInfoFieldsAreAccessible() {
        var error = new FailedEvent.ErrorInfo("IllegalStateException", "bad state", null);
        assertEquals("IllegalStateException", error.type());
        assertEquals("bad state", error.message());
        assertNull(error.stackTrace());
    }

    @Test
    void nullableFieldsAccepted() {
        var event = new FailedEvent(
                "evt-2", "s", "DELETE",
                null, null, null,
                null, 1, FailedEvent.STATUS_PENDING,
                Instant.now(), Instant.now(), Instant.now(),
                null, Collections.emptyMap()
        );
        assertNull(event.documentKey());
        assertNull(event.fullDocument());
        assertNull(event.resumeToken());
        assertNull(event.error());
        assertNull(event.expiresAt());
    }
}
