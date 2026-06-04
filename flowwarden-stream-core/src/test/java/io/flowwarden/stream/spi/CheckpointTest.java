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
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CheckpointTest {

    @Test
    void recordFieldsAccessible() {
        var token = BsonDocument.parse("{\"_data\": \"abc123\"}");
        var now = Instant.now();
        var meta = Map.<String, Object>of("key", "value");

        var cp = new Checkpoint("my-stream", "pod-1", token, now, token, now, meta);

        assertEquals("my-stream", cp.streamName());
        assertEquals("pod-1", cp.instanceId());
        assertEquals(token, cp.lastSeenToken());
        assertEquals(now, cp.lastSeenTimestamp());
        assertEquals(token, cp.lastProcessedToken());
        assertEquals(now, cp.lastProcessedTimestamp());
        assertEquals(meta, cp.metadata());
    }

    @Test
    void nullTokensAllowed() {
        var cp = new Checkpoint("s", null, null, null, null, null, Collections.emptyMap());

        assertNull(cp.instanceId());
        assertNull(cp.lastSeenToken());
        assertNull(cp.lastSeenTimestamp());
        assertNull(cp.lastProcessedToken());
        assertNull(cp.lastProcessedTimestamp());
    }

    @Test
    void metadataCanBeEmpty() {
        var cp = new Checkpoint("s", null, null, null, null, null, Collections.emptyMap());

        assertNotNull(cp.metadata());
        assertTrue(cp.metadata().isEmpty());
    }
}
