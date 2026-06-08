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

import static org.junit.jupiter.api.Assertions.*;

class NoOpCheckpointStoreTest {

    private final CheckpointStore store = CheckpointStore.noOp();

    @Test
    void noOpReturnsSameInstance() {
        assertSame(CheckpointStore.noOp(), CheckpointStore.noOp());
    }

    @Test
    void saveDoesNotThrow() {
        var cp = new Checkpoint("s", null,
                BsonDocument.parse("{\"_data\": \"token\"}"), Instant.now(),
                null, null, Collections.emptyMap());
        assertDoesNotThrow(() -> store.save(cp));
    }

    @Test
    void findReturnsEmpty() {
        assertTrue(store.findByStreamName("non-existent").isEmpty());
    }

    @Test
    void deleteDoesNotThrow() {
        assertDoesNotThrow(() -> store.delete("non-existent"));
    }
}
