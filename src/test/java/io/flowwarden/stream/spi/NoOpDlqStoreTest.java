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

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class NoOpDlqStoreTest {

    private final DlqStore store = DlqStore.noOp();

    @Test
    void noOpReturnsSameInstance() {
        assertSame(DlqStore.noOp(), DlqStore.noOp());
    }

    @Test
    void saveDoesNotThrow() {
        var event = new FailedEvent(
                "id-1", "stream", "INSERT",
                null, null, null,
                new FailedEvent.ErrorInfo("Ex", "msg", null),
                1, FailedEvent.STATUS_PENDING,
                Instant.now(), Instant.now(), Instant.now(),
                null, Collections.emptyMap()
        );
        DlqPolicy policy = new DlqPolicy(30, true, true);
        assertDoesNotThrow(() -> store.save(event, policy));
    }

    @Test
    void findByIdReturnsEmpty() {
        assertTrue(store.findById("non-existent").isEmpty());
    }

    @Test
    void findByStreamNameReturnsEmptyList() {
        assertTrue(store.findByStreamName("non-existent").isEmpty());
    }
}
