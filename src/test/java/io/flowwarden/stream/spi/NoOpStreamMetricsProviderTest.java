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

import io.flowwarden.stream.OperationType;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class NoOpStreamMetricsProviderTest {

    private final StreamMetricsProvider provider = StreamMetricsProvider.noOp();

    @Test
    void noOpReturnsSameInstance() {
        assertSame(StreamMetricsProvider.noOp(), StreamMetricsProvider.noOp());
    }

    @Test
    void onStreamStartedDoesNotThrow() {
        assertDoesNotThrow(() -> provider.onStreamStarted("test",
                new StreamConfiguration("test", "orders", "testdb", "IMPERATIVE", "", "", null, null, null, java.util.List.of(), "ALL_INSTANCES")));
    }

    @Test
    void onEventReceivedDoesNotThrow() {
        assertDoesNotThrow(() -> provider.onEventReceived("test",
                new ChangeEventMetadata("evt-1", OperationType.INSERT, "orders", null, Instant.now(), Instant.now())));
    }

    @Test
    void onEventProcessedDoesNotThrow() {
        assertDoesNotThrow(() -> provider.onEventProcessed("test", 1_000_000L, true));
    }

    @Test
    void onEventErrorDoesNotThrow() {
        assertDoesNotThrow(() -> provider.onEventError("test",
                new RuntimeException("boom"), false, 1,
                new ChangeEventMetadata("evt-1", OperationType.INSERT, "orders", "doc-1", Instant.now(), Instant.now())));
    }

    @Test
    void onCheckpointDoesNotThrow() {
        assertDoesNotThrow(() -> provider.onCheckpoint("test", "resume-token-123"));
    }

    @Test
    void onBufferStatusDoesNotThrow() {
        assertDoesNotThrow(() -> provider.onBufferStatus("test", 50, 100));
    }

    @Test
    void onBackpressureDoesNotThrow() {
        assertDoesNotThrow(() -> provider.onBackpressure("test", BackpressureAction.DROP));
    }
}
