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
package io.flowwarden.stream;

import io.flowwarden.stream.spi.StreamMetricsProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class FlowWardenMetricsTest {

    @AfterEach
    void tearDown() {
        FlowWardenMetrics.reset();
    }

    @Test
    void defaultProviderIsNoOp() {
        assertSame(StreamMetricsProvider.noOp(), FlowWardenMetrics.get());
    }

    @Test
    void setProviderChangesActiveProvider() {
        StreamMetricsProvider custom = mock(StreamMetricsProvider.class);
        FlowWardenMetrics.setProvider(custom);
        assertSame(custom, FlowWardenMetrics.get());
    }

    @Test
    void setProviderRejectsNull() {
        assertThrows(NullPointerException.class, () -> FlowWardenMetrics.setProvider(null));
    }

    @Test
    void resetRestoresToNoOp() {
        FlowWardenMetrics.setProvider(mock(StreamMetricsProvider.class));
        FlowWardenMetrics.reset();
        assertSame(StreamMetricsProvider.noOp(), FlowWardenMetrics.get());
    }

    @Test
    void setAndGetFromDifferentThreads() throws Exception {
        StreamMetricsProvider custom = mock(StreamMetricsProvider.class);

        Thread setter = new Thread(() -> FlowWardenMetrics.setProvider(custom));
        setter.start();
        setter.join();

        assertSame(custom, FlowWardenMetrics.get());
    }
}
