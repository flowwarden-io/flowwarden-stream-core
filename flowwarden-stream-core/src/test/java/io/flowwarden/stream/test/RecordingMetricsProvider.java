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
package io.flowwarden.stream.test;

import io.flowwarden.stream.spi.BackpressureAction;
import io.flowwarden.stream.spi.ChangeEventMetadata;
import io.flowwarden.stream.spi.StopReason;
import io.flowwarden.stream.spi.StreamConfiguration;
import io.flowwarden.stream.spi.StreamMetricsProvider;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Hand-written metrics provider for integration tests that install a provider
 * globally while streams are running.
 *
 * <p>A Mockito mock in that position is a race: the heartbeat probe, the
 * stats executor and the flush scheduler keep calling the provider from
 * their own threads, and any call that lands between a {@code when(mock)}
 * and the stubbed invocation fails the test with {@code UnfinishedStubbing}
 * (seen on CI with {@code ReactiveStreamCrashIntegrationTest}). This class
 * has no stubbing window — fault injection is a volatile field, assertions
 * read concurrent-safe lists.</p>
 */
public final class RecordingMetricsProvider implements StreamMetricsProvider {

    /** One {@code onStreamStopped} signal; {@code cause} compared by identity (exceptions do not override equals). */
    public record Stop(String streamName, StopReason reason, Throwable cause) {
    }

    private final List<Stop> stops = new CopyOnWriteArrayList<>();
    private final Map<String, RuntimeException> failOnEventReceived = new ConcurrentHashMap<>();

    /** Every subsequent {@code onEventReceived} for {@code streamName} throws {@code cause}. */
    public void failOnEventReceived(String streamName, RuntimeException cause) {
        failOnEventReceived.put(streamName, cause);
    }

    public List<Stop> stops() {
        return List.copyOf(stops);
    }

    @Override
    public void onStreamStopped(String streamName, StopReason reason, Throwable cause) {
        stops.add(new Stop(streamName, reason, cause));
    }

    @Override
    public void onEventReceived(String streamName, ChangeEventMetadata metadata) {
        RuntimeException cause = failOnEventReceived.get(streamName);
        if (cause != null) {
            throw cause;
        }
    }

    @Override public void onStreamStarted(String streamName, StreamConfiguration config) {}
    @Override public void onEventProcessed(String streamName, long durationNanos, boolean success) {}
    @Override public void onEventError(String streamName, Throwable error, boolean willRetry,
                                       int attemptNumber, ChangeEventMetadata metadata) {}
    @Override public void onCheckpoint(String streamName, String resumeToken) {}
    @Override public void onBufferStatus(String streamName, int currentSize, int maxSize) {}
    @Override public void onBackpressure(String streamName, BackpressureAction action) {}
    @Override public void onEventSentToDlq(String streamName) {}
    @Override public void onOplogStats(double logLengthHours, String status) {}
}
