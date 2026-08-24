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

import io.flowwarden.stream.spi.StreamMetricsProvider;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** Records {@code onDlqBacklog} emissions; every other callback is a no-op. */
public final class RecordingBacklogMetrics implements StreamMetricsProvider {

    public record Backlog(String streamName, long pending) {
    }

    public final List<Backlog> backlogs = new CopyOnWriteArrayList<>();

    @Override
    public void onDlqBacklog(String streamName, long pendingCount) {
        backlogs.add(new Backlog(streamName, pendingCount));
    }

    @Override
    public void onStreamStarted(String streamName,
            io.flowwarden.stream.spi.StreamConfiguration config) {
    }

    @Override
    public void onEventReceived(String streamName,
            io.flowwarden.stream.spi.ChangeEventMetadata metadata) {
    }

    @Override
    public void onEventProcessed(String streamName, long durationNanos, boolean success) {
    }

    @Override
    public void onEventError(String streamName, Throwable error, boolean willRetry,
            int attemptNumber, io.flowwarden.stream.spi.ChangeEventMetadata metadata) {
    }

    @Override
    public void onCheckpoint(String streamName, String resumeToken) {
    }

    @Override
    public void onBufferStatus(String streamName, int currentSize, int maxSize) {
    }

    @Override
    public void onBackpressure(String streamName,
            io.flowwarden.stream.spi.BackpressureAction action) {
    }

    @Override
    public void onEventSentToDlq(String streamName) {
    }

    @Override
    public void onOplogStats(double logLengthHours, String status) {
    }
}
