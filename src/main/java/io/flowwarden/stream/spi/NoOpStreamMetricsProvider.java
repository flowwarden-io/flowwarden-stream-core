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

/**
 * Default no-op implementation of {@link StreamMetricsProvider}.
 * All methods silently do nothing.
 */
final class NoOpStreamMetricsProvider implements StreamMetricsProvider {

    static final NoOpStreamMetricsProvider INSTANCE = new NoOpStreamMetricsProvider();

    private NoOpStreamMetricsProvider() {
    }

    @Override
    public void onStreamStarted(String streamName, StreamConfiguration config) {
    }

    @Override
    public void onEventReceived(String streamName, ChangeEventMetadata metadata) {
    }

    @Override
    public void onEventProcessed(String streamName, long durationNanos, boolean success) {
    }

    @Override
    public void onEventError(String streamName, Throwable error, boolean willRetry,
                             int attemptNumber, ChangeEventMetadata metadata) {
    }

    @Override
    public void onCheckpoint(String streamName, String resumeToken) {
    }

    @Override
    public void onBufferStatus(String streamName, int currentSize, int maxSize) {
    }

    @Override
    public void onBackpressure(String streamName, BackpressureAction action) {
    }

    @Override
    public void onEventSentToDlq(String streamName) {
    }

    @Override
    public void onOplogStats(double logLengthHours, String status) {
    }
}
