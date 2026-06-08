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
package io.flowwarden.stream.core;

import java.time.Instant;

/**
 * Manages the lifecycle of FlowWarden Change Streams.
 */
public interface FlowWardenStreamManager {

    /**
     * Starts listening on the given stream.
     *
     * @param streamName unique stream identifier
     */
    void startStream(String streamName);

    /**
     * Stops listening on the given stream.
     *
     * @param streamName unique stream identifier
     */
    void stopStream(String streamName);

    /**
     * Returns whether the given stream is currently running.
     *
     * @param streamName unique stream identifier
     */
    boolean isRunning(String streamName);

    /**
     * Returns the time of the last event processed by the given stream,
     * or {@code null} if no event has been processed yet.
     *
     * @param streamName unique stream identifier
     */
    default Instant getLastEventTime(String streamName) {
        return null;
    }
}
