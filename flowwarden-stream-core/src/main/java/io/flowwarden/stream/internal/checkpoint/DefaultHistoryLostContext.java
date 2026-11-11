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
package io.flowwarden.stream.internal.checkpoint;

import io.flowwarden.stream.HistoryLostContext;
import io.flowwarden.stream.OnHistoryLost;
import io.flowwarden.stream.spi.Checkpoint;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * The lost-window bound handed to a {@code @Reconcile} hook. Built by the
 * resume cascade from the dead checkpoint read at detection and the position
 * the stream is about to resume from.
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public record DefaultHistoryLostContext(String streamName,
                                        OnHistoryLost strategy,
                                        Instant lastProcessed,
                                        Instant lastSeen,
                                        Instant lastHeartbeat,
                                        Instant windowStart,
                                        Instant lostWindowEnd,
                                        int attempt) implements HistoryLostContext {

    public DefaultHistoryLostContext {
        Objects.requireNonNull(streamName, "streamName must not be null");
        Objects.requireNonNull(strategy, "strategy must not be null");
        Objects.requireNonNull(lostWindowEnd, "lostWindowEnd must not be null");
    }

    /**
     * First invocation: the window starts at the most recent activity the
     * dead checkpoint recorded.
     */
    public static DefaultHistoryLostContext first(String streamName, OnHistoryLost strategy,
                                                  Checkpoint dead, Instant lostWindowEnd) {
        Instant start = mostRecent(dead.lastProcessedTimestamp(), dead.lastSeenTimestamp(),
                dead.lastHeartbeatTimestamp());
        return new DefaultHistoryLostContext(streamName, strategy, dead.lastProcessedTimestamp(),
                dead.lastSeenTimestamp(), dead.lastHeartbeatTimestamp(), start, lostWindowEnd, 1);
    }

    /**
     * Second, bounded invocation: the window is the residual one, from the
     * end of the first invocation's window to the fresh position.
     */
    public DefaultHistoryLostContext residual(Instant newEnd) {
        return new DefaultHistoryLostContext(streamName, strategy, lastProcessed, lastSeen,
                lastHeartbeat, lostWindowEnd, newEnd, 2);
    }

    @Override
    public Optional<Instant> lastProcessedTimestamp() {
        return Optional.ofNullable(lastProcessed);
    }

    @Override
    public Optional<Instant> lastSeenTimestamp() {
        return Optional.ofNullable(lastSeen);
    }

    @Override
    public Optional<Instant> lastHeartbeatTimestamp() {
        return Optional.ofNullable(lastHeartbeat);
    }

    @Override
    public Optional<Instant> lostWindowStart() {
        return Optional.ofNullable(windowStart);
    }

    static Instant mostRecent(Instant... candidates) {
        Instant best = null;
        for (Instant candidate : candidates) {
            if (candidate != null && (best == null || candidate.isAfter(best))) {
                best = candidate;
            }
        }
        return best;
    }

    @Override
    public String toString() {
        return "HistoryLostContext{stream='" + streamName + "', strategy=" + strategy
                + ", lostWindow=[" + windowStart + " -> " + lostWindowEnd + "], attempt=" + attempt + "}";
    }
}
