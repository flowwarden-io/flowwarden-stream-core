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

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * SPI for persisting Change Stream checkpoints (resume tokens).
 *
 * <p>The Core ships with a {@linkplain #noOp() no-op} implementation and a
 * MongoDB-backed implementation registered via auto-configuration.
 * Users may provide their own implementation as a Spring bean.</p>
 */
public interface CheckpointStore {

    /**
     * Persists (upserts) a checkpoint keyed by {@code checkpoint.streamName()}.
     *
     * @param checkpoint the checkpoint to save
     */
    void save(Checkpoint checkpoint);

    /**
     * Retrieves the checkpoint for the given stream.
     *
     * @param streamName stream identifier
     * @return the checkpoint, or empty if none exists
     */
    Optional<Checkpoint> findByStreamName(String streamName);

    /**
     * Updates only the {@code lastSeenToken} pair for the given stream, leaving
     * {@code lastProcessedToken} and {@code lastProcessedTimestamp} untouched.
     *
     * <p>Intended for hot-path writes that advance the "last event received"
     * cursor independently of handler success, for example the
     * {@code saveIntervalSeconds} timer or any path that tracks events
     * filtered out before the handler runs.</p>
     *
     * <p>Implementations are encouraged to use a targeted update (e.g. a
     * partial {@code $set}) to avoid a round-trip read. The default
     * implementation falls back to {@link #findByStreamName(String)} followed
     * by {@link #save(Checkpoint)} so external implementations continue to
     * work unchanged.</p>
     *
     * @param streamName the stream identifier (must not be null)
     * @param token      the new {@code lastSeenToken} value (must not be null)
     * @param timestamp  the new {@code lastSeenTimestamp} value (must not be null)
     */
    default void saveSeen(String streamName, BsonDocument token, Instant timestamp) {
        Checkpoint current = findByStreamName(streamName).orElseGet(() ->
                new Checkpoint(streamName, null, null, null, null, null, Map.of()));
        save(new Checkpoint(
                current.streamName(),
                current.instanceId(),
                token,
                timestamp,
                current.lastProcessedToken(),
                current.lastProcessedTimestamp(),
                current.metadata()
        ));
    }

    /**
     * Updates only the {@code lastProcessedToken} pair for the given stream,
     * leaving {@code lastSeenToken} and {@code lastSeenTimestamp} untouched.
     *
     * <p>Intended for hot-path writes after a handler has successfully
     * acknowledged an event. {@code lastProcessedToken} must only advance on
     * confirmed handler success to preserve at-least-once delivery semantics.</p>
     *
     * <p>Implementations are encouraged to use a targeted update (e.g. a
     * partial {@code $set}) to avoid a round-trip read. The default
     * implementation falls back to {@link #findByStreamName(String)} followed
     * by {@link #save(Checkpoint)} so external implementations continue to
     * work unchanged.</p>
     *
     * @param streamName the stream identifier (must not be null)
     * @param token      the new {@code lastProcessedToken} value (must not be null)
     * @param timestamp  the new {@code lastProcessedTimestamp} value (must not be null)
     */
    default void saveProcessed(String streamName, BsonDocument token, Instant timestamp) {
        Checkpoint current = findByStreamName(streamName).orElseGet(() ->
                new Checkpoint(streamName, null, null, null, null, null, Map.of()));
        save(new Checkpoint(
                current.streamName(),
                current.instanceId(),
                current.lastSeenToken(),
                current.lastSeenTimestamp(),
                token,
                timestamp,
                current.metadata()
        ));
    }

    /**
     * Deletes the checkpoint for the given stream, if it exists.
     *
     * @param streamName stream identifier
     */
    void delete(String streamName);

    /**
     * Returns the shared no-op implementation that silently ignores all calls.
     */
    static CheckpointStore noOp() {
        return NoOpCheckpointStore.INSTANCE;
    }
}
