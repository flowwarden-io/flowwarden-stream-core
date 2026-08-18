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
                current.lastHeartbeatTimestamp(),
                current.metadata()
        ));
    }

    /**
     * Updates the {@code lastSeenToken} pair together with
     * {@code lastHeartbeatTimestamp} for the given stream, leaving
     * {@code lastProcessedToken} and {@code lastProcessedTimestamp} untouched.
     *
     * <p>Used by the heartbeat when a new gap-free position is confirmed
     * (fresh event token, or empty probe returning a new PBRT). The default
     * implementation delegates to
     * {@link #saveSeen(String, BsonDocument, Instant)} followed by
     * {@link #saveHeartbeat(String, Instant)} — <strong>two writes, not
     * atomic</strong>: a crash between them can leave the position advanced
     * without its confirmation, or third parties may observe the intermediate
     * state. Implementations backed by a store supporting multi-field updates
     * (the shipped Mongo stores do) should override with a single atomic
     * write.</p>
     *
     * @param streamName         the stream identifier (must not be null)
     * @param token              the new {@code lastSeenToken} value (must not be null)
     * @param timestamp          the new {@code lastSeenTimestamp} value (must not be null)
     * @param heartbeatTimestamp the new {@code lastHeartbeatTimestamp} value (must not be null)
     */
    default void saveSeen(String streamName, BsonDocument token, Instant timestamp,
                          Instant heartbeatTimestamp) {
        saveSeen(streamName, token, timestamp);
        saveHeartbeat(streamName, heartbeatTimestamp);
    }

    /**
     * Updates only {@code lastHeartbeatTimestamp} for the given stream, leaving
     * every token and timestamp untouched.
     *
     * <p>Used by the heartbeat when a probe re-certifies the current position
     * without advancing it (empty probe returning the same PBRT): the position
     * is unchanged but its validity has just been confirmed. Never called on
     * probe abstentions or failures.</p>
     *
     * <p>The default implementation falls back to
     * {@link #findByStreamName(String)} followed by {@link #save(Checkpoint)}
     * so external implementations continue to work unchanged.</p>
     *
     * @param streamName         the stream identifier (must not be null)
     * @param heartbeatTimestamp the new {@code lastHeartbeatTimestamp} value (must not be null)
     */
    default void saveHeartbeat(String streamName, Instant heartbeatTimestamp) {
        Checkpoint current = findByStreamName(streamName).orElseGet(() ->
                new Checkpoint(streamName, null, null, null, null, null, Map.of()));
        save(new Checkpoint(
                current.streamName(),
                current.instanceId(),
                current.lastSeenToken(),
                current.lastSeenTimestamp(),
                current.lastProcessedToken(),
                current.lastProcessedTimestamp(),
                heartbeatTimestamp,
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
                current.lastHeartbeatTimestamp(),
                current.metadata()
        ));
    }

    /**
     * Resets the checkpoint after a change stream history loss: the seen
     * position is either replaced by the fresh server-certified one
     * ({@code freshSeenToken} non-null, which also confirms
     * {@code lastHeartbeatTimestamp}) or cleared along with
     * {@code lastHeartbeatTimestamp} ({@code freshSeenToken} null — a
     * heartbeat without any recoverable position would be a lie), and the
     * expired processed pair is removed <strong>conditionally</strong>: only
     * while {@code lastProcessedToken} still equals
     * {@code expectedDeadProcessed}. If a fresh processed token was acquired
     * concurrently (replay handler success, manual checkpoint), it is
     * preserved and only the seen position is written. {@code instanceId}
     * and {@code metadata} are preserved.
     *
     * <p><strong>The conditional check is the at-least-once coordination
     * point, and its race-free guarantee only holds for implementations that
     * evaluate it atomically with the processed removal</strong> — the
     * shipped Mongo stores put the expected value in the update filter of a
     * single {@code $set}/{@code $unset} operation (which additionally
     * preserves fields unknown to this SPI). Implementations whose backend
     * can express a conditional write MUST override this method to get that
     * guarantee. The default implementation is a sequential
     * read-modify-write kept only for source compatibility: a
     * {@code saveProcessed} racing between its read and its write can be
     * lost (a freshly re-acquired processed anchor unset), and it can only
     * preserve the fields of the {@link Checkpoint} model. The testkit
     * contract validates the sequential semantics of this method, not its
     * atomicity.</p>
     *
     * @param streamName            the stream identifier (must not be null)
     * @param freshSeenToken        the fresh certified position, or
     *                              {@code null} to clear the seen pair as well
     * @param expectedDeadProcessed the expired processed token read when the
     *                              history loss was detected (may be null)
     * @param timestamp             the recovery instant (must not be null)
     */
    default void resetAfterHistoryLost(String streamName, BsonDocument freshSeenToken,
                                       BsonDocument expectedDeadProcessed, Instant timestamp) {
        Checkpoint current = findByStreamName(streamName).orElseGet(() ->
                new Checkpoint(streamName, null, null, null, null, null, Map.of()));
        boolean processedStillDead =
                java.util.Objects.equals(current.lastProcessedToken(), expectedDeadProcessed);
        save(new Checkpoint(
                current.streamName(),
                current.instanceId(),
                freshSeenToken,
                freshSeenToken != null ? timestamp : null,
                processedStillDead ? null : current.lastProcessedToken(),
                processedStillDead ? null : current.lastProcessedTimestamp(),
                freshSeenToken != null ? timestamp : null,
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
