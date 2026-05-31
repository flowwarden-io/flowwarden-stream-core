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
