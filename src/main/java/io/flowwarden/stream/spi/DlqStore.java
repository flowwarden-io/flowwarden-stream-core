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

import java.util.List;
import java.util.Optional;

/**
 * SPI for persisting failed events into a Dead Letter Queue.
 *
 * <p>The Core ships with a {@linkplain #noOp() no-op} implementation and a
 * MongoDB-backed implementation registered via auto-configuration.
 * Users may provide their own implementation as a Spring bean.</p>
 */
public interface DlqStore {

    /**
     * Persists a failed event into the DLQ.
     *
     * @param event the failed event to save
     */
    void save(FailedEvent event);

    /**
     * Retrieves a failed event by its unique identifier.
     *
     * @param id event identifier
     * @return the failed event, or empty if none exists
     */
    Optional<FailedEvent> findById(String id);

    /**
     * Retrieves all failed events for the given stream.
     *
     * @param streamName stream identifier
     * @return list of failed events (may be empty)
     */
    List<FailedEvent> findByStreamName(String streamName);

    /**
     * Returns the shared no-op implementation that silently ignores all calls.
     */
    static DlqStore noOp() {
        return NoOpDlqStore.INSTANCE;
    }
}
