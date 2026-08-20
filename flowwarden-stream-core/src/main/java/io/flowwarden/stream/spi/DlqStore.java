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
 *
 * <p>The contract has two halves. The <strong>hot path</strong> is
 * {@link #save}: the runtime invokes it on every handler failure. The
 * <strong>cold path</strong> is the {@code find*} read methods: they exist
 * for downstream consumers (forensics, a replay UI) and are never called by
 * the runtime itself. The reads are {@code default} and return empty so a
 * publish-only backend (message queue, log sink — anywhere consumed means
 * gone) can implement {@code save} honestly without faking reads it cannot
 * serve. Stateful backends that can replay override them.</p>
 */
public interface DlqStore {

    /**
     * Persists a failed event into the DLQ.
     *
     * <p>The {@link DlqPolicy} carries cross-cutting policy ({@code retentionDays},
     * {@code includeOriginalDocument}, {@code includeStackTrace}). The caller has
     * already applied the include-flags to the {@link FailedEvent} payload, so
     * implementations typically only use {@code policy.retentionDays()} to set
     * up backend-native expiry (TTL index, message-TTL, etc.).</p>
     *
     * @param event  the failed event to save
     * @param policy cross-cutting DLQ policy for this entry
     */
    void save(FailedEvent event, DlqPolicy policy);

    /**
     * Retrieves a failed event by its unique identifier.
     *
     * <p>Cold path — read by downstream consumers, never by the runtime.
     * Defaults to empty so publish-only backends can ship without faking
     * reads; backends that can replay override.</p>
     *
     * @param id event identifier
     * @return the failed event, or empty if none exists (always empty for
     *         a backend that does not override this method)
     */
    default Optional<FailedEvent> findById(String id) {
        return Optional.empty();
    }

    /**
     * Retrieves all failed events for the given stream.
     *
     * <p>Cold path — see {@link #findById(String)}.</p>
     *
     * @param streamName stream identifier
     * @return list of failed events (may be empty; always empty for a
     *         backend that does not override this method)
     */
    default List<FailedEvent> findByStreamName(String streamName) {
        return List.of();
    }

    /**
     * Returns the shared no-op implementation that silently ignores all calls.
     */
    static DlqStore noOp() {
        return NoOpDlqStore.INSTANCE;
    }
}
