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
 * {@link #save}: the runtime invokes it when an event settles to the DLQ —
 * retry budget exhausted, {@code @OnError} returning {@code DLQ}, or a manual
 * {@code ctx.sendToDlq(...)}; a failure that will be retried never reaches
 * the store. The
 * <strong>cold path</strong> is the {@code find*} read methods: they exist
 * for downstream consumers (forensics, a replay UI) and are never called by
 * the runtime itself. The reads are {@code default} and return empty so a
 * publish-only backend (message queue, log sink — anywhere consumed means
 * gone) can implement {@code save} honestly without faking reads it cannot
 * serve. Stateful backends that can replay override them.</p>
 *
 * <p>A third, optional, <strong>startup</strong> hook —
 * {@link #onStreamRegistered(DlqStreamRegistration)} — tells the store which
 * streams will write to it before any of them starts, so a backend can
 * provision per-stream resources (a collection and its TTL index, a queue,
 * a topic) once instead of lazily on the first {@code save}. The default
 * does nothing.</p>
 */
public interface DlqStore {

    /**
     * Startup hook: called once per DLQ-enabled stream (a stream carrying
     * {@code @DeadLetterQueue}, or a contributed one with a
     * {@code DeadLetterQueueSpec}) when the application is ready, before
     * that stream starts and therefore before any of its events can reach
     * {@link #save}. Streams without a dead letter queue are not announced.
     *
     * <p>The default is a no-op: a backend that resolves everything from
     * {@code event.streamName()} inside {@code save} needs nothing here.
     * The core's MongoDB stores use it to bind the stream to its
     * {@code @MongoDlqOptions} collection and create the TTL / status
     * indexes there — which is why a store that <em>wraps</em> another one
     * (a composite routing to several backends, a decorator adding
     * metrics) <strong>must forward this call to every delegate</strong>;
     * otherwise the wrapped Mongo store writes every stream to the default
     * collection and enforces no retention.</p>
     *
     * <p>Implementations should be idempotent — the runtime calls the hook
     * once per stream per application start, but a restart of the same
     * JVM context, or a composite forwarding to a delegate that is also a
     * bean on its own, may announce the same stream twice. An exception
     * thrown here is logged at WARN and does not prevent the stream from
     * starting: a DLQ that cannot be provisioned must not take the
     * consumer down with it.</p>
     *
     * @param registration the stream, its policy and the backend hints the
     *                     runtime resolved for it; never {@code null}
     */
    default void onStreamRegistered(DlqStreamRegistration registration) {
    }

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
     * Number of pending DLQ entries for the stream — its standing backlog,
     * i.e. entries with status {@link FailedEvent#STATUS_PENDING} waiting to
     * be reprocessed.
     *
     * <p>Cold path — see {@link #findById(String)}. Defaults to {@code -1},
     * meaning this backend cannot serve counts (publish-only backends:
     * queue consumed means gone). Stateful backends override with a real
     * count; the runtime only emits a backlog gauge for non-negative
     * values, so an unknowing backend never produces a lying zero.</p>
     *
     * @param streamName stream identifier
     * @return the pending entry count, or {@code -1} when the backend
     *         cannot count
     */
    default long count(String streamName) {
        return -1;
    }

    /**
     * Returns the shared no-op implementation that silently ignores all calls.
     */
    static DlqStore noOp() {
        return NoOpDlqStore.INSTANCE;
    }
}
