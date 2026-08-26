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

import io.flowwarden.stream.FlowWardenMetrics;
import io.flowwarden.stream.spi.CheckpointStore;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Per-stream persistence policy of {@code lastProcessedToken} — the single
 * serialization point for its three writers: the count threshold
 * ({@code saveEveryN}, delivery path), the time threshold
 * ({@code saveIntervalSeconds}, flush scheduler) and the manual
 * {@code ctx.saveCheckpointNow()}. Every write goes through one local lock,
 * so a slow write of an older token can never land after — and silently
 * regress — a newer one.
 *
 * <p><strong>Counter semantics.</strong> The counter means "settlements
 * since the last <em>confirmed</em> persistence" and resets only on a
 * successful write. A failed threshold write leaves the state dirty and
 * due: the very next settlement (or the timer) retries, keeping the
 * documented bound of at most {@code N-1} replayed settlements after a
 * crash — a plain modulo counter would silently wait until {@code 2N}
 * after a failure.</p>
 *
 * <p>A settlement whose token is already confirmed durable (a successful
 * manual save) resets the counter instead of counting: the automatic
 * thresholds never rewrite a clean anchor.</p>
 *
 * <p>Deliberately free of any resume or probe state — the seen anchor has
 * its own single writer (the heartbeat probe) and needs no coordination
 * with this policy.</p>
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public final class ProcessedAnchorPolicy {

    private static final Logger log = LoggerFactory.getLogger(ProcessedAnchorPolicy.class);

    private final String streamName;
    private final CheckpointStore checkpointStore;
    private final int saveEveryN;

    private volatile boolean cancelled;

    /** Serializes every processed write across its three writers. */
    private final ReentrantLock lock = new ReentrantLock();

    /** Latest settled token and its anchor timestamp. Guarded by {@link #lock}. */
    private BsonDocument latestToken;
    private Instant latestTimestamp;

    /** Last token confirmed durable. Guarded by {@link #lock}. */
    private BsonDocument lastConfirmed;

    /** Settlements since the last confirmed persistence. Guarded by {@link #lock}. */
    private int settlementsSinceConfirmed;

    public ProcessedAnchorPolicy(String streamName, CheckpointStore checkpointStore,
                                 int saveEveryN) {
        this.streamName = streamName;
        this.checkpointStore = checkpointStore;
        this.saveEveryN = Math.max(1, saveEveryN);
    }

    /**
     * Invalidates this policy: no store write will happen after this call
     * returns (a write already in flight on the wire is the only residual
     * window). Called by stream cleanup alongside the heartbeat's cancel.
     */
    public void cancel() {
        cancelled = true;
    }

    /**
     * Delivery-path entry: records a terminally settled token and applies
     * the count threshold. Never throws.
     *
     * @param token           the settled event's resume token
     * @param anchorTimestamp the event's cluster time when known, or the
     *                        settlement instant
     */
    public void onSettled(BsonDocument token, Instant anchorTimestamp) {
        if (token == null) {
            return;
        }
        lock.lock();
        try {
            latestToken = token;
            latestTimestamp = anchorTimestamp;
            if (token.equals(lastConfirmed)) {
                // Already durable (successful manual save): nothing owed.
                settlementsSinceConfirmed = 0;
                return;
            }
            settlementsSinceConfirmed++;
            if (settlementsSinceConfirmed >= saveEveryN) {
                persistLocked(token, anchorTimestamp);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Time-threshold entry ({@code saveIntervalSeconds} cadence): persists
     * the latest settled token only if dirty. A clean anchor is never
     * rewritten. Never throws.
     */
    public void flushIfDirty() {
        if (cancelled) {
            return;
        }
        lock.lock();
        try {
            if (latestToken == null || latestToken.equals(lastConfirmed)) {
                return;
            }
            persistLocked(latestToken, latestTimestamp);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Manual entry ({@code ctx.saveCheckpointNow()}): persists the given
     * token immediately and reports whether the write was confirmed — the
     * caller must not declare a failed write durable. Never throws.
     */
    public boolean saveNow(BsonDocument token, Instant anchorTimestamp) {
        if (token == null) {
            return false;
        }
        lock.lock();
        try {
            latestToken = token;
            latestTimestamp = anchorTimestamp;
            return persistLocked(token, anchorTimestamp);
        } finally {
            lock.unlock();
        }
    }

    /** Caller holds {@link #lock}. Returns whether the write was confirmed. */
    private boolean persistLocked(BsonDocument token, Instant anchorTimestamp) {
        if (cancelled) {
            return false;
        }
        Instant now = Instant.now();
        try {
            checkpointStore.saveProcessed(streamName, token,
                    anchorTimestamp != null ? anchorTimestamp : now, now);
            lastConfirmed = token;
            settlementsSinceConfirmed = 0;
            FlowWardenMetrics.get().onCheckpoint(streamName, token.toJson());
            return true;
        } catch (RuntimeException e) {
            // Stay dirty and due: the next settlement or timer tick retries.
            FlowWardenMetrics.get().onCheckpointFailed(streamName, e);
            log.warn("Failed to save checkpoint for stream '{}': {}",
                    streamName, e.getMessage(), e);
            return false;
        }
    }
}
