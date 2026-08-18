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
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Checkpoint heartbeat shared by both stream managers, carrying two
 * independent periodic responsibilities:
 *
 * <ul>
 *   <li>{@link #flushTick()} — write-pressure coalescing, driven by
 *       {@code saveIntervalSeconds}: persists the latest <em>event</em> token
 *       only if it changed since the last flush. Never opens a cursor.</li>
 *   <li>{@link #idleTick()} — oplog-rollover protection, driven by
 *       {@code idleHeartbeatIntervalSeconds}: when the main cursor has not
 *       delivered anything for the idle threshold, runs a
 *       {@link HeartbeatProbe} chained from the last known position and — on
 *       a server-certified empty interval — advances the persisted position
 *       to the returned PBRT. Any main-cursor activity re-arms the delay;
 *       active streams never probe.</li>
 * </ul>
 *
 * <p>{@link #startupValidation()} runs one probe at stream start regardless
 * of idleness, so an incompatible pipeline fails loudly at boot instead of an
 * idle-interval later.</p>
 *
 * <p>Correctness properties enforced here:</p>
 * <ul>
 *   <li><strong>Seeds are not events.</strong> A resume position installed at
 *       startup ({@link TokenSnapshot.Source#SEED}) is a chaining point, never
 *       a "newly delivered" token — a {@code PROCESSED_FIRST} resume seeds the
 *       (older) processed token, and flushing it as seen would destroy the
 *       level-2 safety net.</li>
 *   <li><strong>Seen never regresses.</strong> Every seen write is guarded by
 *       a monotonicity check against the persisted high-water mark (resume
 *       token {@code _data} strings of one stream are lexicographically
 *       ordered). Replayed events after a {@code PROCESSED_FIRST} resume and
 *       bounded-scan intermediate PBRTs are downgraded to heartbeat-only
 *       writes.</li>
 *   <li><strong>No write after cancellation.</strong> Stream cleanup calls
 *       {@link #cancel()} before cancelling the scheduled tasks; both ticks
 *       re-check the flag immediately before every store write, so a slow
 *       in-flight probe cannot stamp a fresh heartbeat on a dead stream.</li>
 * </ul>
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public final class CheckpointHeartbeat {

    private static final Logger log = LoggerFactory.getLogger(CheckpointHeartbeat.class);

    /**
     * Consecutive probe abstentions before warning: a probe abstaining in a
     * loop means matching events keep sitting undelivered — the stream is
     * lagging or stuck. Not a probe error, but worth surfacing.
     */
    static final int ABSTENTION_WARN_THRESHOLD = 3;

    private final String streamName;
    private final CheckpointStore checkpointStore;
    private final HeartbeatProbe probe;
    private final Supplier<AtomicReference<TokenSnapshot>> latestTokenRef;
    private final Duration idleThreshold;

    /**
     * Whether the probe may fall back to the persisted {@code lastSeenToken}
     * when no in-memory position exists. True only for
     * {@code StartPosition.RESUME} streams — a {@code LATEST} stream's
     * semantics explicitly ignore persisted tokens, and chaining from one
     * would strand the heartbeat on history the main stream will never
     * consume.
     */
    private final boolean allowPersistedFallback;

    private volatile boolean cancelled;

    /**
     * Last {@code lastSeenToken} value this heartbeat persisted. Only
     * read/written from the single-threaded interval scheduler.
     */
    private BsonDocument lastPersistedSeen;

    /**
     * Highest {@code lastSeenToken} known to be persisted (loaded from the
     * checkpoint on first use, updated on every successful seen write).
     * Guard against regressions. Scheduler-thread only.
     */
    private BsonDocument seenHighWaterMark;
    private boolean highWaterMarkLoaded;

    /** Consecutive EVENT_PENDING outcomes. Scheduler-thread only. */
    private int consecutiveAbstentions;

    public CheckpointHeartbeat(String streamName,
                               CheckpointStore checkpointStore,
                               HeartbeatProbe probe,
                               Supplier<AtomicReference<TokenSnapshot>> latestTokenRef,
                               boolean allowPersistedFallback,
                               Duration idleThreshold) {
        this.streamName = streamName;
        this.checkpointStore = checkpointStore;
        this.probe = probe;
        this.latestTokenRef = latestTokenRef;
        this.allowPersistedFallback = allowPersistedFallback;
        this.idleThreshold = idleThreshold;
    }

    /**
     * Invalidates this heartbeat: no store write will happen after this call
     * returns (a write already in flight on the wire is the only residual
     * window). Must be called by stream cleanup BEFORE cancelling the
     * scheduled tasks, so an in-flight tick cannot stamp a dead stream.
     */
    public void cancel() {
        cancelled = true;
    }

    /**
     * Write-coalescing flush ({@code saveIntervalSeconds} cadence): persists
     * the latest event token only if dirty. Clean tick → zero writes, zero
     * cursors. Never throws.
     */
    public void flushTick() {
        if (cancelled) {
            return;
        }
        try {
            TokenSnapshot snapshot = latestTokenRef.get().get();
            if (snapshot == null
                    || snapshot.source() != TokenSnapshot.Source.EVENT
                    || snapshot.token().equals(lastPersistedSeen)) {
                return; // nothing dirty — SEED positions are never flushed
            }
            loadHighWaterMarkIfNeeded();
            persistSeen(snapshot.token(), snapshot.timestamp(), Instant.now(),
                    "event token below persisted seen (replay) — heartbeat only");
        } catch (Exception e) {
            FlowWardenMetrics.get().onCheckpointFailed(streamName, e);
            log.warn("Failed to flush periodic checkpoint for stream '{}': {}",
                    streamName, e.getMessage(), e);
        }
    }

    /**
     * Idle-protection tick ({@code idleHeartbeatIntervalSeconds} cadence):
     * probes only when the main cursor has not delivered anything for the
     * idle threshold. Never throws.
     */
    public void idleTick() {
        if (cancelled) {
            return;
        }
        try {
            TokenSnapshot snapshot = latestTokenRef.get().get();
            if (snapshot != null
                    && Duration.between(snapshot.timestamp(), Instant.now())
                            .compareTo(idleThreshold) < 0) {
                return; // the main cursor progressed recently — not idle
            }
            runProbe(snapshot);
        } catch (Exception e) {
            FlowWardenMetrics.get().onCheckpointFailed(streamName, e);
            log.warn("Failed idle heartbeat for stream '{}': {}", streamName, e.getMessage(), e);
        }
    }

    /**
     * One-shot probe at stream start, bypassing the idleness precondition:
     * validates the probe pipeline against the server so an incompatibility
     * fails loudly at boot (WARN + {@code onHeartbeatProbeFailed}) instead of
     * an idle-interval later. Never throws.
     */
    public void startupValidation() {
        if (cancelled) {
            return;
        }
        try {
            runProbe(latestTokenRef.get().get());
        } catch (Exception e) {
            FlowWardenMetrics.get().onCheckpointFailed(streamName, e);
            log.warn("Startup heartbeat validation failed for stream '{}': {}",
                    streamName, e.getMessage(), e);
        }
    }

    private void runProbe(TokenSnapshot snapshot) {
        loadHighWaterMarkIfNeeded();
        BsonDocument chainToken = snapshot != null ? snapshot.token()
                : (allowPersistedFallback ? persistedSeenToken() : null);
        if (chainToken == null) {
            // No position exists to chain from (e.g. StartPosition.LATEST with
            // no event yet). Probing from "now" is unsafe — skip.
            log.debug("Heartbeat probe skipped for stream '{}': no position to chain from",
                    streamName);
            return;
        }

        ProbeOutcome outcome = probe.probe(chainToken);
        switch (outcome.type()) {
            case EVENT_PENDING -> {
                // Undelivered events sit between the chained token and the
                // head — the main stream must deliver them first. Repeated
                // abstentions mean the stream is lagging or stuck.
                consecutiveAbstentions++;
                if (consecutiveAbstentions == ABSTENTION_WARN_THRESHOLD) {
                    log.warn("Heartbeat probe abstained {} consecutive times for stream '{}': "
                                    + "matching events keep sitting undelivered — the main stream "
                                    + "appears to be lagging or stuck",
                            consecutiveAbstentions, streamName);
                } else {
                    log.debug("Heartbeat abstained for stream '{}': events pending delivery",
                            streamName);
                }
            }
            case FAILED -> {
                consecutiveAbstentions = 0;
                FlowWardenMetrics.get().onHeartbeatProbeFailed(streamName, outcome.cause());
                log.warn("Heartbeat probe failed for stream '{}': {}", streamName,
                        outcome.cause() != null ? outcome.cause().getMessage() : "unknown",
                        outcome.cause());
            }
            case EMPTY -> {
                consecutiveAbstentions = 0;
                persistCertifiedPosition(snapshot, chainToken, outcome.pbrt(), Instant.now());
            }
        }
    }

    private void persistCertifiedPosition(TokenSnapshot snapshot,
                                          BsonDocument chainToken,
                                          BsonDocument pbrt,
                                          Instant now) {
        if (pbrt.equals(chainToken)) {
            // Position unchanged but re-certified valid: heartbeat-only write.
            saveHeartbeatOnly(now, "re-certified position");
            return;
        }
        // An event delivered while the probe was in flight always wins.
        AtomicReference<TokenSnapshot> ref = latestTokenRef.get();
        if (!ref.compareAndSet(snapshot, new TokenSnapshot(pbrt, now, TokenSnapshot.Source.SEED))) {
            log.debug("Heartbeat probe result discarded for stream '{}': event token won the race",
                    streamName);
            return;
        }
        persistSeen(pbrt, now, now,
                "probe PBRT below persisted seen (bounded scan) — heartbeat only");
    }

    /**
     * Persists a seen position guarded by the monotonicity check; downgrades
     * to a heartbeat-only write when the candidate does not advance the
     * persisted high-water mark. The position remains confirmed in both
     * cases: below the high-water mark, the persisted seen token is still the
     * better resume point and the stream is demonstrably progressing through
     * its certified gap-free range.
     */
    private void persistSeen(BsonDocument token, Instant positionTimestamp, Instant now,
                             String downgradeReason) {
        if (!advances(token, seenHighWaterMark)) {
            lastPersistedSeen = token;
            saveHeartbeatOnly(now, downgradeReason);
            return;
        }
        if (cancelled) {
            return;
        }
        checkpointStore.saveSeen(streamName, token, positionTimestamp, now);
        lastPersistedSeen = token;
        seenHighWaterMark = token;
        FlowWardenMetrics.get().onCheckpoint(streamName, token.toJson());
        log.debug("Periodic lastSeenToken update for stream '{}'", streamName);
    }

    private void saveHeartbeatOnly(Instant now, String reason) {
        if (cancelled) {
            return;
        }
        checkpointStore.saveHeartbeat(streamName, now);
        log.debug("Heartbeat-only write for stream '{}': {}", streamName, reason);
    }

    private void loadHighWaterMarkIfNeeded() {
        if (!highWaterMarkLoaded) {
            seenHighWaterMark = persistedSeenToken();
            highWaterMarkLoaded = true;
        }
    }

    /**
     * Whether {@code candidate} is strictly ahead of {@code current}. Resume
     * token {@code _data} strings of a single stream are lexicographically
     * ordered (KeyString encoding); when either token has no comparable
     * {@code _data}, inequality is trusted (never observed with real MongoDB
     * tokens).
     */
    static boolean advances(BsonDocument candidate, BsonDocument current) {
        if (current == null) {
            return true;
        }
        if (candidate.equals(current)) {
            return false;
        }
        BsonValue candidateData = candidate.get("_data");
        BsonValue currentData = current.get("_data");
        if (candidateData != null && currentData != null
                && candidateData.isString() && currentData.isString()) {
            return candidateData.asString().getValue()
                    .compareTo(currentData.asString().getValue()) > 0;
        }
        return true;
    }

    private BsonDocument persistedSeenToken() {
        return checkpointStore.findByStreamName(streamName)
                .map(io.flowwarden.stream.spi.Checkpoint::lastSeenToken)
                .orElse(null);
    }
}
