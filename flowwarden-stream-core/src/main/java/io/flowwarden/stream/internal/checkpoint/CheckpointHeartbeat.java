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

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Checkpoint heartbeat shared by both stream managers, carrying two
 * independent periodic responsibilities running on two separate threads —
 * one per checkpoint anchor:
 *
 * <ul>
 *   <li>{@link #flushTick()} — the <em>time threshold</em> of the
 *       processed-anchor policy on the flush scheduler
 *       ({@code saveIntervalSeconds}): delegates to the stream's
 *       {@link ProcessedAnchorPolicy}, which serializes every processed
 *       write (count threshold, time threshold, manual save) under its own
 *       local lock — this tick never touches the heartbeat lock, so probes
 *       and flushes can never delay each other. Never opens a cursor.</li>
 *   <li>{@link #idleTick()} — oplog-rollover protection on the dedicated
 *       probe scheduler ({@code idleHeartbeatIntervalSeconds}), the
 *       <strong>sole writer</strong> of {@code lastSeenToken}: probes when
 *       the main cursor has settled nothing for the idle threshold, and
 *       persists exclusively server-certified PBRTs. A token carried by a
 *       delivered event is never written as seen — a resume that replays
 *       old events therefore cannot regress the certified position by
 *       construction, with no state machine involved.</li>
 * </ul>
 *
 * <p><strong>Persist-then-publish.</strong> A probe PBRT is written to the
 * store first and only installed into the shared in-memory snapshot on
 * success (CAS — a concurrent event always wins). A store failure therefore
 * never erases a dirty event token from memory: the next flush retries
 * it.</p>
 *
 * <p><strong>No write after cancellation.</strong> Stream cleanup calls
 * {@link #cancel()} before cancelling the scheduled tasks; both ticks
 * re-check the flag immediately before every store write, so a slow
 * in-flight probe cannot stamp a fresh heartbeat on a dead stream.</p>
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
     * when no in-memory position exists. Derived from the resume cascade's
     * actual outcome ({@code seed != null}), not from the annotation: a
     * {@code LATEST} stream, a {@code RESUME_FROM_OPLOG_START} recovery or
     * any start without an established position must never chain from a
     * checkpoint that is ignored or known-invalid.
     */
    private final boolean allowPersistedFallback;

    private volatile boolean cancelled;

    /**
     * Serializes flush and probe work for THIS stream across the two
     * scheduler threads. The flush side only ever {@code tryLock}s — the
     * flush thread must never wait behind a probe, or one slow probe would
     * re-couple every other stream's flush cadence.
     */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * The stream's processed-anchor persistence policy — the single
     * serialization point of every {@code lastProcessedToken} write. May be
     * null when the stream has no checkpoint annotation (nothing to flush).
     */
    private final ProcessedAnchorPolicy processedPolicy;

    /**
     * Last certified position this heartbeat durably installed as
     * {@code lastSeenToken}. Guarded by {@link #lock}. Deliberately seeded
     * null: the first certification of a run always goes through a durable
     * write — the store may hold a stale (even pathological) seen from an
     * earlier version, and comparing against the probe's chain token would
     * not detect it (the chain token is a settled event anchored as
     * processed, not the stored seen). One extra write per start, and any
     * stale seen heals at the first certification by construction.
     */
    private BsonDocument lastCertifiedSeen;

    /** Consecutive EVENT_PENDING outcomes. Guarded by {@link #lock}. */
    private int consecutiveAbstentions;

    /**
     * Last time {@link #idleTick()} actually ran a probe. The idle check
     * fires on a short cadence to keep the configured threshold an actual
     * bound, so this throttle keeps probes spaced a full idle interval apart
     * during sustained idleness (abstentions and failures must not retry at
     * the check cadence). Guarded by {@link #lock}.
     */
    private Instant lastIdleProbeAt;

    /**
     * {@code RESUME_FROM_OPLOG_START} recovery: last-resort chain source
     * while no token-based position exists (never "now"). Cleared after the
     * first successful durable write. Guarded by {@link #lock}.
     */
    private org.bson.BsonTimestamp initialOperationTime;

    /**
     * {@code OPLOG_START} recovery only: the dead tokens are deliberately
     * left in the checkpoint as the durable "recovery due" marker, and the
     * establishment write performs the deferred cleanup through
     * {@code resetAfterHistoryLost} — whose at-least-once guard (the dead
     * processed token below, evaluated atomically by the store) preserves a
     * processed token re-acquired during the replay. Guarded by
     * {@link #lock}.
     */
    private boolean pendingHistoryLostReset;
    private final BsonDocument deadProcessedToken;

    public CheckpointHeartbeat(String streamName,
                               CheckpointStore checkpointStore,
                               HeartbeatProbe probe,
                               Supplier<AtomicReference<TokenSnapshot>> latestTokenRef,
                               boolean allowPersistedFallback,
                               Duration idleThreshold,
                               org.bson.BsonTimestamp initialOperationTime,
                               BsonDocument deadProcessedToken,
                               ProcessedAnchorPolicy processedPolicy) {
        this.streamName = streamName;
        this.checkpointStore = checkpointStore;
        this.probe = probe;
        this.latestTokenRef = latestTokenRef;
        this.allowPersistedFallback = allowPersistedFallback;
        this.idleThreshold = idleThreshold;
        this.initialOperationTime = initialOperationTime;
        this.pendingHistoryLostReset = initialOperationTime != null;
        this.deadProcessedToken = deadProcessedToken;
        this.processedPolicy = processedPolicy;
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
     * Whether this heartbeat has not been {@linkplain #cancel() cancelled}.
     * Used by the manager's transient retry chain to stop rescheduling once
     * the stream is gone.
     */
    public boolean isActive() {
        return !cancelled;
    }

    /**
     * Whether the transient probe chain still has work to do: a
     * {@code RESUME_FROM_OPLOG_START} recovery that has not yet produced a
     * <em>durable</em> write. A delivered token is only a new chain source,
     * not proof that a position was persisted — only a successful
     * {@code writeSeen} clears {@code initialOperationTime} and releases the
     * chain.
     */
    public boolean needsEstablishment() {
        lock.lock();
        try {
            return initialOperationTime != null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Time threshold of the processed-anchor policy
     * ({@code saveIntervalSeconds} cadence, flush scheduler): delegates to
     * the {@link ProcessedAnchorPolicy}, whose own lock serializes this
     * write against the count threshold and the manual save — an in-flight
     * write of an older token can never land after a newer one. Does not
     * touch the heartbeat lock: probes never delay the flush and vice
     * versa. Never throws.
     */
    public void flushTick() {
        if (cancelled || processedPolicy == null) {
            return;
        }
        processedPolicy.flushIfDirty();
    }

    /**
     * Idle-protection tick (short check cadence on the probe scheduler):
     * probes when the main cursor has been idle past the threshold. During
     * sustained idleness, probes stay spaced a full idle interval apart
     * regardless of the check cadence. Never throws.
     */
    public void idleTick() {
        if (cancelled) {
            return;
        }
        lock.lock();
        try {
            Instant now = Instant.now();
            TokenSnapshot snapshot = latestTokenRef.get().get();
            if (snapshot != null
                    && Duration.between(snapshot.timestamp(), now)
                            .compareTo(idleThreshold) < 0) {
                return; // the main cursor progressed recently — not idle
            }
            if (lastIdleProbeAt != null
                    && Duration.between(lastIdleProbeAt, now)
                            .compareTo(idleThreshold) < 0) {
                return; // probes stay spaced a full interval apart
            }
            lastIdleProbeAt = now;
            runProbe(snapshot);
        } catch (Exception e) {
            FlowWardenMetrics.get().onCheckpointFailed(streamName, e);
            log.warn("Failed idle heartbeat for stream '{}': {}", streamName, e.getMessage(), e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Unconditional probe on the probe scheduler, bypassing the idleness
     * precondition. Two uses: the asynchronous startup diagnostic (an
     * incompatible probe pipeline surfaces as WARN +
     * {@code onHeartbeatProbeFailed} right away instead of an idle-interval
     * later — best-effort, not a startup precondition), and the transient
     * establishment retry chain of the {@code OPLOG_START} recovery (which
     * needs a certification attempt regardless of the idle policy, including
     * when idle probing is opted out). Never throws.
     */
    public void probeNow() {
        if (cancelled) {
            return;
        }
        lock.lock();
        try {
            runProbe(latestTokenRef.get().get());
        } catch (Exception e) {
            FlowWardenMetrics.get().onCheckpointFailed(streamName, e);
            log.warn("Heartbeat probe failed for stream '{}': {}",
                    streamName, e.getMessage(), e);
        } finally {
            lock.unlock();
        }
    }

    private void runProbe(TokenSnapshot snapshot) {
        BsonDocument chainToken = snapshot != null ? snapshot.token()
                : (allowPersistedFallback ? persistedSeenToken() : null);
        ProbeOutcome outcome;
        if (chainToken != null) {
            outcome = probe.probe(chainToken);
        } else if (initialOperationTime != null) {
            // RESUME_FROM_OPLOG_START recovery with no position yet: chain
            // from the recovery operation time — never from "now", which
            // would skip the very history this recovery promises to replay.
            outcome = probe.probeFromOperationTime(initialOperationTime);
        } else {
            // No position exists to chain from (e.g. StartPosition.LATEST with
            // no event yet). Probing from "now" is unsafe — skip.
            log.debug("Heartbeat probe skipped for stream '{}': no position to chain from",
                    streamName);
            return;
        }
        switch (outcome.type()) {
            case EVENT_PENDING -> {
                // Undelivered events sit between the chained token and the
                // head — the main stream must deliver them first. A certified
                // position never crosses unsettled work. Repeated abstentions
                // mean the stream is lagging or stuck.
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
        if (!pendingHistoryLostReset && pbrt.equals(lastCertifiedSeen)) {
            // Fast path only when THIS position is already durably installed
            // and no establishment/reset is owed (during a recovery even an
            // identical PBRT must go through writeSeen — the chain must be
            // able to terminate). Re-certified valid: heartbeat-only write.
            if (cancelled) {
                return;
            }
            checkpointStore.saveHeartbeat(streamName, now);
            log.debug("Heartbeat re-certified position for stream '{}'", streamName);
            return;
        }
        // Persist FIRST, publish to memory only on success: a store failure
        // must never erase a dirty event token from the shared snapshot.
        writeSeen(pbrt, now, now);
        // A concurrent event always wins the in-memory race; its (newer)
        // position stays dirty and the next flush persists it.
        latestTokenRef.get().compareAndSet(snapshot,
                new TokenSnapshot(pbrt, now, TokenSnapshot.Source.SEED));
    }

    /**
     * Persists a certified seen position (+ heartbeat). The only writer of
     * {@code lastSeenToken}: a server-certified PBRT from the probe, or the
     * establishment write of an {@code OPLOG_START} recovery. Caller holds
     * {@link #lock}. Throws on store failure — state is only updated after a
     * successful write.
     */
    private void writeSeen(BsonDocument token, Instant positionTimestamp, Instant now) {
        if (cancelled) {
            return;
        }
        if (pendingHistoryLostReset) {
            // OPLOG_START recovery, deferred cleanup: the dead tokens stayed
            // in the checkpoint as the durable "recovery due" marker; this
            // first durable write replaces them. The store evaluates the
            // dead-processed guard atomically: a processed token re-acquired
            // during the replay is preserved.
            checkpointStore.resetAfterHistoryLost(streamName, token, deadProcessedToken, now);
        } else {
            checkpointStore.saveSeen(streamName, token, positionTimestamp, now);
        }
        lastCertifiedSeen = token;
        initialOperationTime = null; // a durable position exists from here on
        pendingHistoryLostReset = false;
        FlowWardenMetrics.get().onCheckpoint(streamName, token.toJson());
        log.debug("Certified lastSeenToken update for stream '{}'", streamName);
    }

    private BsonDocument persistedSeenToken() {
        return checkpointStore.findByStreamName(streamName)
                .map(io.flowwarden.stream.spi.Checkpoint::lastSeenToken)
                .orElse(null);
    }
}
