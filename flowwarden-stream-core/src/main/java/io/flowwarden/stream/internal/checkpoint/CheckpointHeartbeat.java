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
 * independent periodic responsibilities running on two separate threads:
 *
 * <ul>
 *   <li>{@link #flushTick()} — write-pressure coalescing on the flush
 *       scheduler ({@code saveIntervalSeconds}): persists the latest
 *       <em>event</em> token only if it changed since the last flush. Never
 *       opens a cursor, and never blocks: if this stream's probe is in
 *       flight, the tick is skipped and the token stays dirty for the next
 *       one.</li>
 *   <li>{@link #idleTick()} — oplog-rollover protection on the dedicated
 *       probe scheduler ({@code idleHeartbeatIntervalSeconds}): probes when
 *       the main cursor has delivered nothing for the idle threshold, or
 *       unconditionally while the stream is {@linkplain #isCatchingUp()
 *       catching up} after a resume from a position behind the persisted
 *       one.</li>
 * </ul>
 *
 * <p><strong>Catch-up state.</strong> A {@code PROCESSED_FIRST} resume starts
 * the stream behind the persisted {@code lastSeenToken}. Resume tokens carry
 * no client-comparable order (the driver spec defines none), so instead of
 * comparing tokens the heartbeat tracks an explicit {@code catchingUp} flag:
 * while set, replayed events are neither persisted as seen nor allowed to
 * refresh the heartbeat (nothing has been re-certified), and the probe runs
 * without the idleness precondition. A probe returning {@code EMPTY} with a
 * winning CAS certifies the backlog is consumed: its PBRT is persisted as
 * the new safe position and normal semantics resume. Outside catch-up no
 * ordering is needed at all — the stream resumed exactly at the persisted
 * position, so every delivered event is past it by construction.</p>
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
     * Last {@code lastSeenToken} value this heartbeat persisted. Guarded by
     * {@link #lock}.
     */
    private BsonDocument lastPersistedSeen;

    /** Catch-up state, see class Javadoc. Guarded by {@link #lock}. */
    private boolean catchingUp;

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

    public CheckpointHeartbeat(String streamName,
                               CheckpointStore checkpointStore,
                               HeartbeatProbe probe,
                               Supplier<AtomicReference<TokenSnapshot>> latestTokenRef,
                               boolean allowPersistedFallback,
                               Duration idleThreshold,
                               boolean startInCatchUp) {
        this.streamName = streamName;
        this.checkpointStore = checkpointStore;
        this.probe = probe;
        this.latestTokenRef = latestTokenRef;
        this.allowPersistedFallback = allowPersistedFallback;
        this.idleThreshold = idleThreshold;
        this.catchingUp = startInCatchUp;
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

    public boolean isCatchingUp() {
        lock.lock();
        try {
            return catchingUp;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Whether this heartbeat has not been {@linkplain #cancel() cancelled}.
     * Used by the manager's transient catch-up retry chain to stop
     * rescheduling once the stream is gone.
     */
    public boolean isActive() {
        return !cancelled;
    }

    /**
     * Write-coalescing flush ({@code saveIntervalSeconds} cadence, flush
     * scheduler): persists the latest event token only if dirty. Clean tick,
     * catch-up in progress, or this stream's probe in flight → zero writes,
     * zero blocking. Never throws.
     */
    public void flushTick() {
        if (cancelled) {
            return;
        }
        if (!lock.tryLock()) {
            // This stream's probe is in flight: skip — the token stays dirty
            // and the next flush retries. The flush thread never waits.
            return;
        }
        try {
            if (cancelled || catchingUp) {
                // During catch-up, delivered events are replays: neither a
                // seen position nor a health confirmation.
                return;
            }
            TokenSnapshot snapshot = latestTokenRef.get().get();
            if (snapshot == null
                    || snapshot.source() != TokenSnapshot.Source.EVENT
                    || snapshot.token().equals(lastPersistedSeen)) {
                return; // nothing dirty — SEED positions are never flushed
            }
            writeSeen(snapshot.token(), snapshot.timestamp(), Instant.now());
        } catch (Exception e) {
            FlowWardenMetrics.get().onCheckpointFailed(streamName, e);
            log.warn("Failed to flush periodic checkpoint for stream '{}': {}",
                    streamName, e.getMessage(), e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Idle-protection tick (short check cadence on the probe scheduler):
     * probes when the main cursor has been idle past the threshold. During
     * sustained idleness, probes stay spaced a full idle interval apart
     * regardless of the check cadence. While catching up, this tick abstains
     * entirely — the transient catch-up chain is the sole owner of probing
     * during that transition. Never throws.
     */
    public void idleTick() {
        if (cancelled) {
            return;
        }
        lock.lock();
        try {
            if (catchingUp) {
                return; // the catch-up chain owns probing during catch-up
            }
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
     * catch-up retry chain (which needs a certification attempt regardless of
     * the idle policy, including when idle probing is opted out). Never
     * throws.
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
        if (!catchingUp && pbrt.equals(chainToken)) {
            // Position unchanged but re-certified valid: heartbeat-only write.
            if (cancelled) {
                return;
            }
            checkpointStore.saveHeartbeat(streamName, now);
            log.debug("Heartbeat re-certified position for stream '{}'", streamName);
            return;
        }
        // Persist FIRST, publish to memory only on success: a store failure
        // must never erase a dirty event token from the shared snapshot.
        boolean wasCatchingUp = catchingUp;
        writeSeen(pbrt, now, now);
        if (wasCatchingUp) {
            log.info("Stream '{}' caught up: server certified the backlog consumed, "
                    + "resume position re-established", streamName);
        }
        // A concurrent event always wins the in-memory race; its (newer)
        // position stays dirty and the next flush persists it.
        latestTokenRef.get().compareAndSet(snapshot,
                new TokenSnapshot(pbrt, now, TokenSnapshot.Source.SEED));
    }

    /**
     * Persists a seen position (+ heartbeat) and clears the catch-up state.
     * Caller holds {@link #lock}. Throws on store failure — state is only
     * updated after a successful write.
     */
    private void writeSeen(BsonDocument token, Instant positionTimestamp, Instant now) {
        if (cancelled) {
            return;
        }
        checkpointStore.saveSeen(streamName, token, positionTimestamp, now);
        lastPersistedSeen = token;
        catchingUp = false;
        FlowWardenMetrics.get().onCheckpoint(streamName, token.toJson());
        log.debug("Periodic lastSeenToken update for stream '{}'", streamName);
    }

    private BsonDocument persistedSeenToken() {
        return checkpointStore.findByStreamName(streamName)
                .map(io.flowwarden.stream.spi.Checkpoint::lastSeenToken)
                .orElse(null);
    }
}
