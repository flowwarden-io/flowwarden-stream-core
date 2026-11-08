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
import io.flowwarden.stream.HistoryLostContext;
import io.flowwarden.stream.HistoryLostException;
import io.flowwarden.stream.OnHistoryLost;
import io.flowwarden.stream.ReconciliationFailedException;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.spi.CheckpointStore;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * The resume cascade, shared by both stream managers and by every entry
 * point that (re-)establishes a stream position — boot and runtime restarts
 * alike, in a fixed order: try the processed anchor (level 1), fall back to
 * the certified seen position if it has aged out (level 2), apply the
 * {@link OnHistoryLost} strategy if both have aged out (level 3), and
 * bootstrap a fresh server-certified position when no prior position exists
 * at all.
 *
 * <p>The cascade never touches a cursor builder: its outcome is the
 * immutable {@link ResumeContext}, from which the caller derives the cursor
 * position (non-null {@code seedToken} → resume after it; non-null
 * {@code initialOperationTime} → start at that operation time) and the
 * heartbeat setup — all from the single checkpoint read performed here, so
 * stream startup cannot fail after the cursor has been
 * registered/subscribed.</p>
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public final class ResumeCascade {

    private static final Logger log = LoggerFactory.getLogger(ResumeCascade.class);

    /**
     * Manager-provided resume-token probe: whether a change stream can be
     * opened after the given token (an aged-out token yields 286/136/280).
     * The imperative and reactive managers each bring their template-bound
     * implementation.
     */
    public interface TokenValidator {
        boolean isValid(BsonDocument token);
    }

    /**
     * The stream's {@code @Reconcile} hook, already bound to its bean by the
     * manager: the cascade knows neither reflection nor beans. Invoked at
     * level 3 before any durable write and before the cursor opens; whatever
     * it throws stops the start.
     */
    public interface ReconcileHook {
        void reconcile(HistoryLostContext ctx) throws Exception;
    }

    private ResumeCascade() {
    }

    /**
     * {@link #resolve(String, Checkpoint, CheckpointStore, HeartbeatProbe, TokenValidator, Supplier, boolean, ReconcileHook)}
     * without a reconciliation hook.
     */
    public static ResumeContext resolve(String streamName,
                                        Checkpoint checkpointAnnotation,
                                        CheckpointStore checkpointStore,
                                        HeartbeatProbe probe,
                                        TokenValidator tokenValidator,
                                        Supplier<BsonTimestamp> oldestOplogTimestamp,
                                        boolean oplogStartAllowed) {
        return resolve(streamName, checkpointAnnotation, checkpointStore, probe, tokenValidator,
                oldestOplogTimestamp, oplogStartAllowed, null);
    }

    /**
     * Resolves the resume position for a stream.
     *
     * @param streamName            the stream identifier
     * @param checkpointAnnotation  the stream's {@code @Checkpoint}
     *                              (history-lost strategy)
     * @param checkpointStore       the checkpoint store
     * @param probe                 the stream's heartbeat probe — used for
     *                              {@link HeartbeatProbe#initialPosition()}
     *                              during bootstraps
     * @param tokenValidator        template-bound resume-token validation
     * @param oldestOplogTimestamp  reads the oldest entry of the stream's own
     *                              cluster's oplog; may throw — the failure
     *                              falls back to a RESUME_FROM_NOW bootstrap
     * @param oplogStartAllowed     whether a {@code RESUME_FROM_OPLOG_START}
     *                              recovery may actually resume at the oldest
     *                              oplog entry. {@code false} when the
     *                              previous attempt of the same restart cycle
     *                              opened at that entry and died with a
     *                              history-lost error (the entry rolled over
     *                              between the read and the open): the
     *                              recovery then degrades to a fresh
     *                              certified position instead of losing the
     *                              same race again
     * @param reconcileHook         the stream's {@code @Reconcile} hook, or
     *                              {@code null}. With a hook, a level-3
     *                              recovery under a self-repairing strategy
     *                              invokes it before any durable write: the
     *                              dead tokens stay in the checkpoint as the
     *                              "reconciliation due" marker until the hook
     *                              has returned, whatever the strategy
     * @return the immutable resume outcome
     * @throws HistoryLostException under {@code OnHistoryLost.FAIL} when both
     *                              tokens have aged out; a
     *                              {@link ReconciliationFailedException} when
     *                              the hook threw or the reconciliation
     *                              outlived the oplog window twice
     */
    public static ResumeContext resolve(String streamName,
                                        Checkpoint checkpointAnnotation,
                                        CheckpointStore checkpointStore,
                                        HeartbeatProbe probe,
                                        TokenValidator tokenValidator,
                                        Supplier<BsonTimestamp> oldestOplogTimestamp,
                                        boolean oplogStartAllowed,
                                        ReconcileHook reconcileHook) {
        Optional<io.flowwarden.stream.spi.Checkpoint> cpOpt =
                checkpointStore.findByStreamName(streamName);
        if (cpOpt.isEmpty()) {
            // No prior checkpoint → bootstrap: capture an initial PBRT and
            // start the main stream from it, so no window is ever unprotected
            // and the heartbeat always has a position to chain from.
            return bootstrapInitialPosition(streamName, checkpointStore, probe, null);
        }
        io.flowwarden.stream.spi.Checkpoint cp = cpOpt.get();
        BsonDocument processedToken = cp.lastProcessedToken();
        BsonDocument seenToken = cp.lastSeenToken();

        // Level 1: the processed anchor — everything after the last settled
        // event is re-delivered, strict at-least-once.
        if (processedToken != null && tokenValidator.isValid(processedToken)) {
            log.info("Resuming stream '{}' from lastProcessedToken", streamName);
            return new ResumeContext(processedToken);
        }

        // Level 2: the certified position, validated exactly once. With a null
        // processed anchor (never recorded — typical after a history-lost
        // self-repair) this is not a degradation: INFO, no "aged out" warning,
        // no fallback metric.
        if (seenToken != null
                && (processedToken == null || !seenToken.equals(processedToken))
                && tokenValidator.isValid(seenToken)) {
            if (processedToken == null) {
                log.info("Resuming stream '{}' from lastSeenToken "
                        + "(lastProcessedToken not recorded)", streamName);
            } else {
                log.warn("Resuming stream '{}' from lastSeenToken: "
                        + "lastProcessedToken aged out of oplog", streamName);
                FlowWardenMetrics.get().onResumeFallbackToSeen(streamName);
            }
            return new ResumeContext(seenToken);
        }

        // Level 3: both tokens unusable → apply onHistoryLost strategy
        if (processedToken != null || seenToken != null) {
            FlowWardenMetrics.get().onResumeHistoryLost(streamName);
            return handleHistoryLost(streamName, checkpointAnnotation, checkpointStore, probe,
                    tokenValidator, cp, processedToken, oldestOplogTimestamp, oplogStartAllowed,
                    reconcileHook);
        }
        // Checkpoint document exists but both tokens are null → bootstrap,
        // same as a stream with no prior checkpoint.
        return bootstrapInitialPosition(streamName, checkpointStore, probe, null);
    }

    /**
     * Bootstrap for a stream with no usable prior position: capture the
     * server's current position from the change stream's <em>initial</em>
     * reply (before any event can be returned — no cursor hand-off window),
     * persist it, and resume the main stream after it. Both the capture and
     * the persistence are startup preconditions: a failure propagates instead
     * of silently starting an unprotected, non-durable stream.
     */
    private static ResumeContext bootstrapInitialPosition(String streamName,
                                                          CheckpointStore checkpointStore,
                                                          HeartbeatProbe probe,
                                                          BsonDocument deadProcessedToken) {
        return persistBootstrap(streamName, checkpointStore, probe.initialPosition(), deadProcessedToken);
    }

    /**
     * The durable half of a bootstrap: installs {@code pbrt} as the stream's
     * position (purging the dead processed pair when recovering from a
     * history loss) and returns the resume context. Split from the capture so
     * a {@code @Reconcile} hook can run between the two.
     */
    private static ResumeContext persistBootstrap(String streamName,
                                                  CheckpointStore checkpointStore,
                                                  BsonDocument pbrt,
                                                  BsonDocument deadProcessedToken) {
        Instant now = Instant.now();
        try {
            if (deadProcessedToken != null) {
                // History-lost recovery: the history is explicitly abandoned,
                // so the dead processed pair is removed along with installing
                // the fresh position — atomically, guarded on the value read
                // at detection, preserving instanceId, metadata and any
                // fields unknown to the SPI.
                checkpointStore.resetAfterHistoryLost(streamName, pbrt, deadProcessedToken, now);
            } else {
                checkpointStore.saveSeen(streamName, pbrt, now, now);
            }
        } catch (RuntimeException e) {
            // A non-durable position cannot guarantee at-least-once: a crash
            // before the next checkpoint would silently restart from a newer
            // position. Fail the startup.
            FlowWardenMetrics.get().onCheckpointFailed(streamName, e);
            throw e;
        }
        FlowWardenMetrics.get().onCheckpoint(streamName, pbrt.toJson());
        log.info("Bootstrapped stream '{}' from an initial server-certified position", streamName);
        return new ResumeContext(pbrt);
    }

    private static ResumeContext handleHistoryLost(String streamName,
                                                   Checkpoint checkpointAnnotation,
                                                   CheckpointStore checkpointStore,
                                                   HeartbeatProbe probe,
                                                   TokenValidator tokenValidator,
                                                   io.flowwarden.stream.spi.Checkpoint deadCheckpoint,
                                                   BsonDocument deadProcessedToken,
                                                   Supplier<BsonTimestamp> oldestOplogTimestamp,
                                                   boolean oplogStartAllowed,
                                                   ReconcileHook reconcileHook) {
        OnHistoryLost strategy = checkpointAnnotation.onHistoryLost();
        Instant lastCheckpointTimestamp = DefaultHistoryLostContext.mostRecent(
                deadCheckpoint.lastProcessedTimestamp(), deadCheckpoint.lastSeenTimestamp(),
                deadCheckpoint.lastHeartbeatTimestamp());
        log.warn("Resume token expired for stream '{}' (last checkpoint: {}). Applying strategy: {}",
                streamName, lastCheckpointTimestamp, strategy);
        // With a hook, every self-repair goes through the reconciliation
        // first: the dead tokens stay in the checkpoint until the hook has
        // returned, so a failed or interrupted reconciliation is re-entered
        // on the next start instead of being forgotten behind a valid token.
        Reconciliation reconciliation = reconcileHook == null ? null
                : new Reconciliation(streamName, strategy, checkpointStore, probe, tokenValidator,
                        deadCheckpoint, deadProcessedToken, reconcileHook);

        switch (strategy) {
            case FAIL -> throw new HistoryLostException(streamName, lastCheckpointTimestamp);
            case RESUME_FROM_NOW -> {
                // This strategy explicitly accepts abandoning history, so a
                // fresh server-certified position is strictly better than an
                // implicit non-durable "from now": the checkpoint self-repairs
                // immediately and the heartbeat never consults the expired
                // token again.
                log.info("Stream '{}' will start from a fresh certified position", streamName);
                if (reconciliation != null) {
                    return reconciliation.reconcileThenBootstrap(null, 1);
                }
                return bootstrapInitialPosition(streamName, checkpointStore, probe, deadProcessedToken);
            }
            case RESUME_FROM_OPLOG_START -> {
                if (!oplogStartAllowed) {
                    // The previous attempt of this restart cycle resumed at
                    // the oldest entry and died at open with a history-lost
                    // error: the entry rolled over between the read and the
                    // open, and an oplog truncating that fast will win the
                    // race again. Degrade once instead of looping.
                    log.warn("Stream '{}': the previous attempt to resume from the oldest oplog "
                                    + "entry died at open with a history-lost error — the oplog is "
                                    + "truncating faster than the stream can open. Starting from a "
                                    + "fresh certified position instead; events since the last "
                                    + "checkpoint activity ({}) were NOT replayed",
                            streamName, lastCheckpointTimestamp);
                    if (reconciliation != null) {
                        return reconciliation.reconcileThenBootstrap(null, 1);
                    }
                    return bootstrapInitialPosition(streamName, checkpointStore, probe, deadProcessedToken);
                }
                BsonTimestamp oldestTs;
                try {
                    // The stream's own template: on multi-cluster setups the
                    // default template's oplog may be a different cluster's.
                    oldestTs = oldestOplogTimestamp.get();
                } catch (Exception e) {
                    // Same self-repair as RESUME_FROM_NOW (matching the logged
                    // fallback): a fresh certified position instead of an
                    // implicit non-durable "from now" with expired tokens
                    // lingering. A bootstrap failure propagates — no path
                    // starts on a non-durable position.
                    log.warn("Failed to read oplog for stream '{}': {}. Falling back to RESUME_FROM_NOW.",
                            streamName, e.getMessage());
                    if (reconciliation != null) {
                        return reconciliation.reconcileThenBootstrap(null, 1);
                    }
                    return bootstrapInitialPosition(streamName, checkpointStore, probe, deadProcessedToken);
                }
                // Validate the position before committing the stream to it,
                // exactly as the cascade validates tokens: the oldest entry
                // read a moment ago may already be gone on an oplog that is
                // being truncated. Only a non-resumable answer degrades; a
                // transient probe failure leaves the decision to the open.
                ProbeOutcome check = probe.probeFromOperationTime(oldestTs);
                if (check.type() == ProbeOutcome.Type.FAILED
                        && HistoryLostErrors.isHistoryLost(check.cause())) {
                    log.warn("Stream '{}': the oldest oplog entry ({}) rolled over before the stream "
                                    + "could resume from it — the oplog is truncating faster than the "
                                    + "stream can open. Starting from a fresh certified position "
                                    + "instead; events since the last checkpoint activity ({}) were "
                                    + "NOT replayed",
                            streamName, toInstant(oldestTs), lastCheckpointTimestamp);
                    if (reconciliation != null) {
                        return reconciliation.reconcileThenBootstrap(null, 1);
                    }
                    return bootstrapInitialPosition(streamName, checkpointStore, probe, deadProcessedToken);
                }
                // The dead tokens deliberately STAY in the checkpoint: they
                // are the only durable marker that a recovery is due. A crash
                // before the establishment write re-enters this recovery on
                // restart (re-replaying is at-least-once safe) instead of
                // silently bootstrapping "from now". The establishment write
                // performs the deferred cleanup, guarded by the dead
                // processed token carried in the context.
                //
                // The replay bound is the operator's reconciliation range:
                // whatever happened on the watched collection between the
                // last checkpoint activity and the oldest surviving entry is
                // gone with the oplog, and nothing here can tell whether
                // there was anything.
                log.warn("Stream '{}' resumes from the oldest oplog entry at {}: events on the watched "
                                + "collection between the last checkpoint activity ({}) and that entry, "
                                + "if any, are gone with the oplog and were NOT replayed — reconcile "
                                + "that range on the application side",
                        streamName, toInstant(oldestTs), lastCheckpointTimestamp);
                if (reconciliation != null) {
                    // The hook runs BEFORE the cursor opens on the oldest
                    // entry; the entry is then validated again, because the
                    // reconciliation may have outlived the oplog window.
                    reconciliation.runHook(DefaultHistoryLostContext.first(
                            streamName, strategy, deadCheckpoint, toInstant(oldestTs)));
                    ProbeOutcome recheck = probe.probeFromOperationTime(oldestTs);
                    if (recheck.type() == ProbeOutcome.Type.FAILED
                            && HistoryLostErrors.isHistoryLost(recheck.cause())) {
                        log.warn("Stream '{}': the oldest oplog entry ({}) rolled over while the "
                                        + "@Reconcile hook ran. Starting from a fresh certified position "
                                        + "after one more, bounded reconciliation",
                                streamName, toInstant(oldestTs));
                        return reconciliation.reconcileThenBootstrap(toInstant(oldestTs), 2);
                    }
                }
                return new ResumeContext(null, oldestTs, deadProcessedToken);
            }
        }
        throw new IllegalStateException("Unknown OnHistoryLost strategy: " + strategy);
    }

    /**
     * A level-3 recovery with a {@code @Reconcile} hook. Owns the two rules
     * the hook adds to the cascade: no durable write before the hook has
     * returned, and at most two invocations per start attempt — the second
     * one bounded to the residual window when the first outlived the oplog.
     */
    private static final class Reconciliation {
        private final String streamName;
        private final OnHistoryLost strategy;
        private final CheckpointStore checkpointStore;
        private final HeartbeatProbe probe;
        private final TokenValidator tokenValidator;
        private final io.flowwarden.stream.spi.Checkpoint deadCheckpoint;
        private final BsonDocument deadProcessedToken;
        private final ReconcileHook hook;

        Reconciliation(String streamName, OnHistoryLost strategy, CheckpointStore checkpointStore,
                       HeartbeatProbe probe, TokenValidator tokenValidator,
                       io.flowwarden.stream.spi.Checkpoint deadCheckpoint,
                       BsonDocument deadProcessedToken, ReconcileHook hook) {
            this.streamName = streamName;
            this.strategy = strategy;
            this.checkpointStore = checkpointStore;
            this.probe = probe;
            this.tokenValidator = tokenValidator;
            this.deadCheckpoint = deadCheckpoint;
            this.deadProcessedToken = deadProcessedToken;
            this.hook = hook;
        }

        /**
         * Fresh-position recovery with the hook in the middle: capture a
         * certified position (the end of the lost window), run the hook,
         * validate the position again, and only then persist it. A position
         * that rolled out during the hook gets one bounded retry; a second
         * roll-out is a clear failure — the reconciliation does not fit in
         * the oplog window.
         *
         * @param windowStart the start of the window for this invocation, or
         *                    {@code null} to derive it from the dead checkpoint
         * @param attempt     {@code 1} or {@code 2}
         */
        ResumeContext reconcileThenBootstrap(Instant windowStart, int attempt) {
            BsonDocument pbrt = probe.initialPosition();
            Instant windowEnd = Instant.now();
            DefaultHistoryLostContext ctx = windowStart == null
                    ? DefaultHistoryLostContext.first(streamName, strategy, deadCheckpoint, windowEnd)
                    : new DefaultHistoryLostContext(streamName, strategy,
                            deadCheckpoint.lastProcessedTimestamp(), deadCheckpoint.lastSeenTimestamp(),
                            deadCheckpoint.lastHeartbeatTimestamp(), windowStart, windowEnd, attempt);
            runHook(ctx);
            if (!tokenValidator.isValid(pbrt)) {
                if (attempt >= 2) {
                    ReconciliationFailedException doesNotFit = new ReconciliationFailedException(
                            streamName, ctx.lostWindowStart().orElse(null), windowEnd);
                    emitReconciliationFailed(attempt, doesNotFit);
                    log.error("Stream '{}': the reconciliation does not fit in the oplog window — the "
                                    + "position captured before the second, bounded @Reconcile call ({}) "
                                    + "rolled out as well. Grow the oplog or make the reconciliation faster",
                            streamName, windowEnd);
                    throw doesNotFit;
                }
                log.warn("Stream '{}': the certified position captured before the @Reconcile hook ({}) "
                                + "rolled out of the oplog while it ran. Starting from a fresh certified "
                                + "position after one more, bounded reconciliation",
                        streamName, windowEnd);
                return reconcileThenBootstrap(windowEnd, 2);
            }
            return persistBootstrap(streamName, checkpointStore, pbrt, deadProcessedToken);
        }

        /** Runs the hook once; emits the SPI signal after the fact; a throw stops the start. */
        void runHook(DefaultHistoryLostContext ctx) {
            log.warn("Stream '{}': invoking the @Reconcile hook before resuming (attempt {}): the "
                            + "application reconciles the lost window [{} -> {}] — nothing is delivered "
                            + "meanwhile, the expired checkpoint tokens stay until it returns",
                    streamName, ctx.attempt(), ctx.lostWindowStart().map(Object::toString).orElse("?"),
                    ctx.lostWindowEnd());
            long startedNanos = System.nanoTime();
            try {
                hook.reconcile(ctx);
            } catch (Throwable t) {
                emitReconciliationFailed(ctx.attempt(), t);
                log.error("Stream '{}': the @Reconcile hook failed (attempt {}) — the stream will not "
                                + "resume on an unreconciled view; the expired tokens are kept so the "
                                + "next start reconciles again",
                        streamName, ctx.attempt(), t);
                throw new ReconciliationFailedException(streamName,
                        ctx.lostWindowStart().orElse(null), ctx.attempt(), t);
            }
            Duration elapsed = Duration.ofNanos(System.nanoTime() - startedNanos);
            emitReconciled(ctx.attempt(), elapsed);
            log.info("Stream '{}': @Reconcile hook returned after {} ms (attempt {})",
                    streamName, elapsed.toMillis(), ctx.attempt());
        }

        /**
         * Best-effort SPI signal: a failing metrics provider must neither
         * fail a start whose reconciliation succeeded nor change what the
         * caller sees — same isolation as the restarter and the coordinator.
         */
        private void emitReconciled(int attempt, Duration elapsed) {
            try {
                FlowWardenMetrics.get().onHistoryLostReconciled(streamName, attempt, elapsed);
            } catch (Exception e) {
                log.warn("Metrics provider failed on reconciliation signal for stream '{}' (attempt {}): {}",
                        streamName, attempt, e.getMessage());
            }
        }

        /**
         * Best-effort SPI signal: the terminal {@code ReconciliationFailedException}
         * — and the hook's own cause inside it — must reach the caller even
         * when the provider throws, so the start stays on the terminal path.
         */
        private void emitReconciliationFailed(int attempt, Throwable cause) {
            try {
                FlowWardenMetrics.get().onHistoryLostReconciliationFailed(streamName, attempt, cause);
            } catch (Exception e) {
                log.warn("Metrics provider failed on reconciliation-failure signal for stream '{}' "
                        + "(attempt {}): {}", streamName, attempt, e.getMessage());
            }
        }
    }

    private static Instant toInstant(BsonTimestamp ts) {
        return Instant.ofEpochSecond(ts.getTime());
    }

}
