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
import io.flowwarden.stream.HistoryLostException;
import io.flowwarden.stream.OnHistoryLost;
import io.flowwarden.stream.ResumeStrategy;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.spi.CheckpointStore;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * The resume cascade, shared by both stream managers and by every entry
 * point that (re-)establishes a stream position — boot and runtime restarts
 * alike: try the primary token chosen by {@link ResumeStrategy} (level 1),
 * fall back to the secondary if the primary has aged out (level 2), apply
 * the {@link OnHistoryLost} strategy if both have aged out (level 3), and
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

    private ResumeCascade() {
    }

    /**
     * Resolves the resume position for a stream.
     *
     * @param streamName            the stream identifier
     * @param checkpointAnnotation  the stream's {@code @Checkpoint}
     *                              (resume strategy + history-lost strategy)
     * @param checkpointStore       the checkpoint store
     * @param probe                 the stream's heartbeat probe — used for
     *                              {@link HeartbeatProbe#initialPosition()}
     *                              during bootstraps
     * @param tokenValidator        template-bound resume-token validation
     * @param oldestOplogTimestamp  reads the oldest entry of the stream's own
     *                              cluster's oplog; may throw — the failure
     *                              falls back to a RESUME_FROM_NOW bootstrap
     * @return the immutable resume outcome
     * @throws HistoryLostException under {@code OnHistoryLost.FAIL} when both
     *                              tokens have aged out
     */
    public static ResumeContext resolve(String streamName,
                                        Checkpoint checkpointAnnotation,
                                        CheckpointStore checkpointStore,
                                        HeartbeatProbe probe,
                                        TokenValidator tokenValidator,
                                        Supplier<BsonTimestamp> oldestOplogTimestamp) {
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
        ResumeStrategy strategy = checkpointAnnotation.resumeStrategy();

        BsonDocument primary;
        BsonDocument secondary;
        String primaryLabel;
        String secondaryLabel;
        Runnable onFallback;
        switch (strategy) {
            case SEEN_FIRST -> {
                primary = seenToken;
                secondary = processedToken;
                primaryLabel = "lastSeenToken";
                secondaryLabel = "lastProcessedToken";
                onFallback = () -> FlowWardenMetrics.get().onResumeFallbackToProcessed(streamName);
            }
            case PROCESSED_FIRST -> {
                primary = processedToken;
                secondary = seenToken;
                primaryLabel = "lastProcessedToken";
                secondaryLabel = "lastSeenToken";
                onFallback = () -> FlowWardenMetrics.get().onResumeFallbackToSeen(streamName);
            }
            default -> throw new IllegalStateException("Unknown ResumeStrategy: " + strategy);
        }

        // Level 1: try the primary token
        if (primary != null && tokenValidator.isValid(primary)) {
            log.info("Resuming stream '{}' from {}", streamName, primaryLabel);
            return new ResumeContext(primary, seenToken);
        }

        // Level 2: the secondary, validated exactly once. With a null primary
        // (never recorded — typical after a history-lost self-repair) this is
        // not a degradation: INFO, no "aged out" warning, no fallback metric.
        if (secondary != null
                && (primary == null || !secondary.equals(primary))
                && tokenValidator.isValid(secondary)) {
            if (primary == null) {
                log.info("Resuming stream '{}' from {} ({} not recorded)",
                        streamName, secondaryLabel, primaryLabel);
            } else {
                log.warn("Resuming stream '{}' from {}: {} aged out of oplog",
                        streamName, secondaryLabel, primaryLabel);
                onFallback.run();
            }
            return new ResumeContext(secondary, seenToken);
        }

        // Level 3: both tokens unusable → apply onHistoryLost strategy
        if (processedToken != null || seenToken != null) {
            FlowWardenMetrics.get().onResumeHistoryLost(streamName);
            return handleHistoryLost(streamName, checkpointAnnotation, checkpointStore, probe,
                    mostRecent(cp.lastProcessedTimestamp(), cp.lastSeenTimestamp(),
                            cp.lastHeartbeatTimestamp()),
                    processedToken, oldestOplogTimestamp);
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
        BsonDocument pbrt = probe.initialPosition();
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
        // seed == freshly persisted seen: never a phantom catch-up.
        return new ResumeContext(pbrt, pbrt);
    }

    private static ResumeContext handleHistoryLost(String streamName,
                                                   Checkpoint checkpointAnnotation,
                                                   CheckpointStore checkpointStore,
                                                   HeartbeatProbe probe,
                                                   Instant lastCheckpointTimestamp,
                                                   BsonDocument deadProcessedToken,
                                                   Supplier<BsonTimestamp> oldestOplogTimestamp) {
        OnHistoryLost strategy = checkpointAnnotation.onHistoryLost();
        log.warn("Resume token expired for stream '{}' (last checkpoint: {}). Applying strategy: {}",
                streamName, lastCheckpointTimestamp, strategy);

        switch (strategy) {
            case FAIL -> throw new HistoryLostException(streamName, lastCheckpointTimestamp);
            case RESUME_FROM_NOW -> {
                // This strategy explicitly accepts abandoning history, so a
                // fresh server-certified position is strictly better than an
                // implicit non-durable "from now": the checkpoint self-repairs
                // immediately and the heartbeat never consults the expired
                // token again.
                log.info("Stream '{}' will start from a fresh certified position", streamName);
                return bootstrapInitialPosition(streamName, checkpointStore, probe, deadProcessedToken);
            }
            case RESUME_FROM_OPLOG_START -> {
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
                    return bootstrapInitialPosition(streamName, checkpointStore, probe, deadProcessedToken);
                }
                // The dead tokens deliberately STAY in the checkpoint: they
                // are the only durable marker that a recovery is due. A crash
                // before the establishment write re-enters this recovery on
                // restart (re-replaying is at-least-once safe) instead of
                // silently bootstrapping "from now". The establishment write
                // performs the deferred cleanup, guarded by the dead
                // processed token carried in the context.
                log.info("Stream '{}' will resume from oldest oplog entry at {}", streamName, oldestTs);
                return new ResumeContext(null, null, oldestTs, deadProcessedToken);
            }
        }
        throw new IllegalStateException("Unknown OnHistoryLost strategy: " + strategy);
    }

    private static Instant mostRecent(Instant... candidates) {
        Instant best = null;
        for (Instant candidate : candidates) {
            if (candidate != null && (best == null || candidate.isAfter(best))) {
                best = candidate;
            }
        }
        return best;
    }
}
