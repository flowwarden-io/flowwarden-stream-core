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
package io.flowwarden.stream.internal.lock;

import io.flowwarden.stream.FlowWardenMetrics;
import io.flowwarden.stream.HistoryLostException;
import io.flowwarden.stream.spi.LockService;
import io.flowwarden.stream.spi.StopReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Coordinates leader election for {@code SINGLE_LEADER} streams on top of a {@link LockService}.
 *
 * <p>Shared by both {@code ImperativeStreamManager} and {@code ReactiveStreamManager}. Manages
 * heartbeat renewal for the leader and standby polling for non-leaders, and owns the calling
 * instance's identity ({@code instanceId}) and lock TTL — the lock service itself is stateless.</p>
 */
public class LeaderElectionCoordinator {

    private static final Logger log = LoggerFactory.getLogger(LeaderElectionCoordinator.class);

    public static final Duration DEFAULT_LOCK_TTL = Duration.ofSeconds(60);

    static final long HEARTBEAT_INTERVAL_SECONDS = 15;
    static final long STANDBY_POLL_INTERVAL_SECONDS = 20;

    private final LockService lockService;
    private final String instanceId;
    private final Duration lockTtl;
    private final ScheduledExecutorService scheduler;
    private final Map<String, ScheduledFuture<?>> heartbeatTasks = new ConcurrentHashMap<>();
    private final Map<String, ScheduledFuture<?>> pollingTasks = new ConcurrentHashMap<>();
    private final Map<String, LeaderRole> roles = new ConcurrentHashMap<>();

    public LeaderElectionCoordinator(LockService lockService, String instanceId) {
        this(lockService, instanceId, DEFAULT_LOCK_TTL);
    }

    public LeaderElectionCoordinator(LockService lockService, String instanceId, Duration lockTtl) {
        this.lockService = lockService;
        this.instanceId = instanceId;
        this.lockTtl = lockTtl;
        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "fw-leader-election");
            t.setDaemon(true);
            return t;
        });
    }

    public String getInstanceId() {
        return instanceId;
    }

    /**
     * Starts the leader election process for a stream.
     *
     * <p>Attempts to acquire the lock immediately. If successful, invokes {@code onBecameLeader}
     * and starts heartbeat renewal. If not, enters standby polling mode.</p>
     *
     * @param streamName       the stream to elect a leader for
     * @param onBecameLeader   callback when this instance becomes the leader
     * @param onLostLeadership callback when this instance loses leadership
     */
    public void startElection(String streamName, Runnable onBecameLeader, Runnable onLostLeadership) {
        if (lockService.tryAcquire(streamName, instanceId, lockTtl)) {
            becomeLeader(streamName, onBecameLeader, onLostLeadership);
        } else {
            becomeStandby(streamName, onBecameLeader, onLostLeadership);
        }
    }

    /**
     * Stops election for a stream: cancels heartbeat/polling and releases the lock.
     */
    public void stop(String streamName) {
        cancelTask(heartbeatTasks, streamName);
        cancelTask(pollingTasks, streamName);
        lockService.release(streamName, instanceId);
        roles.remove(streamName);
    }

    /**
     * Graceful shutdown: release all locks held by this instance and stop the scheduler.
     */
    public void shutdown() {
        // Snapshot tracked streams before cancelling tasks
        Set<String> trackedStreams = new HashSet<>(roles.keySet());

        // Cancel all tasks
        heartbeatTasks.forEach((name, task) -> task.cancel(false));
        heartbeatTasks.clear();
        pollingTasks.forEach((name, task) -> task.cancel(false));
        pollingTasks.clear();
        roles.clear();

        // Release each lock — release is a no-op if this instance is not the owner
        int released = 0;
        for (String streamName : trackedStreams) {
            try {
                lockService.release(streamName, instanceId);
                released++;
            } catch (Exception e) {
                log.warn("Failed to release lock for stream '{}' during shutdown: {}",
                        streamName, e.getMessage());
            }
        }
        if (released > 0) {
            log.info("Attempted release of {} lock(s) during shutdown (instanceId={})",
                    released, instanceId);
        }

        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Returns the current role of this instance for a given stream.
     */
    public LeaderRole getRole(String streamName) {
        return roles.getOrDefault(streamName, LeaderRole.NOT_APPLICABLE);
    }

    private void becomeLeader(String streamName, Runnable onBecameLeader, Runnable onLostLeadership) {
        roles.put(streamName, LeaderRole.LEADER);
        log.info("Instance became LEADER for stream '{}' (instanceId={})", streamName, instanceId);
        emitLeadershipChange(streamName, "LEADER");

        try {
            onBecameLeader.run();
        } catch (HistoryLostException e) {
            // Terminal: the next attempt fails identically (the checkpoint is
            // the problem), and handing over to a standby is pointless — it
            // would fail on the same checkpoint. Stop the election entirely
            // and surface the crash; an operator action is required.
            log.error("Leader callback failed terminally for stream '{}' — stopping its "
                    + "election (no retry, no standby handover): {}",
                    streamName, e.getMessage(), e);
            cancelTask(heartbeatTasks, streamName);
            cancelTask(pollingTasks, streamName);
            // Compensation BEFORE the release: a throwing callback may have
            // partially started the stream (a registered container, an
            // installed subscription); releasing the lease first would let a
            // standby become leader while this consumer is still active.
            invokeLostLeadership(streamName, onLostLeadership);
            releaseQuietly(streamName);
            roles.remove(streamName);
            // Emitted LAST: the compensation is typically stopStream, which
            // publishes its own GRACEFUL stop — the terminal status must be
            // the final word for last-status-wins consumers, and a throwing
            // provider must not short-circuit the cleanup above.
            emitStreamCrashed(streamName, e);
            return;
        } catch (RuntimeException e) {
            // Transient (network error during the resume cascade, template
            // resolution failure, …): hand the lease back and re-enter
            // standby polling — the poll retries the whole election.
            log.warn("Leader callback failed for stream '{}' — releasing the lock and "
                    + "re-entering standby (retry in {}s): {}",
                    streamName, STANDBY_POLL_INTERVAL_SECONDS, e.getMessage(), e);
            // Same compensation-before-release ordering as the terminal
            // branch — a partial start must be undone before another
            // instance can acquire the lease.
            invokeLostLeadership(streamName, onLostLeadership);
            releaseQuietly(streamName);
            becomeStandby(streamName, onBecameLeader, onLostLeadership);
            return;
        }

        // Schedule heartbeat renewal — only after the callback returned
        // normally: a leader whose startup failed must never renew the lease.
        ScheduledFuture<?> heartbeat = scheduler.scheduleAtFixedRate(
                () -> heartbeatTick(streamName, onBecameLeader, onLostLeadership),
                HEARTBEAT_INTERVAL_SECONDS, HEARTBEAT_INTERVAL_SECONDS, TimeUnit.SECONDS);

        heartbeatTasks.put(streamName, heartbeat);
    }

    /**
     * Best-effort metrics emission: a {@link io.flowwarden.stream.spi.StreamMetricsProvider}
     * is external SPI code and must stay observational — its failure must
     * never drive (or interrupt) the election state machine.
     */
    private void emitLeadershipChange(String streamName, String role) {
        try {
            FlowWardenMetrics.get().onLeadershipChange(streamName, role, instanceId);
        } catch (Exception e) {
            log.warn("Metrics provider failed on leadership change ({} → {}): {}",
                    streamName, role, e.getMessage());
        }
    }

    /** Best-effort crash signal — same isolation rationale as {@link #emitLeadershipChange}. */
    private void emitStreamCrashed(String streamName, Throwable cause) {
        try {
            FlowWardenMetrics.get().onStreamStopped(streamName, StopReason.CRASHED, cause);
        } catch (Exception e) {
            log.warn("Metrics provider failed on crash signal for stream '{}': {}",
                    streamName, e.getMessage());
        }
    }

    private void releaseQuietly(String streamName) {
        try {
            lockService.release(streamName, instanceId);
        } catch (Exception e) {
            // Best-effort: the lease expires on its own TTL, after which a
            // standby (or this instance's poll) can acquire it normally.
            log.warn("Failed to release lock for stream '{}': {}", streamName, e.getMessage());
        }
    }

    /**
     * A single heartbeat tick: renew the lock; on {@code false} or on a propagated exception
     * trigger the fail-stop path. Package-private so it can be exercised in unit tests without
     * waiting for the scheduler to fire.
     */
    void heartbeatTick(String streamName, Runnable onBecameLeader, Runnable onLostLeadership) {
        try {
            if (!lockService.renew(streamName, instanceId, lockTtl)) {
                handleLeadershipLoss(streamName, onBecameLeader, onLostLeadership,
                        "lock renewal returned false");
            }
        } catch (Exception e) {
            // Defensive: a conformant LockService should convert transient errors
            // into a false return (see LockService Javadoc). Treat a propagated
            // exception identically to renew() == false so a non-conformant backend
            // can't strand us in a double-leader state — the underlying lease will
            // expire and another instance may legitimately take over.
            log.warn("Lock renewal for stream '{}' threw — treating as leadership loss: {}",
                    streamName, e.getMessage(), e);
            handleLeadershipLoss(streamName, onBecameLeader, onLostLeadership,
                    "lock renewal threw " + e.getClass().getSimpleName());
        }
    }

    /**
     * Common fail-stop path for the heartbeat loop: cancel the heartbeat, transition the role
     * to {@code STANDBY}, invoke the user's {@code onLostLeadership} callback (best-effort),
     * and re-enter standby polling so the instance can become leader again later. Called from
     * both the {@code renew() == false} branch and the defensive {@code renew()} throw branch.
     */
    private void handleLeadershipLoss(String streamName,
                                       Runnable onBecameLeader,
                                       Runnable onLostLeadership,
                                       String reason) {
        log.warn("Lost leadership for stream '{}' — {}", streamName, reason);
        cancelTask(heartbeatTasks, streamName);
        roles.put(streamName, LeaderRole.STANDBY);

        invokeLostLeadership(streamName, onLostLeadership);
        becomeStandby(streamName, onBecameLeader, onLostLeadership);
    }

    /**
     * Runs the user's {@code onLostLeadership} callback, best-effort: its
     * failure must never prevent the role transition, the lock release or the
     * standby re-entry of the caller. Used on genuine leadership loss and as
     * the compensation for a failed {@code onBecameLeader} (which may have
     * partially started the stream before throwing).
     */
    private void invokeLostLeadership(String streamName, Runnable onLostLeadership) {
        try {
            onLostLeadership.run();
        } catch (Exception e) {
            log.error("Error during leadership loss handling for stream '{}': {}",
                    streamName, e.getMessage());
        }
    }

    private void becomeStandby(String streamName, Runnable onBecameLeader, Runnable onLostLeadership) {
        roles.put(streamName, LeaderRole.STANDBY);
        log.info("Instance is STANDBY for stream '{}' — polling for leadership (instanceId={})",
                streamName, instanceId);
        emitLeadershipChange(streamName, "STANDBY");

        // Schedule standby polling
        ScheduledFuture<?> polling = scheduler.scheduleAtFixedRate(
                () -> standbyPollTick(streamName, onBecameLeader, onLostLeadership),
                STANDBY_POLL_INTERVAL_SECONDS, STANDBY_POLL_INTERVAL_SECONDS, TimeUnit.SECONDS);

        pollingTasks.put(streamName, polling);
    }

    /**
     * A single standby poll: try to acquire the lock and, on success, become
     * the leader. Callback failures are classified and contained by
     * {@code becomeLeader} itself and never reach the catch below — it
     * remains as the safety net for {@code tryAcquire} errors (a conformant
     * {@link LockService} returns {@code false} instead of throwing) and for
     * unexpected internal transition errors. Package-private so it can be
     * exercised in unit tests without waiting for the scheduler.
     */
    void standbyPollTick(String streamName, Runnable onBecameLeader, Runnable onLostLeadership) {
        try {
            if (lockService.tryAcquire(streamName, instanceId, lockTtl)) {
                log.info("Standby acquired leadership for stream '{}'", streamName);
                cancelTask(pollingTasks, streamName);
                becomeLeader(streamName, onBecameLeader, onLostLeadership);
            }
        } catch (Exception e) {
            log.debug("Standby poll failed for stream '{}': {}", streamName, e.getMessage());
        }
    }

    /** Test hook: whether a heartbeat renewal task is registered for the stream. */
    boolean hasHeartbeatTask(String streamName) {
        return heartbeatTasks.containsKey(streamName);
    }

    /** Test hook: whether a standby polling task is registered for the stream. */
    boolean hasPollingTask(String streamName) {
        return pollingTasks.containsKey(streamName);
    }

    private void cancelTask(Map<String, ScheduledFuture<?>> tasks, String streamName) {
        ScheduledFuture<?> task = tasks.remove(streamName);
        if (task != null) {
            task.cancel(false);
        }
    }

    /**
     * Role of this instance for a given stream.
     */
    public enum LeaderRole {
        LEADER,
        STANDBY,
        NOT_APPLICABLE
    }
}
