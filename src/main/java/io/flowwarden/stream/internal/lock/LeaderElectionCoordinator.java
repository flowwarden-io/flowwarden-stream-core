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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Coordinates leader election for SINGLE_LEADER streams (ARCH-025).
 *
 * <p>Shared by both {@code ImperativeStreamManager} and {@code ReactiveStreamManager}.
 * Manages heartbeat renewal and standby polling for each stream.</p>
 */
public class LeaderElectionCoordinator {

    private static final Logger log = LoggerFactory.getLogger(LeaderElectionCoordinator.class);

    static final long HEARTBEAT_INTERVAL_SECONDS = 15;
    static final long STANDBY_POLL_INTERVAL_SECONDS = 20;

    private final MongoLockService lockService;
    private final ScheduledExecutorService scheduler;
    private final Map<String, ScheduledFuture<?>> heartbeatTasks = new ConcurrentHashMap<>();
    private final Map<String, ScheduledFuture<?>> pollingTasks = new ConcurrentHashMap<>();
    private final Map<String, LeaderRole> roles = new ConcurrentHashMap<>();

    public LeaderElectionCoordinator(MongoLockService lockService) {
        this.lockService = lockService;
        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "fw-leader-election");
            t.setDaemon(true);
            return t;
        });
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
        if (lockService.tryAcquire(streamName)) {
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
        lockService.release(streamName);
        roles.remove(streamName);
    }

    /**
     * Graceful shutdown: release all locks and stop the scheduler.
     */
    public void shutdown() {
        // Cancel all tasks
        heartbeatTasks.forEach((name, task) -> task.cancel(false));
        heartbeatTasks.clear();
        pollingTasks.forEach((name, task) -> task.cancel(false));
        pollingTasks.clear();
        roles.clear();

        lockService.releaseAll();

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
        log.info("Instance became LEADER for stream '{}' (instanceId={})",
                streamName, lockService.getInstanceId());
        FlowWardenMetrics.get().onLeadershipChange(streamName, "LEADER", lockService.getInstanceId());

        onBecameLeader.run();

        // Schedule heartbeat renewal
        ScheduledFuture<?> heartbeat = scheduler.scheduleAtFixedRate(() -> {
            try {
                if (!lockService.renew(streamName)) {
                    log.warn("Lost leadership for stream '{}' — lock renewal failed", streamName);
                    cancelTask(heartbeatTasks, streamName);
                    roles.put(streamName, LeaderRole.STANDBY);

                    // Stop the stream and re-enter standby polling
                    try {
                        onLostLeadership.run();
                    } catch (Exception e) {
                        log.error("Error during leadership loss handling for stream '{}': {}",
                                streamName, e.getMessage());
                    }
                    becomeStandby(streamName, onBecameLeader, onLostLeadership);
                }
            } catch (Exception e) {
                log.warn("Error during lock heartbeat for stream '{}': {}", streamName, e.getMessage());
            }
        }, HEARTBEAT_INTERVAL_SECONDS, HEARTBEAT_INTERVAL_SECONDS, TimeUnit.SECONDS);

        heartbeatTasks.put(streamName, heartbeat);
    }

    private void becomeStandby(String streamName, Runnable onBecameLeader, Runnable onLostLeadership) {
        roles.put(streamName, LeaderRole.STANDBY);
        log.info("Instance is STANDBY for stream '{}' — polling for leadership (instanceId={})",
                streamName, lockService.getInstanceId());
        FlowWardenMetrics.get().onLeadershipChange(streamName, "STANDBY", lockService.getInstanceId());

        // Schedule standby polling
        ScheduledFuture<?> polling = scheduler.scheduleAtFixedRate(() -> {
            try {
                if (lockService.tryAcquire(streamName)) {
                    log.info("Standby acquired leadership for stream '{}'", streamName);
                    cancelTask(pollingTasks, streamName);
                    becomeLeader(streamName, onBecameLeader, onLostLeadership);
                }
            } catch (Exception e) {
                log.debug("Standby poll failed for stream '{}': {}", streamName, e.getMessage());
            }
        }, STANDBY_POLL_INTERVAL_SECONDS, STANDBY_POLL_INTERVAL_SECONDS, TimeUnit.SECONDS);

        pollingTasks.put(streamName, polling);
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
