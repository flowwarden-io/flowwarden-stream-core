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
import io.flowwarden.stream.spi.StreamMetricsProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.mockito.InOrder;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LeaderElectionCoordinatorTest {

    private static final String STREAM = "stream-x";
    private static final String INSTANCE = "test-instance";

    private LockService lockService;
    private LeaderElectionCoordinator coordinator;
    private Runnable onBecameLeader;
    private Runnable onLostLeadership;
    private RecordingMetrics metrics;

    @BeforeEach
    void setUp() {
        lockService = mock(LockService.class);
        coordinator = new LeaderElectionCoordinator(lockService, INSTANCE, Duration.ofSeconds(60));
        onBecameLeader = mock(Runnable.class);
        onLostLeadership = mock(Runnable.class);
        metrics = new RecordingMetrics();
        FlowWardenMetrics.setProvider(metrics);
    }

    @AfterEach
    void tearDown() {
        coordinator.shutdown();
        FlowWardenMetrics.setProvider(StreamMetricsProvider.noOp());
    }

    @Test
    void heartbeatTick_renewReturnsTrue_keepsLeadership() {
        becomeLeader();

        when(lockService.renew(eq(STREAM), eq(INSTANCE), any())).thenReturn(true);

        coordinator.heartbeatTick(STREAM, onBecameLeader, onLostLeadership);

        verify(onLostLeadership, never()).run();
        assertThat(coordinator.getRole(STREAM))
                .isEqualTo(LeaderElectionCoordinator.LeaderRole.LEADER);
    }

    @Test
    void heartbeatTick_renewReturnsFalse_triggersLeadershipLoss() {
        becomeLeader();

        when(lockService.renew(eq(STREAM), eq(INSTANCE), any())).thenReturn(false);

        coordinator.heartbeatTick(STREAM, onBecameLeader, onLostLeadership);

        verify(onLostLeadership).run();
        assertThat(coordinator.getRole(STREAM))
                .isEqualTo(LeaderElectionCoordinator.LeaderRole.STANDBY);
    }

    @Test
    void heartbeatTick_renewThrows_triggersLeadershipLossDefensively() {
        becomeLeader();

        when(lockService.renew(eq(STREAM), eq(INSTANCE), any()))
                .thenThrow(new RuntimeException("simulated network blip"));

        coordinator.heartbeatTick(STREAM, onBecameLeader, onLostLeadership);

        // Before the #39 fix this assertion failed: onLostLeadership stayed un-invoked
        // and the role remained LEADER, leaving the instance convinced it owned the lock
        // even though the underlying lease was about to expire.
        verify(onLostLeadership).run();
        assertThat(coordinator.getRole(STREAM))
                .isEqualTo(LeaderElectionCoordinator.LeaderRole.STANDBY);
    }

    @Test
    void heartbeatTick_onLostLeadershipCallbackThrows_stillTransitionsToStandby() {
        becomeLeader();

        when(lockService.renew(eq(STREAM), eq(INSTANCE), any())).thenReturn(false);
        Runnable throwingCallback = () -> {
            throw new RuntimeException("user callback boom");
        };

        // Must not bubble out — leadership loss handling is best-effort
        coordinator.heartbeatTick(STREAM, onBecameLeader, throwingCallback);

        assertThat(coordinator.getRole(STREAM))
                .isEqualTo(LeaderElectionCoordinator.LeaderRole.STANDBY);
    }

    // --- #59: onBecameLeader failures must not corrupt the state machine ---

    @Test
    void startElection_callbackThrowsHistoryLost_stopsElectionAndSurfacesCrash() {
        when(lockService.tryAcquire(eq(STREAM), eq(INSTANCE), any())).thenReturn(true);
        HistoryLostException terminal = new HistoryLostException(STREAM, Instant.now());
        Runnable failingStart = () -> {
            throw terminal;
        };

        // Must not bubble out: on the boot path this would fail Spring startup
        // and orchestrators would crash-loop on an error that never heals.
        coordinator.startElection(STREAM, failingStart, onLostLeadership);

        assertThat(coordinator.getRole(STREAM))
                .as("a terminally failed leader stops its election entirely")
                .isEqualTo(LeaderElectionCoordinator.LeaderRole.NOT_APPLICABLE);
        assertThat(coordinator.hasHeartbeatTask(STREAM)).isFalse();
        assertThat(coordinator.hasPollingTask(STREAM)).isFalse();
        verify(lockService).release(STREAM, INSTANCE);
        // A failed start may be partial — the compensation must run.
        verify(onLostLeadership).run();
        assertThat(metrics.stops)
                .as("the console must see the crash")
                .containsExactly(STREAM + ":CRASHED:" + terminal.getClass().getSimpleName());
        assertThat(metrics.leadershipChanges)
                .as("LEADER then a crash stop — never a standby re-entry")
                .containsExactly(STREAM + ":LEADER");
    }

    @Test
    void startElection_callbackThrowsTransient_releasesLockAndReentersStandby() {
        when(lockService.tryAcquire(eq(STREAM), eq(INSTANCE), any())).thenReturn(true);
        AtomicInteger attempts = new AtomicInteger();
        Runnable failingOnce = () -> {
            if (attempts.incrementAndGet() == 1) {
                throw new RuntimeException("transient cascade failure");
            }
        };

        coordinator.startElection(STREAM, failingOnce, onLostLeadership);

        assertThat(coordinator.getRole(STREAM))
                .isEqualTo(LeaderElectionCoordinator.LeaderRole.STANDBY);
        assertThat(coordinator.hasHeartbeatTask(STREAM)).isFalse();
        assertThat(coordinator.hasPollingTask(STREAM))
                .as("the standby poll is the retry loop")
                .isTrue();
        verify(lockService).release(STREAM, INSTANCE);
        // A failed start may be partial — the compensation must run.
        verify(onLostLeadership).run();
        assertThat(metrics.stops).as("a transient failure is not a crash").isEmpty();

        // Next successful poll becomes leader normally.
        coordinator.standbyPollTick(STREAM, failingOnce, onLostLeadership);
        assertThat(coordinator.getRole(STREAM))
                .isEqualTo(LeaderElectionCoordinator.LeaderRole.LEADER);
        assertThat(coordinator.hasHeartbeatTask(STREAM)).isTrue();
        assertThat(coordinator.hasPollingTask(STREAM)).isFalse();
        assertThat(attempts.get()).isEqualTo(2);
        assertThat(metrics.leadershipChanges)
                .as("full observable sequence: failed leadership, standby retry, leadership")
                .containsExactly(STREAM + ":LEADER", STREAM + ":STANDBY", STREAM + ":LEADER");
    }

    @Test
    void becomeLeaderTerminalFailure_compensationUndoesPartialStart_beforeLockRelease() {
        when(lockService.tryAcquire(eq(STREAM), eq(INSTANCE), any())).thenReturn(true);
        AtomicBoolean resourceStarted = new AtomicBoolean(false);
        Runnable partialStart = () -> {
            resourceStarted.set(true); // the container registered, then…
            throw new HistoryLostException(STREAM, Instant.now());
        };
        doAnswer(inv -> {
            resourceStarted.set(false);
            return null;
        }).when(onLostLeadership).run();

        coordinator.startElection(STREAM, partialStart, onLostLeadership);

        assertThat(resourceStarted.get())
                .as("the partially started consumer must be stopped")
                .isFalse();
        // Compensation strictly BEFORE the release: no standby may become
        // leader while the old consumer is still active.
        InOrder order = inOrder(onLostLeadership, lockService);
        order.verify(onLostLeadership).run();
        order.verify(lockService).release(STREAM, INSTANCE);
    }

    @Test
    void becomeLeaderTransientFailure_compensationUndoesPartialStart_beforeLockRelease() {
        when(lockService.tryAcquire(eq(STREAM), eq(INSTANCE), any())).thenReturn(true);
        AtomicBoolean resourceStarted = new AtomicBoolean(false);
        Runnable partialStart = () -> {
            resourceStarted.set(true);
            throw new RuntimeException("transient failure after partial start");
        };
        doAnswer(inv -> {
            resourceStarted.set(false);
            return null;
        }).when(onLostLeadership).run();

        coordinator.startElection(STREAM, partialStart, onLostLeadership);

        assertThat(resourceStarted.get()).isFalse();
        InOrder order = inOrder(onLostLeadership, lockService);
        order.verify(onLostLeadership).run();
        order.verify(lockService).release(STREAM, INSTANCE);
        assertThat(coordinator.getRole(STREAM))
                .isEqualTo(LeaderElectionCoordinator.LeaderRole.STANDBY);
    }

    // --- #59 round 2: metrics stay observational, CRASHED is the last word ---

    @Test
    void providerThrowsOnLeaderEmit_callbackStillRuns_heartbeatStillInstalled() {
        metrics.throwOnRole = "LEADER";
        when(lockService.tryAcquire(eq(STREAM), eq(INSTANCE), any())).thenReturn(true);

        coordinator.startElection(STREAM, onBecameLeader, onLostLeadership);

        verify(onBecameLeader).run();
        assertThat(coordinator.getRole(STREAM))
                .isEqualTo(LeaderElectionCoordinator.LeaderRole.LEADER);
        assertThat(coordinator.hasHeartbeatTask(STREAM))
                .as("a throwing metrics provider must never drive the state machine")
                .isTrue();
    }

    @Test
    void providerThrowsOnStandbyEmit_afterTransientFailure_pollStillInstalled() {
        metrics.throwOnRole = "STANDBY";
        when(lockService.tryAcquire(eq(STREAM), eq(INSTANCE), any())).thenReturn(true);
        Runnable failingStart = () -> {
            throw new RuntimeException("transient");
        };

        coordinator.startElection(STREAM, failingStart, onLostLeadership);

        assertThat(coordinator.getRole(STREAM))
                .isEqualTo(LeaderElectionCoordinator.LeaderRole.STANDBY);
        assertThat(coordinator.hasPollingTask(STREAM))
                .as("recovery must not be silently disabled by a metrics failure")
                .isTrue();
        verify(lockService).release(STREAM, INSTANCE);
    }

    @Test
    void providerThrowsOnCrashedEmit_cleanupCompletes_nothingPropagates() {
        metrics.throwOnStops = true;
        when(lockService.tryAcquire(eq(STREAM), eq(INSTANCE), any())).thenReturn(true);
        Runnable failingStart = () -> {
            throw new HistoryLostException(STREAM, Instant.now());
        };

        // Must not bubble out of startElection.
        coordinator.startElection(STREAM, failingStart, onLostLeadership);

        verify(onLostLeadership).run();
        verify(lockService).release(STREAM, INSTANCE);
        assertThat(coordinator.getRole(STREAM))
                .isEqualTo(LeaderElectionCoordinator.LeaderRole.NOT_APPLICABLE);
        assertThat(coordinator.hasHeartbeatTask(STREAM)).isFalse();
        assertThat(coordinator.hasPollingTask(STREAM)).isFalse();
    }

    @Test
    void terminalCrashSignal_isEmittedAfterTheCompensationsOwnGracefulStop() {
        when(lockService.tryAcquire(eq(STREAM), eq(INSTANCE), any())).thenReturn(true);
        HistoryLostException terminal = new HistoryLostException(STREAM, Instant.now());
        Runnable failingStart = () -> {
            throw terminal;
        };
        // The real compensation is stopStream, which publishes its own
        // GRACEFUL stop (directly in the imperative manager, via doFinally in
        // the reactive one).
        doAnswer(inv -> {
            FlowWardenMetrics.get().onStreamStopped(STREAM, StopReason.GRACEFUL, null);
            return null;
        }).when(onLostLeadership).run();

        coordinator.startElection(STREAM, failingStart, onLostLeadership);

        assertThat(metrics.stops)
                .as("the terminal status must be the last word for "
                        + "last-status-wins consumers")
                .containsExactly(
                        STREAM + ":GRACEFUL:null",
                        STREAM + ":CRASHED:" + terminal.getClass().getSimpleName());
    }

    @Test
    void becomeLeaderFailure_compensationThrows_stateMachineAndReleaseUnaffected() {
        when(lockService.tryAcquire(eq(STREAM), eq(INSTANCE), any())).thenReturn(true);
        HistoryLostException terminal = new HistoryLostException(STREAM, Instant.now());
        Runnable failingStart = () -> {
            throw terminal;
        };
        doThrow(new RuntimeException("compensation boom")).when(onLostLeadership).run();

        coordinator.startElection(STREAM, failingStart, onLostLeadership);

        assertThat(coordinator.getRole(STREAM))
                .as("a throwing compensation must not brick the state machine")
                .isEqualTo(LeaderElectionCoordinator.LeaderRole.NOT_APPLICABLE);
        verify(lockService).release(STREAM, INSTANCE);
        assertThat(metrics.stops)
                .containsExactly(STREAM + ":CRASHED:" + terminal.getClass().getSimpleName());
    }

    @Test
    void standbyPollTick_callbackThrowsHistoryLost_stopsElectionInsteadOfBrickingSilently() {
        // Start in standby (lock held elsewhere), then the holder disappears.
        when(lockService.tryAcquire(eq(STREAM), eq(INSTANCE), any())).thenReturn(false);
        HistoryLostException terminal = new HistoryLostException(STREAM, Instant.now());
        Runnable failingStart = () -> {
            throw terminal;
        };
        coordinator.startElection(STREAM, failingStart, onLostLeadership);
        assertThat(coordinator.getRole(STREAM))
                .isEqualTo(LeaderElectionCoordinator.LeaderRole.STANDBY);

        when(lockService.tryAcquire(eq(STREAM), eq(INSTANCE), any())).thenReturn(true);
        coordinator.standbyPollTick(STREAM, failingStart, onLostLeadership);

        // Before the fix: role LEADER, lock held and expiring, no heartbeat,
        // no polling — silently bricked (the poll's catch logged at debug).
        assertThat(coordinator.getRole(STREAM))
                .isEqualTo(LeaderElectionCoordinator.LeaderRole.NOT_APPLICABLE);
        assertThat(coordinator.hasHeartbeatTask(STREAM)).isFalse();
        assertThat(coordinator.hasPollingTask(STREAM)).isFalse();
        verify(lockService).release(STREAM, INSTANCE);
        assertThat(metrics.stops)
                .containsExactly(STREAM + ":CRASHED:" + terminal.getClass().getSimpleName());
    }

    @Test
    void standbyPollTick_callbackThrowsTransient_resumesPollingAndRecovers() {
        when(lockService.tryAcquire(eq(STREAM), eq(INSTANCE), any())).thenReturn(false);
        AtomicInteger attempts = new AtomicInteger();
        Runnable failingOnce = () -> {
            if (attempts.incrementAndGet() == 1) {
                throw new RuntimeException("transient cascade failure");
            }
        };
        coordinator.startElection(STREAM, failingOnce, onLostLeadership);

        when(lockService.tryAcquire(eq(STREAM), eq(INSTANCE), any())).thenReturn(true);
        coordinator.standbyPollTick(STREAM, failingOnce, onLostLeadership);

        assertThat(coordinator.getRole(STREAM))
                .isEqualTo(LeaderElectionCoordinator.LeaderRole.STANDBY);
        assertThat(coordinator.hasPollingTask(STREAM)).isTrue();
        verify(lockService).release(STREAM, INSTANCE);

        coordinator.standbyPollTick(STREAM, failingOnce, onLostLeadership);
        assertThat(coordinator.getRole(STREAM))
                .isEqualTo(LeaderElectionCoordinator.LeaderRole.LEADER);
        assertThat(coordinator.hasHeartbeatTask(STREAM)).isTrue();
        assertThat(attempts.get()).isEqualTo(2);
    }

    /**
     * Drives the coordinator into the LEADER state via {@code startElection}. The mocked
     * {@code tryAcquire} returns {@code true} so {@code becomeLeader} fires synchronously
     * before {@code heartbeatTick} is invoked manually by each test.
     */
    private void becomeLeader() {
        when(lockService.tryAcquire(eq(STREAM), eq(INSTANCE), any())).thenReturn(true);
        coordinator.startElection(STREAM, onBecameLeader, onLostLeadership);
        verify(onBecameLeader).run();
        assertThat(coordinator.getRole(STREAM))
                .isEqualTo(LeaderElectionCoordinator.LeaderRole.LEADER);
    }

    private static final class RecordingMetrics implements StreamMetricsProvider {
        final List<String> stops = new CopyOnWriteArrayList<>();
        final List<String> leadershipChanges = new CopyOnWriteArrayList<>();
        /** When set, {@code onLeadershipChange} throws for that role. */
        volatile String throwOnRole;
        /** When set, {@code onStreamStopped} throws. */
        volatile boolean throwOnStops;

        @Override
        public void onStreamStopped(String streamName, StopReason reason, Throwable cause) {
            stops.add(streamName + ":" + reason + ":"
                    + (cause != null ? cause.getClass().getSimpleName() : "null"));
            if (throwOnStops) {
                throw new RuntimeException("provider boom on stop");
            }
        }

        @Override
        public void onLeadershipChange(String streamName, String role, String instanceId) {
            leadershipChanges.add(streamName + ":" + role);
            if (role.equals(throwOnRole)) {
                throw new RuntimeException("provider boom on " + role);
            }
        }

        @Override
        public void onStreamStarted(String streamName,
                io.flowwarden.stream.spi.StreamConfiguration config) {
        }

        @Override
        public void onEventReceived(String streamName,
                io.flowwarden.stream.spi.ChangeEventMetadata metadata) {
        }

        @Override
        public void onEventProcessed(String streamName, long durationNanos, boolean success) {
        }

        @Override
        public void onEventError(String streamName, Throwable error, boolean willRetry,
                int attemptNumber, io.flowwarden.stream.spi.ChangeEventMetadata metadata) {
        }

        @Override
        public void onCheckpoint(String streamName, String resumeToken) {
        }

        @Override
        public void onBufferStatus(String streamName, int currentSize, int maxSize) {
        }

        @Override
        public void onBackpressure(String streamName,
                io.flowwarden.stream.spi.BackpressureAction action) {
        }

        @Override
        public void onEventSentToDlq(String streamName) {
        }

        @Override
        public void onOplogStats(double logLengthHours, String status) {
        }
    }
}
