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

import io.flowwarden.stream.spi.LockService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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

    @BeforeEach
    void setUp() {
        lockService = mock(LockService.class);
        coordinator = new LeaderElectionCoordinator(lockService, INSTANCE, Duration.ofSeconds(60));
        onBecameLeader = mock(Runnable.class);
        onLostLeadership = mock(Runnable.class);
    }

    @AfterEach
    void tearDown() {
        coordinator.shutdown();
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
}
