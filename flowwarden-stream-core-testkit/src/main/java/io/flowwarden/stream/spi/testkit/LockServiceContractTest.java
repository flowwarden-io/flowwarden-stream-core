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
package io.flowwarden.stream.spi.testkit;

import io.flowwarden.stream.spi.LockService;
import io.flowwarden.stream.spi.LockState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Behavior contract that every {@link LockService} implementation must satisfy.
 *
 * <p>Subclass for each backend (Mongo, Redis, …) and provide a fresh {@link LockService}
 * via {@link #createLockService()} plus a {@link #cleanState()} hook that resets the
 * backing storage between tests. Override the optional {@link #expireLeaseInPlace(String)}
 * hook to also run the cases that need an expired lease.</p>
 */
public abstract class LockServiceContractTest {

    protected static final String INSTANCE_A = "host-a:app:8080";
    protected static final String INSTANCE_B = "host-b:app:8080";
    protected static final Duration TTL = Duration.ofSeconds(60);

    protected LockService lockService;

    /**
     * Returns a fresh {@link LockService} bound to a clean backend.
     */
    protected abstract LockService createLockService();

    /**
     * Clears all state in the backing storage so tests are isolated.
     */
    protected abstract void cleanState();

    /**
     * Optional hook: makes the lease of {@code streamName} expired in the backing storage,
     * in place, without changing its owner — as if the owner's renewals had stopped past
     * the TTL. Returns {@code true} when done; the default returns {@code false}, which
     * skips the contract cases that need an expired lease ({@link #renew_failsIfLeaseExpired}).
     *
     * <p>Backends that can edit a lease's expiry directly (a document store, a key with a
     * stored expiry) should override it so those cases run against them.</p>
     *
     * @param streamName the stream whose lease to expire
     * @return {@code true} if the lease is now expired, {@code false} if the backend does
     *         not support expiring a lease in place
     */
    protected boolean expireLeaseInPlace(String streamName) {
        return false;
    }

    @BeforeEach
    void setUpContract() {
        cleanState();
        lockService = createLockService();
    }

    @Test
    void renew_failsIfLeaseExpired() {
        assertThat(lockService.tryAcquire("my-stream", INSTANCE_A, TTL)).isTrue();
        assumeTrue(expireLeaseInPlace("my-stream"),
                "backend cannot expire a lease in place — override expireLeaseInPlace() to run this case");

        // Nobody took the lease over, but it is no longer A's to extend: the owner must go
        // through the leadership-loss path and acquire again explicitly.
        assertThat(lockService.renew("my-stream", INSTANCE_A, TTL)).isFalse();
        assertThat(lockService.getLockState("my-stream")).isEmpty();

        assertThat(lockService.tryAcquire("my-stream", INSTANCE_A, TTL)).isTrue();
        assertThat(lockService.renew("my-stream", INSTANCE_A, TTL)).isTrue();
    }

    @Test
    void tryAcquire_succeedsOnFreeLock() {
        assertThat(lockService.tryAcquire("my-stream", INSTANCE_A, TTL)).isTrue();
    }

    @Test
    void tryAcquire_failsIfHeldByOtherInstance() {
        lockService.tryAcquire("my-stream", INSTANCE_A, TTL);
        assertThat(lockService.tryAcquire("my-stream", INSTANCE_B, TTL)).isFalse();
    }

    @Test
    void tryAcquire_succeedsIfAlreadyOwned() {
        lockService.tryAcquire("my-stream", INSTANCE_A, TTL);
        assertThat(lockService.tryAcquire("my-stream", INSTANCE_A, TTL)).isTrue();
    }

    @Test
    void renew_succeedsForOwner() {
        lockService.tryAcquire("my-stream", INSTANCE_A, TTL);
        assertThat(lockService.renew("my-stream", INSTANCE_A, TTL)).isTrue();
    }

    @Test
    void renew_failsForNonOwner() {
        lockService.tryAcquire("my-stream", INSTANCE_A, TTL);
        assertThat(lockService.renew("my-stream", INSTANCE_B, TTL)).isFalse();
    }

    @Test
    void renew_failsIfNoLockExists() {
        assertThat(lockService.renew("nonexistent", INSTANCE_A, TTL)).isFalse();
    }

    @Test
    void release_freesLockForOtherInstance() {
        lockService.tryAcquire("my-stream", INSTANCE_A, TTL);
        lockService.release("my-stream", INSTANCE_A);

        assertThat(lockService.tryAcquire("my-stream", INSTANCE_B, TTL)).isTrue();
    }

    @Test
    void release_isNoOpForNonOwner() {
        lockService.tryAcquire("my-stream", INSTANCE_A, TTL);
        lockService.release("my-stream", INSTANCE_B); // B is not the owner — no-op

        // A still owns it, can still renew
        assertThat(lockService.renew("my-stream", INSTANCE_A, TTL)).isTrue();
    }

    @Test
    void getLockState_returnsCurrentHolder() {
        Instant before = Instant.now();
        lockService.tryAcquire("my-stream", INSTANCE_A, TTL);

        LockState state = lockService.getLockState("my-stream").orElseThrow();
        assertThat(state.streamName()).isEqualTo("my-stream");
        assertThat(state.instanceId()).isEqualTo(INSTANCE_A);
        assertThat(state.acquiredAt()).isAfterOrEqualTo(before.minusSeconds(1));
        assertThat(state.expiresAt()).isAfter(state.acquiredAt());
    }

    @Test
    void getLockState_emptyIfNoLock() {
        assertThat(lockService.getLockState("nonexistent")).isEmpty();
    }

    @Test
    void getCurrentLeader_shorthandForLockState() {
        lockService.tryAcquire("my-stream", INSTANCE_A, TTL);
        assertThat(lockService.getCurrentLeader("my-stream")).hasValue(INSTANCE_A);
    }

    @Test
    void getCurrentLeader_emptyIfNoLock() {
        assertThat(lockService.getCurrentLeader("nonexistent")).isEmpty();
    }

    @Test
    void multipleStreams_areIndependent() {
        assertThat(lockService.tryAcquire("stream-1", INSTANCE_A, TTL)).isTrue();
        assertThat(lockService.tryAcquire("stream-2", INSTANCE_B, TTL)).isTrue();

        assertThat(lockService.getCurrentLeader("stream-1")).hasValue(INSTANCE_A);
        assertThat(lockService.getCurrentLeader("stream-2")).hasValue(INSTANCE_B);
    }
}
