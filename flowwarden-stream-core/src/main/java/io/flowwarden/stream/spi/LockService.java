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
package io.flowwarden.stream.spi;

import java.time.Duration;
import java.util.Optional;

/**
 * SPI for the distributed lock backing {@code DeploymentMode.SINGLE_LEADER}.
 *
 * <p>Implementations are stateless with respect to caller identity: {@code instanceId} and the
 * lock {@code ttl} are passed on every acquire / renew call. This keeps the SPI agnostic to the
 * underlying storage (MongoDB, Redis, Consul, JDBC, …) and makes the contract trivial to test in
 * isolation.</p>
 *
 * <p>The Core ships with a {@linkplain #noOp() no-op} implementation and a MongoDB-backed
 * implementation registered via auto-configuration. Users may provide their own implementation as
 * a Spring bean — the auto-configured bean is annotated with {@code @ConditionalOnMissingBean}.</p>
 *
 * <h2>Lifecycle contract</h2>
 * <ul>
 *   <li>{@link #tryAcquire} succeeds if the lock does not exist, has expired, or is already owned
 *       by {@code instanceId}. It must be atomic against concurrent callers.</li>
 *   <li>{@link #renew} extends the expiration of a lock owned by {@code instanceId}. It must
 *       fail if the lock is owned by a different instance or has expired.</li>
 *   <li>{@link #release} removes the lock if and only if it is owned by {@code instanceId}. It
 *       is a no-op otherwise.</li>
 * </ul>
 *
 * <h2>Error contract for hot-path methods</h2>
 * <p>Implementations of {@link #tryAcquire} and {@link #renew} must convert transient backend
 * errors (network timeout, primary stepdown, command timeout, lost connection, etc.) into a
 * {@code false} return rather than propagating exceptions. The leader election coordinator
 * treats propagated exceptions defensively — a {@link #renew} throw triggers the same fail-stop
 * path as {@code renew() == false} — but this is a safety net for non-conformant implementations,
 * not the contract. A propagated cause is only logged at {@code WARN}, losing the structured
 * signal that a {@code false} return provides.</p>
 */
public interface LockService {

    /**
     * Attempts to acquire the lock for {@code streamName} on behalf of {@code instanceId}.
     *
     * <p>Succeeds if the lock doesn't exist, has expired, or is already owned by
     * {@code instanceId}. The operation must be atomic with respect to concurrent callers.</p>
     *
     * <p>Implementations must convert transient backend errors into a {@code false} return
     * rather than propagating exceptions — see the class-level
     * {@linkplain LockService error contract} for hot-path methods.</p>
     *
     * @param streamName the stream identifier to lock
     * @param instanceId the calling instance's identity
     * @param ttl        how long the lock should remain valid without a renew
     * @return {@code true} if the caller now holds the lock
     */
    boolean tryAcquire(String streamName, String instanceId, Duration ttl);

    /**
     * Renews the lock for {@code streamName}. Only succeeds if {@code instanceId} still owns it.
     *
     * <p>Implementations must convert transient backend errors into a {@code false} return
     * rather than propagating exceptions — see the class-level
     * {@linkplain LockService error contract} for hot-path methods. The leader election
     * coordinator treats propagated exceptions as a fail-stop equivalent to {@code false}
     * defensively, but loses the structured cause in the process.</p>
     *
     * @param streamName the stream identifier
     * @param instanceId the calling instance's identity
     * @param ttl        the new lock validity duration
     * @return {@code true} if the renewal succeeded
     */
    boolean renew(String streamName, String instanceId, Duration ttl);

    /**
     * Releases the lock for {@code streamName} if and only if it is owned by {@code instanceId}.
     *
     * @param streamName the stream identifier
     * @param instanceId the calling instance's identity
     */
    void release(String streamName, String instanceId);

    /**
     * Returns the current state of the lock for {@code streamName}, if any active lock exists.
     *
     * <p>Implementations must return {@link Optional#empty()} for missing or expired locks.</p>
     *
     * @param streamName the stream identifier
     * @return the lock state, or empty if no active lock exists
     */
    Optional<LockState> getLockState(String streamName);

    /**
     * Returns the {@code instanceId} of the current leader for {@code streamName}, if any.
     *
     * <p>Convenience shorthand for {@code getLockState(streamName).map(LockState::instanceId)}.</p>
     *
     * @param streamName the stream identifier
     * @return the leader's instance id, or empty if no active lock exists
     */
    default Optional<String> getCurrentLeader(String streamName) {
        return getLockState(streamName).map(LockState::instanceId);
    }

    /**
     * Returns the shared no-op implementation that always grants the lock to the caller.
     *
     * <p>Suitable for tests and for {@code DeploymentMode.ALL_INSTANCES} setups where no
     * coordination is required.</p>
     */
    static LockService noOp() {
        return NoOpLockService.INSTANCE;
    }
}
