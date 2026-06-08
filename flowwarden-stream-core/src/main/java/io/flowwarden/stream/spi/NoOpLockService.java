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
 * Default no-op implementation of {@link LockService}.
 *
 * <p>Always grants the lock to the caller and reports no active leader. Suitable for tests
 * and for {@code DeploymentMode.ALL_INSTANCES} setups where no coordination is required.</p>
 */
final class NoOpLockService implements LockService {

    static final NoOpLockService INSTANCE = new NoOpLockService();

    private NoOpLockService() {
    }

    @Override
    public boolean tryAcquire(String streamName, String instanceId, Duration ttl) {
        return true;
    }

    @Override
    public boolean renew(String streamName, String instanceId, Duration ttl) {
        return true;
    }

    @Override
    public void release(String streamName, String instanceId) {
    }

    @Override
    public Optional<LockState> getLockState(String streamName) {
        return Optional.empty();
    }
}
