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

import java.time.Instant;

/**
 * Snapshot of a distributed lock at a point in time.
 *
 * <p>Returned by {@link LockService#getLockState(String)} for the Reporter / Console
 * Leadership panel and any external consumer that needs to inspect lock ownership.</p>
 *
 * @param streamName the stream the lock protects
 * @param instanceId the identifier of the instance currently holding the lock
 * @param acquiredAt when the lock was last acquired or renewed
 * @param expiresAt  when the lock will expire if not renewed
 */
public record LockState(
        String streamName,
        String instanceId,
        Instant acquiredAt,
        Instant expiresAt) {
}
