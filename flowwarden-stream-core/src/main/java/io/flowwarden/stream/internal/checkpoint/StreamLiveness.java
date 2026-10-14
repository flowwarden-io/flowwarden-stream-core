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

import io.flowwarden.stream.StreamStuckException;

/**
 * Manager-side liveness hooks consulted by {@link CheckpointHeartbeat} when
 * an abstention streak reaches the stream's {@code stuckAfterProbes}
 * ceiling.
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public interface StreamLiveness {

    /**
     * Atomically claims the stuck verdict against the generation's
     * {@link LivenessGate}. {@code false} when a dispatch is in flight
     * (initial attempt, retry backoff, {@code @OnError} resolution,
     * settlement): a stream with work in flight is <em>lagging</em>, not
     * stuck — its settled-token snapshot cannot move until the dispatch
     * ends, which is exactly the signal a dead cursor produces — so the
     * verdict is deferred. Once {@code true} is returned, no handler of
     * this generation can start any more.
     */
    boolean claimStuck();

    /**
     * The verdict, after a successful {@link #claimStuck()}. Called once,
     * outside the heartbeat's lock, after the heartbeat cancelled itself.
     * The manager takes the runtime-death path — crash signal, eviction,
     * restart hand-off.
     */
    void onStuck(StreamStuckException cause);
}
