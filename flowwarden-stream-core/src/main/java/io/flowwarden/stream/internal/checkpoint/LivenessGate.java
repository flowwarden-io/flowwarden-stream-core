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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-generation gate serializing "a handler starts" against "the stream is
 * declared stuck": {@code IDLE → HANDLING → IDLE} around every dispatch, and
 * {@code IDLE → STUCK} once, for the verdict. Both transitions are
 * compare-and-set, so exactly one of them wins a race — a handler that
 * started cannot be restarted from under, and a generation declared stuck
 * cannot start a handler: the event is dropped, and the restart applies the
 * stream's {@code StartPosition} — redelivered under {@code RESUME} (the
 * cascade restarts from {@code lastProcessedToken}), skipped under
 * {@code LATEST} (the loss that position accepts on every restart).
 *
 * <p>A plain flag would only give visibility, not the mutual exclusion
 * between the check and the act.</p>
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public final class LivenessGate {

    private static final int IDLE = 0;
    private static final int HANDLING = 1;
    private static final int STUCK = 2;

    private final AtomicInteger phase = new AtomicInteger(IDLE);

    /**
     * Claims the gate for a dispatch. {@code false} when the generation was
     * declared stuck — the caller must not run the handler.
     */
    public boolean enterHandling() {
        return phase.compareAndSet(IDLE, HANDLING);
    }

    /** Releases the gate after a dispatch settled (success, skip, DLQ, failure). */
    public void exitHandling() {
        phase.compareAndSet(HANDLING, IDLE);
    }

    /**
     * Claims the stuck verdict. {@code false} when a handler is in flight
     * (the verdict is deferred) or the verdict was already claimed.
     */
    public boolean claimStuck() {
        return phase.compareAndSet(IDLE, STUCK);
    }

    public boolean isHandling() {
        return phase.get() == HANDLING;
    }

    public boolean isStuck() {
        return phase.get() == STUCK;
    }
}
