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
package io.flowwarden.stream.internal;

import io.flowwarden.stream.FlowWardenMetrics;
import io.flowwarden.stream.HistoryLostException;
import io.flowwarden.stream.spi.StopReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Managed resubscription loop shared by both stream managers: when a stream
 * dies at <em>runtime</em> (cursor death — network outage, primary stepdown,
 * non-resumable server error), the manager evicts the per-stream state,
 * emits {@code onStreamStopped(CRASHED)}, and hands the stream to this class,
 * which re-enters the full startup path (resume cascade included) with capped
 * exponential backoff.
 *
 * <p><strong>Semantics.</strong> Transient failures are retried indefinitely
 * — a database down for two hours must not leave every stream dead once it
 * comes back. The backoff attempt counter resets on the first successful
 * resubscription. A {@link HistoryLostException} escaping the restart (the
 * resume cascade escalated to {@code OnHistoryLost.FAIL}) is terminal: the
 * loop stops for that stream, the crash is surfaced, and the manager's
 * terminal callback runs (under {@code SINGLE_LEADER} it releases the lock
 * so the lease is not renewed for a corpse).</p>
 *
 * <p><strong>Lifecycle.</strong> Each stream owns a single
 * {@link RestartState} guarded by this instance's monitor, carrying a
 * <em>globally monotonic, never reused</em> generation, the attempt counter,
 * the death cause, and the <em>only</em> valid pending future (a newer death
 * notification cancels the previous future before scheduling — no blind
 * removals, no orphaned handles). {@link #cancel(String)} removes the state:
 * an in-flight attempt detects the mismatch and stands down — it never stops
 * a stream itself; the operator stop that triggered the cancel is serialized
 * behind the attempt by the manager's per-stream lifecycle lock and performs
 * the actual teardown, so a by-name rollback can never hit a newer manual
 * generation.</p>
 *
 * <p>Restarts run on a dedicated single thread: a restart attempt performs
 * blocking I/O (cascade validation, bootstrap probe) and must not delay
 * heartbeat probes or flushes of healthy streams.</p>
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public final class StreamRestarter {

    private static final Logger log = LoggerFactory.getLogger(StreamRestarter.class);

    static final long BASE_DELAY_SECONDS = 1;
    static final long MAX_DELAY_SECONDS = 60;

    /** Manager-side operations the restart loop drives. */
    public interface Callbacks {

        /**
         * Full startup path — resume cascade, heartbeat setup, subscription.
         * The manager serializes it per stream against {@code stopStream} and
         * manual starts (the lifecycle lock): an operator stop issued while
         * an attempt is inside this call is guaranteed to run <em>after</em>
         * it and tears down whatever the attempt installed — the restarter
         * itself never stops streams.
         */
        void startStream(String streamName);

        /**
         * Whether the stream's state is currently installed in the manager
         * (i.e. the last {@code startStream} took — a reactive subscription
         * that terminated synchronously does not count and reports its own
         * death through {@code onRuntimeDeath} again).
         */
        boolean isInstalled(String streamName);

        /**
         * Terminal give-up hook: the restart loop stops for this stream.
         * Under {@code SINGLE_LEADER} the manager stops the election so the
         * lock is released instead of being renewed for a dead stream.
         */
        void onTerminalGiveUp(String streamName);
    }

    /** Per-stream lifecycle state. All fields guarded by the restarter's monitor. */
    private static final class RestartState {
        final long generation;
        int attempt;
        Throwable cause;
        ScheduledFuture<?> future;

        RestartState(long generation) {
            this.generation = generation;
        }
    }

    private final Callbacks callbacks;
    private final ScheduledExecutorService scheduler;
    private final Map<String, RestartState> states = new HashMap<>();
    /**
     * Monotonic generation source, shared across all streams and never
     * reused: a state created after a {@code cancel()} can never carry the
     * same generation an in-flight attempt captured from the removed one
     * (the ABA the per-state counter allowed).
     */
    private final java.util.concurrent.atomic.AtomicLong generations =
            new java.util.concurrent.atomic.AtomicLong();

    public StreamRestarter(String threadName, Callbacks callbacks) {
        this.callbacks = callbacks;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, threadName);
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Reports a runtime death and schedules a resubscription attempt. The
     * caller has already emitted {@code onStreamStopped(CRASHED)} and evicted
     * the per-stream state. If an attempt is already pending, it is cancelled
     * and replaced — one valid future per stream, always.
     */
    public void onRuntimeDeath(String streamName, Throwable cause) {
        int attempt;
        long delay;
        long generation;
        synchronized (this) {
            RestartState state = states.computeIfAbsent(streamName,
                    k -> new RestartState(generations.incrementAndGet()));
            if (state.future != null) {
                state.future.cancel(false);
                state.future = null;
            }
            if (cause != null) {
                state.cause = cause;
            }
            state.attempt++;
            attempt = state.attempt;
            generation = state.generation;
            delay = delaySeconds(attempt);
            scheduleLocked(streamName, state, generation, delay);
        }
        log.warn("Stream '{}' died at runtime — resubscription attempt {} in {}s",
                streamName, attempt, delay);
    }

    /**
     * Cancels the restart lifecycle for the stream — called by graceful
     * {@code stopStream} (an operator stop must win over the loop, including
     * against an attempt already in flight) and by shutdown.
     */
    public synchronized void cancel(String streamName) {
        // Removing the state invalidates any in-flight attempt: its captured
        // generation can never match again (generations are monotonic and
        // never reused, so a state re-created by a later legitimate death
        // carries a fresh identity — no ABA).
        RestartState state = states.remove(streamName);
        if (state != null && state.future != null) {
            state.future.cancel(false);
        }
    }

    /** Whether a restart is scheduled or in flight for the stream (test/diagnostic hook). */
    public synchronized boolean isRestartPending(String streamName) {
        return states.containsKey(streamName);
    }

    public void shutdown() {
        scheduler.shutdownNow();
    }

    /** Caller holds the monitor. */
    private void scheduleLocked(String streamName, RestartState state, long generation, long delaySeconds) {
        try {
            state.future = scheduler.schedule(
                    () -> attemptRestart(streamName, generation),
                    delaySeconds, TimeUnit.SECONDS);
        } catch (RejectedExecutionException e) {
            // Scheduler shut down — the application is going away.
            states.remove(streamName);
        }
    }

    private void attemptRestart(String streamName, long generation) {
        int attempt;
        Throwable cause;
        synchronized (this) {
            RestartState state = states.get(streamName);
            if (state == null || state.generation != generation) {
                return; // cancelled while pending
            }
            state.future = null; // now in flight
            attempt = state.attempt;
            cause = state.cause;
        }

        if (callbacks.isInstalled(streamName)) {
            // Manually restarted (or never fully died) in the meantime — the
            // installed stream wins, the loop stands down.
            clearIfCurrent(streamName, generation);
            return;
        }

        try {
            callbacks.startStream(streamName);
        } catch (HistoryLostException e) {
            // Terminal: the cascade escalated to OnHistoryLost.FAIL — the
            // next attempt fails identically until an operator intervenes.
            if (!clearIfCurrent(streamName, generation)) {
                return; // cancelled mid-flight: the stop already won
            }
            log.error("Stream '{}' restart failed terminally (attempt {}) — giving up: {}",
                    streamName, attempt, e.getMessage(), e);
            emitCrashed(streamName, e);
            callbacks.onTerminalGiveUp(streamName);
            return;
        } catch (RuntimeException e) {
            // Transient (server still down, cascade probe failure, …): keep
            // trying — the backoff is capped, the loop never gives up on a
            // transient class of failure.
            synchronized (this) {
                RestartState state = states.get(streamName);
                if (state == null || state.generation != generation) {
                    return; // cancelled mid-flight
                }
                state.attempt++;
                long delay = delaySeconds(state.attempt);
                log.warn("Stream '{}' restart attempt {} failed ({}) — retrying in {}s",
                        streamName, attempt, e.getMessage(), delay);
                scheduleLocked(streamName, state, generation, delay);
            }
            return;
        }

        Outcome outcome;
        synchronized (this) {
            RestartState state = states.get(streamName);
            if (state == null || state.generation != generation) {
                // cancel() removed the state while startStream was in
                // flight: the operator stop wins. NO rollback here — the
                // manager's per-stream lifecycle lock guarantees the stop
                // that triggered the cancel runs AFTER this attempt's
                // startStream and tears down whatever it installed; a
                // by-name rollback from here could stop a newer manual
                // generation instead.
                outcome = Outcome.CANCELLED;
            } else if (state.future != null) {
                // The subscription terminated synchronously during this very
                // startStream (reactive) and its death notification re-armed
                // the lifecycle: the newer future owns the next step.
                outcome = Outcome.REARMED;
            } else {
                states.remove(streamName);
                outcome = Outcome.CURRENT;
            }
        }
        if (outcome != Outcome.CURRENT) {
            return;
        }
        if (!callbacks.isInstalled(streamName)) {
            // Defensive: returned without installing and without a death
            // notification — nothing left to own.
            return;
        }
        log.info("Stream '{}' resubscribed after runtime death (attempt {})",
                streamName, attempt);
        try {
            FlowWardenMetrics.get().onStreamRestarted(streamName, attempt, cause);
        } catch (Exception e) {
            log.warn("Metrics provider failed on restart signal for stream '{}': {}",
                    streamName, e.getMessage());
        }
    }

    private enum Outcome { CANCELLED, REARMED, CURRENT }

    /**
     * Removes the state if it still belongs to this generation, cancelling
     * any future that re-armed it in the meantime (a terminal give-up owns
     * the lifecycle end — a re-armed attempt would fail identically).
     */
    private synchronized boolean clearIfCurrent(String streamName, long generation) {
        RestartState state = states.get(streamName);
        if (state == null || state.generation != generation) {
            return false;
        }
        if (state.future != null) {
            state.future.cancel(false);
        }
        states.remove(streamName);
        return true;
    }

    private void emitCrashed(String streamName, Throwable cause) {
        try {
            FlowWardenMetrics.get().onStreamStopped(streamName, StopReason.CRASHED, cause);
        } catch (Exception e) {
            log.warn("Metrics provider failed on crash signal for stream '{}': {}",
                    streamName, e.getMessage());
        }
    }

    static long delaySeconds(int attempt) {
        // 1, 2, 4, 8, 16, 32, then capped at 60 — never a hot loop against a
        // hard-down server, never more than a minute behind a recovered one.
        long exponential = BASE_DELAY_SECONDS << Math.min(attempt - 1, 6);
        return Math.min(MAX_DELAY_SECONDS, exponential);
    }
}
