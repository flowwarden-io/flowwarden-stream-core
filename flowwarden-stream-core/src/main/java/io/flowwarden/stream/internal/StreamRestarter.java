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
import io.flowwarden.stream.internal.restart.RestartPolicyConfig;
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
 * <p><strong>Semantics.</strong> The cadence and the attempt ceiling are a
 * per-stream {@link RestartPolicyConfig}, resolved through
 * {@link Callbacks#policyFor(String)} when a stream's lifecycle is created
 * ({@code @RestartPolicy}, or {@link RestartPolicyConfig#DEFAULTS} without
 * it). By default transient failures are retried indefinitely — a database
 * down for two hours must not leave every stream dead once it comes back.
 * The backoff attempt counter resets on the first successful resubscription.
 * A {@link HistoryLostException} escaping the restart (the resume cascade
 * escalated to {@code OnHistoryLost.FAIL}) is terminal: the loop stops for
 * that stream, the crash is surfaced, and the manager's terminal callback
 * runs (under {@code SINGLE_LEADER} it releases the lock so the lease is not
 * renewed for a corpse). Exhausting a bounded policy's {@code maxAttempts}
 * takes exactly the same path, with the last death cause as the crash
 * cause.</p>
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

    /** Manager-side operations the restart loop drives. */
    public interface Callbacks {

        /**
         * The restart policy of the stream — from its {@code @RestartPolicy}
         * (annotated or contributed), or {@link RestartPolicyConfig#DEFAULTS}
         * when it has none. Consulted on each death notification, never under
         * the restarter's monitor; the value captured when the stream's
         * lifecycle is created is the one the loop uses until it ends.
         */
        RestartPolicyConfig policyFor(String streamName);

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
        final RestartPolicyConfig policy;
        int attempt;
        Throwable cause;
        ScheduledFuture<?> future;

        RestartState(long generation, RestartPolicyConfig policy) {
            this.generation = generation;
            this.policy = policy;
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
        // Resolved outside the monitor: the manager consults its registry.
        RestartPolicyConfig policy = callbacks.policyFor(streamName);
        int attempt;
        long delay;
        int maxAttempts;
        synchronized (this) {
            RestartState state = states.computeIfAbsent(streamName,
                    k -> new RestartState(generations.incrementAndGet(), policy));
            if (state.future != null) {
                state.future.cancel(false);
                state.future = null;
            }
            if (cause != null) {
                state.cause = cause;
            }
            state.attempt++;
            attempt = state.attempt;
            maxAttempts = state.policy.maxAttempts();
            delay = scheduleNextLocked(streamName, state);
        }
        if (delay < 0) {
            log.warn("Stream '{}' died at runtime — restart attempts exhausted ({}), giving up",
                    streamName, maxAttempts);
        } else {
            log.warn("Stream '{}' died at runtime — resubscription attempt {} in {}ms",
                    streamName, attempt, delay);
        }
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

    /**
     * Schedules the next step for {@code state.attempt}: the attempt itself
     * after the policy's backoff, or — when the policy's ceiling is
     * exhausted — the give-up, immediately. Returns the delay in
     * milliseconds, or {@code -1} when the step is a give-up. The give-up
     * runs on the restart thread like every other terminal path, so a
     * death notification never calls back into the manager from the thread
     * that reported it. Caller holds the monitor.
     */
    private long scheduleNextLocked(String streamName, RestartState state) {
        long generation = state.generation;
        boolean exhausted = state.policy.isExhausted(state.attempt);
        long delayMillis = exhausted ? 0 : state.policy.computeDelayMillis(state.attempt);
        try {
            state.future = scheduler.schedule(
                    () -> attemptRestart(streamName, generation),
                    delayMillis, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            // Scheduler shut down — the application is going away.
            states.remove(streamName);
        }
        return exhausted ? -1 : delayMillis;
    }

    private void attemptRestart(String streamName, long generation) {
        int attempt;
        Throwable cause;
        RestartPolicyConfig policy;
        synchronized (this) {
            RestartState state = states.get(streamName);
            if (state == null || state.generation != generation) {
                return; // cancelled while pending
            }
            state.future = null; // now in flight
            attempt = state.attempt;
            cause = state.cause;
            policy = state.policy;
        }

        if (callbacks.isInstalled(streamName)) {
            // Manually restarted (or never fully died) in the meantime — the
            // installed stream wins, the loop stands down. Checked before the
            // ceiling: giving up on a running stream would release its lease.
            clearIfCurrent(streamName, generation);
            return;
        }

        if (policy.isExhausted(attempt)) {
            // Terminal: the policy's ceiling is reached — same path as a
            // history loss, with the last death cause as the crash cause.
            if (!clearIfCurrent(streamName, generation)) {
                return; // cancelled mid-flight: the stop already won
            }
            log.error("Stream '{}' restart attempts exhausted ({}) — giving up; death cause: {}",
                    streamName, policy.maxAttempts(),
                    cause != null ? cause.getMessage() : "unknown", cause);
            emitCrashed(streamName, cause);
            callbacks.onTerminalGiveUp(streamName);
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
            // trying — the backoff is capped, and the loop gives up on a
            // transient class of failure only when the policy bounds it.
            synchronized (this) {
                RestartState state = states.get(streamName);
                if (state == null || state.generation != generation) {
                    return; // cancelled mid-flight
                }
                state.attempt++;
                long delay = scheduleNextLocked(streamName, state);
                if (delay < 0) {
                    log.warn("Stream '{}' restart attempt {} failed ({}) — attempts exhausted, giving up",
                            streamName, attempt, e.getMessage());
                } else {
                    log.warn("Stream '{}' restart attempt {} failed ({}) — retrying in {}ms",
                            streamName, attempt, e.getMessage(), delay);
                }
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
}
