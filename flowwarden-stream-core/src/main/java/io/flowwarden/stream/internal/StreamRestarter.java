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
import java.util.concurrent.ExecutorService;
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
 * The backoff attempt counter resets once a resubscribed generation
 * <em>proves alive</em> — never on the synchronous return of
 * {@link Callbacks#startStream}: the cursor opens asynchronously (on
 * Spring's reading thread, or in the reactive driver's callbacks), so a
 * refused open reaches the manager's death path after that return. Resetting
 * there would retry a persistently refused open at {@code initialDelay}
 * forever, never ramp up, never exhaust a bounded {@code maxAttempts}, and
 * report a successful restart per turn. The state therefore survives the
 * return as <em>installed, unproven</em>: a death for such a generation
 * continues the attempt count, and the manager's
 * {@link #onGenerationProven proof of life} (first delivered event, or the
 * imperative reading task's completed start) ends the cycle — that is also
 * when {@code onStreamRestarted} is emitted. The proof is bound to the
 * generation it is about: the manager passes the identity of its installed
 * state, compared under the monitor with the one recorded when the attempt
 * committed, so a late proof of a stopped or replaced generation can never
 * end the cycle of the one that succeeded it. A reactive stream proves
 * itself on its first event only, so an idle generation would otherwise keep
 * the cycle open indefinitely; as a <em>heuristic</em>, in both modes, an
 * installed generation that outlives the policy's {@code maxDelay} without
 * a death notification is <em>assumed</em> alive and ends the cycle the
 * same way. This is an assumption, not a guarantee: {@code maxDelay} is
 * configured independently of the driver's timeouts, so an open that fails
 * later than {@code maxDelay} is counted as a fresh death at attempt 1 — the
 * price for a bounded policy that must not give up on a merely idle stream
 * after N transient deaths spread over days.
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
 * <p>Restart timing runs on a dedicated single thread; each attempt is then
 * handed to a separate start executor, because an attempt performs blocking
 * I/O (cascade validation, bootstrap probe) that must not delay the timing of
 * other streams' restarts nor heartbeat probes or flushes of healthy
 * streams.</p>
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
         * death through {@code onRuntimeDeath} again). Installed is not
         * alive: the manager reports the latter through
         * {@link StreamRestarter#onGenerationProven}.
         */
        boolean isInstalled(String streamName);

        /**
         * The identity of the stream's currently installed generation — the
         * manager's own per-generation state object, the same one it later
         * passes to {@link StreamRestarter#onGenerationProven} — or
         * {@code null} when nothing is installed. Read under the restarter's
         * monitor when an attempt commits, so a proof of life can be matched
         * to the generation the attempt actually installed (identity
         * comparison, never {@code equals}).
         */
        Object installedGeneration(String streamName);

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
        /**
         * The only valid pending step: the next attempt (or the give-up)
         * while the stream is down, the age-based proof timer once
         * {@link #installed}.
         */
        ScheduledFuture<?> future;
        /**
         * The current attempt's {@code startStream} returned and installed a
         * generation whose proof of life is awaited. A death notification
         * continues this state's attempt count instead of starting over.
         */
        boolean installed;
        /**
         * Identity of the generation the current attempt installed
         * ({@link Callbacks#installedGeneration}), recorded at commit. A
         * proof of life is honoured only for this identity — a late proof of
         * a stopped or replaced generation must never end this cycle.
         */
        Object installedId;
        /**
         * Identity carried by a proof of life that arrived while the
         * attempt's {@code startStream} was still in flight (an event
         * delivered before the start method returned). Recorded only when
         * it is the identity the manager has installed at that moment
         * ({@link Callbacks#installedGeneration}, read under the monitor),
         * so a stale proof can never overwrite a real one; honoured at
         * commit only if it is still the identity the attempt installed.
         */
        Object provenInFlightId;

        RestartState(long generation, RestartPolicyConfig policy) {
            this.generation = generation;
            this.policy = policy;
        }
    }

    private final Callbacks callbacks;
    private final ScheduledExecutorService scheduler;
    /**
     * Where restart attempts actually run. The scheduler only times them: a
     * start attempt can block for as long as the stream's resume cascade and
     * its {@code @Reconcile} hook take (a reconciliation may run for
     * minutes), and it must never hold the single timing thread hostage —
     * every other stream's restart would wait behind it.
     */
    private final ExecutorService startExecutor;
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
        this.startExecutor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, threadName + "-start");
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
            // An installed-but-unproven generation died: its attempt was
            // not a resubscription, the count goes on.
            state.installed = false;
            state.installedId = null;
            state.provenInFlightId = null;
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

    /**
     * Proof that the generation installed by the last restart attempt is
     * alive — reported by the manager on the first delivered event or, in
     * imperative mode, on the reading task's completed start, with the
     * identity of the generation it is about (the manager's own state
     * object, as returned by {@link Callbacks#installedGeneration}). Ends
     * the restart cycle when that identity is the one the attempt
     * installed: the state is cleared (the next death starts at attempt 1)
     * and {@code onStreamRestarted} is emitted with the attempt that
     * succeeded. Any other proof is a late signal of a stopped or replaced
     * generation and changes nothing — the manager's own fence
     * (terminated flag, current-state check) runs outside any lock and can
     * be overtaken by a stop, a death and a new attempt; the identity
     * comparison under the monitor is what makes the proof safe. A proof
     * for a stream outside a restart cycle (boot start, manual start) is a
     * no-op; one arriving while the attempt's {@code startStream} is still
     * in flight is kept until the attempt commits — but only if its identity
     * is the generation the manager has installed at that moment, checked
     * under the monitor: a stale proof is rejected on arrival, so it cannot
     * overwrite a real in-flight proof whatever the order the two land in
     * (the manager sends each generation's proof once). The commit then
     * re-checks the kept identity against what the attempt installed.
     */
    public void onGenerationProven(String streamName, Object generationId) {
        int attempt;
        Throwable cause;
        synchronized (this) {
            RestartState state = states.get(streamName);
            if (state == null || generationId == null) {
                return; // not in a restart cycle
            }
            if (!state.installed) {
                if (state.future == null
                        && generationId == callbacks.installedGeneration(streamName)) {
                    // Attempt in flight, and the proof is about the generation
                    // the manager currently has installed — the one this
                    // startStream registered, which delivered before
                    // returning. Validated HERE, not only at commit: a stale
                    // proof (its generation stopped or replaced, so never the
                    // installed one) must not overwrite a real one recorded
                    // earlier, whatever the arrival order — the manager sends
                    // each generation's proof once and never resends it.
                    state.provenInFlightId = generationId;
                }
                return; // else pending, or a stray proof of another generation
            }
            if (state.installedId != generationId) {
                return; // a late proof of a stopped or replaced generation
            }
            if (state.future != null) {
                state.future.cancel(false); // the age-based proof timer
            }
            states.remove(streamName);
            attempt = state.attempt;
            cause = state.cause;
        }
        emitRestarted(streamName, attempt, cause);
    }

    /**
     * Whether a restart is scheduled or in flight for the stream — the
     * stream is down and the loop owns its next step (test/diagnostic
     * hook). {@code false} once the attempt installed a generation, even
     * while its proof of life is still awaited; see {@link #isAwaitingProof}.
     */
    public synchronized boolean isRestartPending(String streamName) {
        RestartState state = states.get(streamName);
        return state != null && !state.installed;
    }

    /**
     * Whether the last attempt installed a generation whose proof of life is
     * still awaited — the restart cycle is open, a death would continue its
     * attempt count (test/diagnostic hook).
     */
    public synchronized boolean isAwaitingProof(String streamName) {
        RestartState state = states.get(streamName);
        return state != null && state.installed;
    }

    public void shutdown() {
        scheduler.shutdownNow();
        startExecutor.shutdownNow();
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
                    () -> dispatchAttempt(streamName, generation),
                    delayMillis, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            // Scheduler shut down — the application is going away.
            states.remove(streamName);
        }
        return exhausted ? -1 : delayMillis;
    }

    /**
     * Hands the attempt to the start executor so the timing thread is free
     * at once. A rejection means the restarter is shutting down — the
     * application is going away, the attempt is dropped like a cancelled
     * one.
     */
    private void dispatchAttempt(String streamName, long generation) {
        try {
            startExecutor.execute(() -> attemptRestart(streamName, generation));
        } catch (RejectedExecutionException e) {
            synchronized (this) {
                states.remove(streamName);
            }
        }
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
                state.provenInFlightId = null; // whatever delivered, the start failed
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
        boolean proven = false;
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
                // The identity of what this attempt installed, read under the
                // monitor: every later proof of life is matched against it.
                Object installedId = callbacks.installedGeneration(streamName);
                if (installedId != null && state.provenInFlightId == installedId) {
                    // This very generation delivered before startStream
                    // returned: the cycle ends here.
                    states.remove(streamName);
                    proven = true;
                } else {
                    // The return proves nothing about the cursor, which opens
                    // asynchronously: keep the state, installed and unproven,
                    // until the manager reports this generation's proof of
                    // life — or the generation outlives the policy's
                    // maxDelay. A proof carried in flight for another
                    // identity was a stray and is dropped.
                    state.installed = true;
                    state.installedId = installedId;
                    state.provenInFlightId = null;
                    scheduleAgeProofLocked(streamName, state);
                }
                outcome = Outcome.CURRENT;
            }
        }
        if (outcome != Outcome.CURRENT) {
            return;
        }
        if (!callbacks.isInstalled(streamName)) {
            // Defensive: returned without installing and without a death
            // notification — nothing left to own.
            clearIfInstalled(streamName, generation);
            return;
        }
        if (proven) {
            emitRestarted(streamName, attempt, cause);
            return;
        }
        log.info("Stream '{}' resubscribed after runtime death (attempt {}) — "
                + "awaiting the cursor's proof of life", streamName, attempt);
    }

    /**
     * Arms the age-based proof — a <em>heuristic</em>, armed in both modes:
     * an installed generation that outlives the policy's {@code maxDelay}
     * without a death notification is <em>assumed</em> alive and ends the
     * cycle as a proof of life would. A reactive stream only proves itself
     * through its first event, so without this an idle reactive generation
     * would keep the cycle open indefinitely and a death hours later would
     * count as attempt N+1 — and, under a bounded policy, eventually give
     * up on a stream that was merely idle. Nothing guarantees that an open
     * fails within {@code maxDelay}: it is configured independently of the
     * driver's timeouts (server selection, a {@code socketTimeoutMS} of 0).
     * An open that fails later is simply counted as a fresh death at
     * attempt 1 — the loop still backs off, it only forgets the previous
     * attempts. Caller holds the monitor.
     */
    private void scheduleAgeProofLocked(String streamName, RestartState state) {
        long generation = state.generation;
        try {
            state.future = scheduler.schedule(
                    () -> proveByAge(streamName, generation),
                    state.policy.maxDelay().toMillis(), TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            // Scheduler shut down — the application is going away.
            states.remove(streamName);
        }
    }

    private void proveByAge(String streamName, long generation) {
        int attempt;
        Throwable cause;
        synchronized (this) {
            RestartState state = states.get(streamName);
            if (state == null || state.generation != generation || !state.installed) {
                return; // died, cancelled, or proven in the meantime
            }
            states.remove(streamName);
            attempt = state.attempt;
            cause = state.cause;
        }
        log.info("Stream '{}' outlived the restart policy's maxDelay without dying — "
                + "assumed alive, counted as resubscribed (attempt {})", streamName, attempt);
        emitRestarted(streamName, attempt, cause);
    }

    /**
     * Removes the installed-unproven state of this generation; a state a
     * death notification already re-armed (pending attempt) is left alone.
     */
    private synchronized void clearIfInstalled(String streamName, long generation) {
        RestartState state = states.get(streamName);
        if (state == null || state.generation != generation || !state.installed) {
            return;
        }
        if (state.future != null) {
            state.future.cancel(false);
        }
        states.remove(streamName);
    }

    private void emitRestarted(String streamName, int attempt, Throwable cause) {
        log.info("Stream '{}' resubscribed after runtime death (attempt {}) — cursor alive",
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
