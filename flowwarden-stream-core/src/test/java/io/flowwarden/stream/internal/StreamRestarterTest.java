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
import io.flowwarden.stream.spi.StreamMetricsProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Unit tests for the managed resubscription loop: per-stream policy
 * (backoff cadence and attempt ceiling), indefinite retry on transient
 * failures by default, terminal give-up on {@link HistoryLostException} or
 * on an exhausted ceiling, stand-down when the stream was restarted
 * manually, and cancellation by a graceful stop — and the cycle's end: the
 * synchronous return of {@code startStream} proves nothing (the cursor opens
 * asynchronously), only the manager's proof of life resets the attempt
 * counter and emits the restart signal.
 */
class StreamRestarterTest {

    private static final String STREAM = "restart-test";

    /** Default cadence compressed 10x so the tests stay quick: 100ms, 200ms, 400ms … capped at 6s. */
    private static final RestartPolicyConfig FAST_UNBOUNDED = new RestartPolicyConfig(
            0, Duration.ofMillis(100), Duration.ofSeconds(6), 2.0, false);

    private RecordingMetrics metrics;
    private StreamRestarter restarter;

    @BeforeEach
    void setUp() {
        metrics = new RecordingMetrics();
        FlowWardenMetrics.setProvider(metrics);
    }

    /** Restarter whose scripted callbacks prove each installed generation from inside {@code startStream}. */
    private StreamRestarter restarter(RecordingCallbacks callbacks) {
        restarter = new StreamRestarter("fw-restart-test", callbacks);
        callbacks.restarter = restarter;
        return restarter;
    }

    @AfterEach
    void tearDown() {
        if (restarter != null) {
            restarter.shutdown();
        }
        FlowWardenMetrics.setProvider(StreamMetricsProvider.noOp());
    }

    @Test
    void policy_isResolvedPerStream_andDrivesTheCadence() {
        RecordingCallbacks callbacks = new RecordingCallbacks();
        callbacks.policy = FAST_UNBOUNDED;
        callbacks.failStartsUntilAttempt = 2;
        restarter(callbacks);

        restarter.onRuntimeDeath(STREAM, new RuntimeException("cursor died"));

        // attempt 1 at +100ms (fails), attempt 2 at +200ms — far faster than
        // the 1s/2s a default-policy stream would wait.
        await().atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(metrics.restarts).containsExactly(STREAM + ":2:cursor died"));
        assertThat(callbacks.policyLookups)
                .as("the policy is resolved by stream name, at the death notification")
                .containsExactly(STREAM);
    }

    @Test
    void boundedPolicy_exhaustedByTransientFailures_isTerminal_givesUpAndSurfacesCrash() {
        RecordingCallbacks callbacks = new RecordingCallbacks();
        callbacks.policy = new RestartPolicyConfig(2, Duration.ofMillis(100), Duration.ofSeconds(1), 2.0, false);
        callbacks.failStartsUntilAttempt = Integer.MAX_VALUE; // the server never comes back
        restarter(callbacks);
        RuntimeException death = new RuntimeException("cursor died");

        restarter.onRuntimeDeath(STREAM, death);

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(callbacks.terminalGiveUps).containsExactly(STREAM));
        assertThat(callbacks.startCalls.get())
                .as("exactly maxAttempts resubscriptions were tried")
                .isEqualTo(2);
        assertThat(restarter.isRestartPending(STREAM)).isFalse();
        assertThat(metrics.stops)
                .as("same terminal path as a history loss — CRASHED with the death cause")
                .containsExactly(STREAM + ":CRASHED:RuntimeException");
        assertThat(metrics.restarts).isEmpty();
    }

    @Test
    void boundedPolicy_exhaustedByRepeatedDeaths_givesUpWithoutWaitingForBackoff() {
        // Each death notification is an attempt consumed (a reactive
        // subscription that terminates synchronously re-arms the loop this
        // way). Once the ceiling is crossed the give-up is immediate, not
        // after another backoff.
        RecordingCallbacks callbacks = new RecordingCallbacks();
        callbacks.policy = new RestartPolicyConfig(1, Duration.ofSeconds(30), Duration.ofSeconds(30), 2.0, false);
        restarter(callbacks);

        restarter.onRuntimeDeath(STREAM, new RuntimeException("first death"));
        restarter.onRuntimeDeath(STREAM, new RuntimeException("second death"));

        await().atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(callbacks.terminalGiveUps).containsExactly(STREAM));
        assertThat(callbacks.startCalls.get()).isZero();
        assertThat(metrics.stops).containsExactly(STREAM + ":CRASHED:RuntimeException");
    }

    @Test
    void boundedPolicy_installedStreamWinsOverExhaustion_leaseIsNotReleased() {
        RecordingCallbacks callbacks = new RecordingCallbacks();
        callbacks.policy = new RestartPolicyConfig(1, Duration.ofSeconds(30), Duration.ofSeconds(30), 2.0, false);
        callbacks.installed.set(true); // the operator restarted it in the meantime
        restarter(callbacks);

        restarter.onRuntimeDeath(STREAM, new RuntimeException("first death"));
        restarter.onRuntimeDeath(STREAM, new RuntimeException("second death"));

        await().atMost(Duration.ofSeconds(2))
                .until(() -> !restarter.isRestartPending(STREAM));
        assertThat(callbacks.terminalGiveUps)
                .as("giving up on a running stream would release its lease")
                .isEmpty();
        assertThat(metrics.stops).isEmpty();
    }

    @Test
    void boundedPolicy_counterResetsOnSuccess_nextLifecycleStartsFresh() {
        RecordingCallbacks callbacks = new RecordingCallbacks();
        callbacks.policy = new RestartPolicyConfig(2, Duration.ofMillis(100), Duration.ofSeconds(1), 2.0, false);
        callbacks.failStartsUntilAttempt = 2; // first lifecycle: fail once, then succeed
        restarter(callbacks);

        restarter.onRuntimeDeath(STREAM, new RuntimeException("first death"));
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(metrics.restarts).containsExactly(STREAM + ":2:first death"));

        // Second lifecycle: a fresh state, a fresh counter — attempt 1 again.
        callbacks.installed.set(false);
        restarter.onRuntimeDeath(STREAM, new RuntimeException("second death"));
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(metrics.restarts)
                        .containsExactly(STREAM + ":2:first death", STREAM + ":1:second death"));
        assertThat(callbacks.terminalGiveUps).isEmpty();
    }

    @Test
    void runtimeDeath_restartsTheStream_emitsRestartSignalWithAttemptAndCause() {
        RecordingCallbacks callbacks = new RecordingCallbacks();
        restarter(callbacks);
        RuntimeException cause = new RuntimeException("cursor died");

        restarter.onRuntimeDeath(STREAM, cause);

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(callbacks.startCalls.get()).isEqualTo(1));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(metrics.restarts).containsExactly(STREAM + ":1:cursor died"));
        assertThat(restarter.isRestartPending(STREAM)).isFalse();
    }

    @Test
    void transientStartFailures_retryIndefinitely_untilSuccess() {
        RecordingCallbacks callbacks = new RecordingCallbacks();
        callbacks.failStartsUntilAttempt = 3; // first two startStream calls throw
        restarter(callbacks);

        restarter.onRuntimeDeath(STREAM, new RuntimeException("cursor died"));

        // attempt 1 at +1s (fails), attempt 2 at +2s (fails), attempt 3 at +4s
        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThat(callbacks.startCalls.get()).isEqualTo(3));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(metrics.restarts)
                        .as("the restart signal carries the attempt that succeeded")
                        .containsExactly(STREAM + ":3:cursor died"));
    }

    @Test
    void historyLostAtRestart_isTerminal_givesUpAndSurfacesCrash() {
        RecordingCallbacks callbacks = new RecordingCallbacks();
        callbacks.startFailure = new HistoryLostException(STREAM, Instant.now());
        restarter(callbacks);

        restarter.onRuntimeDeath(STREAM, new RuntimeException("cursor died"));

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(callbacks.terminalGiveUps).containsExactly(STREAM));
        assertThat(callbacks.startCalls.get()).isEqualTo(1);
        assertThat(restarter.isRestartPending(STREAM))
                .as("a terminal failure must not schedule another attempt")
                .isFalse();
        assertThat(metrics.stops)
                .containsExactly(STREAM + ":CRASHED:HistoryLostException");
        assertThat(metrics.restarts).isEmpty();
    }

    @Test
    void manuallyRestartedStream_winsOverTheLoop() {
        RecordingCallbacks callbacks = new RecordingCallbacks();
        callbacks.installed.set(true); // the operator restarted it during the backoff
        restarter(callbacks);

        restarter.onRuntimeDeath(STREAM, new RuntimeException("cursor died"));

        await().atMost(Duration.ofSeconds(5))
                .until(() -> !restarter.isRestartPending(STREAM));
        assertThat(callbacks.startCalls.get())
                .as("the installed stream wins — the loop stands down")
                .isZero();
        assertThat(metrics.restarts).isEmpty();
    }

    @Test
    void longStartOnOneStream_doesNotDelayAnotherStreamsRestart() throws Exception {
        java.util.concurrent.CountDownLatch slowEntered = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch slowGate = new java.util.concurrent.CountDownLatch(1);
        java.util.Set<String> started = java.util.concurrent.ConcurrentHashMap.newKeySet();
        StreamRestarter.Callbacks callbacks = new StreamRestarter.Callbacks() {
            @Override
            public RestartPolicyConfig policyFor(String streamName) {
                return FAST_UNBOUNDED;
            }

            @Override
            public void startStream(String streamName) {
                if (streamName.equals("slow")) {
                    slowEntered.countDown();
                    try {
                        slowGate.await(10, java.util.concurrent.TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                started.add(streamName);
            }

            @Override
            public boolean isInstalled(String streamName) {
                return started.contains(streamName);
            }

            @Override
            public Object installedGeneration(String streamName) {
                return started.contains(streamName) ? streamName : null;
            }

            @Override
            public void onTerminalGiveUp(String streamName) {
            }
        };
        restarter = new StreamRestarter("restart-test", callbacks);
        try {
            restarter.onRuntimeDeath("slow", new RuntimeException("cursor died"));
            assertThat(slowEntered.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();

            // The slow start (a reconciliation, say) is still blocked: the
            // other stream's attempt must run regardless.
            restarter.onRuntimeDeath("quick", new RuntimeException("cursor died"));
            await().atMost(Duration.ofSeconds(5)).until(() -> started.contains("quick"));
            assertThat(started).doesNotContain("slow");
        } finally {
            slowGate.countDown();
        }
        await().atMost(Duration.ofSeconds(5)).until(() -> started.contains("slow"));
    }

    @Test
    void cancelDuringInFlightAttempt_attemptStandsDown_neverEmitsNorRetries() throws Exception {
        // Round 2 contract: the attempt detects the cancellation after
        // startStream and stands down — no emission, no retry. It does NOT
        // stop the stream itself: the operator stop that triggered the
        // cancel is serialized behind the attempt by the manager's lifecycle
        // lock and performs the actual teardown (a by-name rollback from the
        // restarter could stop a newer manual generation).
        RecordingCallbacks callbacks = new RecordingCallbacks();
        callbacks.startEntered = new java.util.concurrent.CountDownLatch(1);
        callbacks.startGate = new java.util.concurrent.CountDownLatch(1);
        restarter(callbacks);

        restarter.onRuntimeDeath(STREAM, new RuntimeException("cursor died"));
        assertThat(callbacks.startEntered.await(5, java.util.concurrent.TimeUnit.SECONDS))
                .isTrue();

        // The attempt is in flight inside startStream: cancel now.
        restarter.cancel(STREAM);
        callbacks.startGate.countDown();

        Thread.sleep(1_500);
        assertThat(metrics.restarts)
                .as("a cancelled attempt must not report a restart")
                .isEmpty();
        assertThat(restarter.isRestartPending(STREAM)).isFalse();
        assertThat(callbacks.startCalls.get()).isEqualTo(1);
    }

    @Test
    void lateDeathAfterCancel_freshGeneration_oldAttemptCannotInterfere() throws Exception {
        // Round 2 blocker (generation ABA): with a per-state counter, a state
        // re-created after cancel() restarted at the same generation an
        // in-flight attempt had captured — the old attempt could then act on
        // the new state. Generations are now globally monotonic and never
        // reused: the old attempt observes a mismatch and stands down, while
        // the fresh state's own attempt runs its lifecycle untouched.
        RecordingCallbacks callbacks = new RecordingCallbacks();
        callbacks.startEntered = new java.util.concurrent.CountDownLatch(1);
        callbacks.startGate = new java.util.concurrent.CountDownLatch(1);
        // The old attempt's startStream fails transiently (its subscription
        // never took); the fresh generation's attempt succeeds.
        callbacks.failStartsUntilAttempt = 2;
        restarter(callbacks);

        restarter.onRuntimeDeath(STREAM, new RuntimeException("first death"));
        assertThat(callbacks.startEntered.await(5, java.util.concurrent.TimeUnit.SECONDS))
                .isTrue();

        // Operator stop invalidates the in-flight attempt, then a legitimate
        // death of a NEWER manager generation re-arms the lifecycle.
        restarter.cancel(STREAM);
        restarter.onRuntimeDeath(STREAM, new RuntimeException("newer generation death"));
        callbacks.startGate.countDown();

        // The old attempt stands down; the fresh state's attempt runs and
        // completes ITS lifecycle (attempt 1 of the new generation).
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(metrics.restarts)
                        .containsExactly(STREAM + ":1:newer generation death"));
        assertThat(restarter.isRestartPending(STREAM)).isFalse();
    }

    @Test
    void secondDeathBeforePendingAttempt_replacesTheFuture_neverOrphansIt() throws Exception {
        // Review round 1 blocker: two death notifications before the first
        // attempt must leave exactly ONE valid, cancellable future — never an
        // orphaned handle that could fire after a stop or a success.
        RecordingCallbacks callbacks = new RecordingCallbacks();
        restarter(callbacks);

        restarter.onRuntimeDeath(STREAM, new RuntimeException("first death"));
        restarter.onRuntimeDeath(STREAM, new RuntimeException("second death"));

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(metrics.restarts)
                        .as("one attempt runs, carrying the accumulated count and latest cause")
                        .containsExactly(STREAM + ":2:second death"));
        Thread.sleep(2_000);
        assertThat(callbacks.startCalls.get())
                .as("the replaced future must not fire a second attempt")
                .isEqualTo(1);
    }

    // --- the cycle ends on the proof of life, not on the return of startStream ---

    @Test
    void startReturn_endsNothing_deathBeforeProofContinuesTheAttemptCount() {
        // A refused open: startStream returns normally (the cursor opens
        // asynchronously), then the manager reports the death. The attempt
        // count must go on — attempt 2 after initialDelay * multiplier —
        // instead of a fresh attempt 1 at initialDelay forever.
        RecordingCallbacks callbacks = new RecordingCallbacks();
        callbacks.policy = FAST_UNBOUNDED; // 100ms, 200ms, 400ms …
        callbacks.proveOnInstall = false;
        restarter(callbacks);

        restarter.onRuntimeDeath(STREAM, new RuntimeException("open refused"));
        await().atMost(Duration.ofSeconds(5))
                .until(() -> restarter.isAwaitingProof(STREAM));
        assertThat(callbacks.startCalls.get()).isEqualTo(1);
        assertThat(restarter.isRestartPending(STREAM))
                .as("the stream is installed — no restart pending, the proof is awaited")
                .isFalse();
        assertThat(metrics.restarts)
                .as("the return of startStream is not a resubscription")
                .isEmpty();

        callbacks.installed.set(false); // the manager evicted the dead generation
        long death = System.nanoTime();
        restarter.onRuntimeDeath(STREAM, new RuntimeException("open refused"));
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(callbacks.startCalls.get()).isEqualTo(2));
        assertThat(Duration.ofNanos(callbacks.startTimes.get(1) - death))
                .as("attempt 2 waits initialDelay * multiplier, not initialDelay")
                .isGreaterThanOrEqualTo(Duration.ofMillis(190));

        callbacks.installed.set(false);
        death = System.nanoTime();
        restarter.onRuntimeDeath(STREAM, new RuntimeException("open refused"));
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(callbacks.startCalls.get()).isEqualTo(3));
        assertThat(Duration.ofNanos(callbacks.startTimes.get(2) - death))
                .as("attempt 3 waits initialDelay * multiplier^2")
                .isGreaterThanOrEqualTo(Duration.ofMillis(390));
        assertThat(metrics.restarts).isEmpty();
        assertThat(callbacks.terminalGiveUps).as("unbounded policy").isEmpty();
    }

    @Test
    void boundedPolicy_exhaustedByUnprovenGenerations_givesUp_neverEmitsRestarted() {
        // Every start "succeeds" synchronously and dies asynchronously: a
        // bounded policy must still reach its ceiling and release the lease.
        RecordingCallbacks callbacks = new RecordingCallbacks();
        callbacks.policy = new RestartPolicyConfig(2, Duration.ofMillis(100), Duration.ofSeconds(1), 2.0, false);
        callbacks.proveOnInstall = false;
        restarter(callbacks);
        RuntimeException death = new RuntimeException("open refused");

        for (int i = 1; i <= 2; i++) {
            callbacks.installed.set(false);
            restarter.onRuntimeDeath(STREAM, death);
            await().atMost(Duration.ofSeconds(5))
                    .until(() -> restarter.isAwaitingProof(STREAM));
        }
        callbacks.installed.set(false);
        restarter.onRuntimeDeath(STREAM, death); // attempt 3 > maxAttempts

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(callbacks.terminalGiveUps).containsExactly(STREAM));
        assertThat(callbacks.startCalls.get())
                .as("exactly maxAttempts resubscriptions were tried")
                .isEqualTo(2);
        assertThat(restarter.isRestartPending(STREAM)).isFalse();
        assertThat(restarter.isAwaitingProof(STREAM)).isFalse();
        assertThat(metrics.stops).containsExactly(STREAM + ":CRASHED:RuntimeException");
        assertThat(metrics.restarts)
                .as("a generation that never proved itself is never reported as restarted")
                .isEmpty();
    }

    @Test
    void proofAfterReturn_endsTheCycle_emitsRestarted_nextDeathIsAttemptOne() {
        RecordingCallbacks callbacks = new RecordingCallbacks();
        callbacks.policy = FAST_UNBOUNDED;
        callbacks.proveOnInstall = false;
        restarter(callbacks);

        restarter.onRuntimeDeath(STREAM, new RuntimeException("first death"));
        await().atMost(Duration.ofSeconds(5)).until(() -> restarter.isAwaitingProof(STREAM));
        callbacks.installed.set(false);
        restarter.onRuntimeDeath(STREAM, new RuntimeException("first death")); // unproven: attempt 2
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(callbacks.startCalls.get()).isEqualTo(2));
        await().atMost(Duration.ofSeconds(5)).until(() -> restarter.isAwaitingProof(STREAM));
        assertThat(metrics.restarts).isEmpty();

        // The manager reports the cursor alive: the signal carries the
        // attempt that succeeded, the state is gone.
        restarter.onGenerationProven(STREAM, callbacks.generation);
        assertThat(metrics.restarts).containsExactly(STREAM + ":2:first death");
        assertThat(restarter.isAwaitingProof(STREAM)).isFalse();
        assertThat(restarter.isRestartPending(STREAM)).isFalse();

        // A later death is a new incident: attempt 1 again.
        callbacks.installed.set(false);
        restarter.onRuntimeDeath(STREAM, new RuntimeException("second death"));
        await().atMost(Duration.ofSeconds(5)).until(() -> restarter.isAwaitingProof(STREAM));
        restarter.onGenerationProven(STREAM, callbacks.generation);
        assertThat(metrics.restarts)
                .containsExactly(STREAM + ":2:first death", STREAM + ":1:second death");
        assertThat(callbacks.terminalGiveUps).isEmpty();
    }

    @Test
    void proofDuringInFlightStart_isHonouredWhenTheAttemptCommits() throws Exception {
        // An event delivered on the reading thread before startStream
        // returned: the proof lands while the attempt is in flight and must
        // end the cycle as soon as the attempt commits — never be lost.
        RecordingCallbacks callbacks = new RecordingCallbacks();
        callbacks.policy = FAST_UNBOUNDED;
        callbacks.proveOnInstall = false;
        callbacks.startEntered = new java.util.concurrent.CountDownLatch(1);
        callbacks.startGate = new java.util.concurrent.CountDownLatch(1);
        restarter(callbacks);

        restarter.onRuntimeDeath(STREAM, new RuntimeException("cursor died"));
        assertThat(callbacks.startEntered.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        restarter.onGenerationProven(STREAM, callbacks.generation); // in flight, this generation
        assertThat(metrics.restarts).as("not committed yet").isEmpty();

        callbacks.startGate.countDown();
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(metrics.restarts).containsExactly(STREAM + ":1:cursor died"));
        assertThat(restarter.isAwaitingProof(STREAM)).isFalse();
        assertThat(restarter.isRestartPending(STREAM)).isFalse();
    }

    @Test
    void strayProofWhileAnAttemptIsPending_isIgnored() {
        RecordingCallbacks callbacks = new RecordingCallbacks();
        callbacks.policy = new RestartPolicyConfig(0, Duration.ofSeconds(30), Duration.ofSeconds(30), 2.0, false);
        restarter(callbacks);

        restarter.onRuntimeDeath(STREAM, new RuntimeException("cursor died"));
        restarter.onGenerationProven(STREAM, new Object()); // a late signal of the dead generation

        assertThat(restarter.isRestartPending(STREAM))
                .as("the pending attempt still owns the next step")
                .isTrue();
        assertThat(metrics.restarts).isEmpty();
        assertThat(callbacks.startCalls.get()).isZero();
    }

    @Test
    void proofOutsideARestartCycle_isNoOp() {
        RecordingCallbacks callbacks = new RecordingCallbacks();
        restarter(callbacks);

        restarter.onGenerationProven(STREAM, new Object()); // boot start, manual start: no cycle

        assertThat(metrics.restarts).isEmpty();
        assertThat(restarter.isRestartPending(STREAM)).isFalse();
        assertThat(restarter.isAwaitingProof(STREAM)).isFalse();
    }

    @Test
    void installedGeneration_outlivingMaxDelay_countsAsAlive() {
        // Reactive streams prove themselves on their first event only: an
        // idle resubscribed generation that outlives the policy's maxDelay
        // without dying is counted as alive — the restart is reported and
        // the next death starts a fresh cycle.
        RecordingCallbacks callbacks = new RecordingCallbacks();
        callbacks.policy = new RestartPolicyConfig(0, Duration.ofMillis(100), Duration.ofMillis(400), 2.0, false);
        callbacks.proveOnInstall = false;
        restarter(callbacks);

        restarter.onRuntimeDeath(STREAM, new RuntimeException("first death"));
        await().atMost(Duration.ofSeconds(5)).until(() -> restarter.isAwaitingProof(STREAM));
        callbacks.installed.set(false);
        restarter.onRuntimeDeath(STREAM, new RuntimeException("first death")); // attempt 2
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(callbacks.startCalls.get()).isEqualTo(2));
        await().atMost(Duration.ofSeconds(5)).until(() -> restarter.isAwaitingProof(STREAM));
        long installedAt = callbacks.startTimes.get(1); // the install follows the start's return
        assertThat(metrics.restarts).isEmpty();

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(metrics.restarts).containsExactly(STREAM + ":2:first death"));
        assertThat(Duration.ofNanos(System.nanoTime() - installedAt))
                .as("not before maxDelay")
                .isGreaterThanOrEqualTo(Duration.ofMillis(390));
        assertThat(restarter.isAwaitingProof(STREAM)).isFalse();

        callbacks.installed.set(false);
        restarter.onRuntimeDeath(STREAM, new RuntimeException("second death"));
        await().atMost(Duration.ofSeconds(5)).until(() -> restarter.isAwaitingProof(STREAM));
        restarter.onGenerationProven(STREAM, callbacks.generation);
        assertThat(metrics.restarts)
                .containsExactly(STREAM + ":2:first death", STREAM + ":1:second death");
    }

    @Test
    void deathBeforeMaxDelay_cancelsTheAgeProof() throws Exception {
        RecordingCallbacks callbacks = new RecordingCallbacks();
        callbacks.policy = new RestartPolicyConfig(0, Duration.ofMillis(100), Duration.ofMillis(300), 2.0, false);
        callbacks.proveOnInstall = false;
        restarter(callbacks);

        restarter.onRuntimeDeath(STREAM, new RuntimeException("open refused"));
        await().atMost(Duration.ofSeconds(5)).until(() -> restarter.isAwaitingProof(STREAM));
        callbacks.installed.set(false);
        restarter.onRuntimeDeath(STREAM, new RuntimeException("open refused")); // within maxDelay
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(callbacks.startCalls.get()).isEqualTo(2));

        // Past both would-be age proofs: the dead generation's timer was
        // cancelled by its death (it would have reported attempt 1); only
        // the installed generation's fires, with the continued count.
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(metrics.restarts).isNotEmpty());
        Thread.sleep(400);
        assertThat(metrics.restarts)
                .as("one age proof, for the living generation, carrying attempt 2")
                .containsExactly(STREAM + ":2:open refused");
    }

    @Test
    void cancelWhileAwaitingProof_clearsTheState() {
        RecordingCallbacks callbacks = new RecordingCallbacks();
        callbacks.policy = FAST_UNBOUNDED;
        callbacks.proveOnInstall = false;
        restarter(callbacks);

        restarter.onRuntimeDeath(STREAM, new RuntimeException("cursor died"));
        await().atMost(Duration.ofSeconds(5)).until(() -> restarter.isAwaitingProof(STREAM));

        Object stopped = callbacks.generation;
        restarter.cancel(STREAM); // operator stop
        assertThat(restarter.isAwaitingProof(STREAM)).isFalse();
        restarter.onGenerationProven(STREAM, stopped); // late, after the stop
        assertThat(metrics.restarts).isEmpty();
    }

    // --- the proof is bound to its generation ---

    @Test
    void staleProofOfAReplacedGeneration_whileTheNextIsInstalledUnproven_isIgnored() {
        // Generation A is proven; its manager thread passes its lock-free
        // fence and is suspended before reaching the restarter. Meanwhile an
        // operator stops A (cycle cancelled), a later start dies, and the
        // new cycle's attempt installs B, unproven. A's late proof must not
        // end B's cycle: B stays awaiting proof, nothing is reported, and
        // B's death continues B's count.
        RecordingCallbacks callbacks = new RecordingCallbacks();
        callbacks.policy = FAST_UNBOUNDED;
        callbacks.proveOnInstall = false;
        restarter(callbacks);

        restarter.onRuntimeDeath(STREAM, new RuntimeException("first death"));
        await().atMost(Duration.ofSeconds(5)).until(() -> restarter.isAwaitingProof(STREAM));
        Object generationA = callbacks.generation;

        restarter.cancel(STREAM); // operator stop of A
        callbacks.installed.set(false);
        restarter.onRuntimeDeath(STREAM, new RuntimeException("later death")); // new cycle
        await().atMost(Duration.ofSeconds(5)).until(() -> restarter.isAwaitingProof(STREAM));
        assertThat(callbacks.generation).isNotSameAs(generationA);

        restarter.onGenerationProven(STREAM, generationA); // A's thread resumes
        assertThat(restarter.isAwaitingProof(STREAM))
                .as("B's cycle is untouched by a proof about A")
                .isTrue();
        assertThat(metrics.restarts).isEmpty();

        callbacks.installed.set(false);
        long death = System.nanoTime();
        restarter.onRuntimeDeath(STREAM, new RuntimeException("later death")); // B dies unproven
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(callbacks.startCalls.get()).isEqualTo(3));
        assertThat(Duration.ofNanos(callbacks.startTimes.get(2) - death))
                .as("B's death is attempt 2 of B's cycle, not a fresh attempt 1")
                .isGreaterThanOrEqualTo(Duration.ofMillis(190));
        await().atMost(Duration.ofSeconds(5)).until(() -> restarter.isAwaitingProof(STREAM));
        restarter.onGenerationProven(STREAM, callbacks.generation); // the real proof
        assertThat(metrics.restarts).containsExactly(STREAM + ":2:later death");
    }

    @Test
    void staleProofWhileTheNextAttemptIsInFlight_isNotHonoured_itsOwnProofIs() throws Exception {
        // Same race, with B's startStream still in flight when A's late
        // proof lands: it must not be remembered as B's proof. B commits
        // unproven; B's own proof then ends the cycle.
        RecordingCallbacks callbacks = new RecordingCallbacks();
        callbacks.policy = FAST_UNBOUNDED;
        callbacks.proveOnInstall = false;
        restarter(callbacks);

        restarter.onRuntimeDeath(STREAM, new RuntimeException("first death"));
        await().atMost(Duration.ofSeconds(5)).until(() -> restarter.isAwaitingProof(STREAM));
        Object generationA = callbacks.generation;

        restarter.cancel(STREAM); // operator stop of A
        callbacks.installed.set(false);
        callbacks.startEntered = new java.util.concurrent.CountDownLatch(1);
        callbacks.startGate = new java.util.concurrent.CountDownLatch(1);
        restarter.onRuntimeDeath(STREAM, new RuntimeException("later death")); // new cycle
        assertThat(callbacks.startEntered.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();

        restarter.onGenerationProven(STREAM, generationA); // A's late proof, B in flight
        callbacks.startGate.countDown();
        await().atMost(Duration.ofSeconds(5)).until(() -> restarter.isAwaitingProof(STREAM));
        assertThat(metrics.restarts)
                .as("B committed unproven: A's proof was about another generation")
                .isEmpty();

        restarter.onGenerationProven(STREAM, callbacks.generation); // B's own proof
        assertThat(metrics.restarts).containsExactly(STREAM + ":1:later death");
        assertThat(restarter.isAwaitingProof(STREAM)).isFalse();
    }

    @Test
    void staleProofAfterTheInFlightProof_cannotOverwriteIt() throws Exception {
        // B delivers before its startStream returns (its proof is recorded
        // in flight — and the manager's CAS will never resend it), THEN A's
        // late proof lands, still before the commit. A stale proof must not
        // overwrite B's: B's restart is reported when the start completes.
        RecordingCallbacks callbacks = new RecordingCallbacks();
        callbacks.policy = FAST_UNBOUNDED;
        callbacks.proveOnInstall = false;
        restarter(callbacks);

        restarter.onRuntimeDeath(STREAM, new RuntimeException("first death"));
        await().atMost(Duration.ofSeconds(5)).until(() -> restarter.isAwaitingProof(STREAM));
        Object generationA = callbacks.generation;

        restarter.cancel(STREAM); // operator stop of A
        callbacks.installed.set(false);
        callbacks.startEntered = new java.util.concurrent.CountDownLatch(1);
        callbacks.startGate = new java.util.concurrent.CountDownLatch(1);
        restarter.onRuntimeDeath(STREAM, new RuntimeException("later death")); // new cycle
        assertThat(callbacks.startEntered.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();

        restarter.onGenerationProven(STREAM, callbacks.generation); // B delivers, in flight
        restarter.onGenerationProven(STREAM, generationA); // A's stale proof, after it
        callbacks.startGate.countDown();

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(metrics.restarts).containsExactly(STREAM + ":1:later death"));
        assertThat(restarter.isAwaitingProof(STREAM))
                .as("B's cycle is over — no state left awaiting a proof that will never be resent")
                .isFalse();
        assertThat(restarter.isRestartPending(STREAM)).isFalse();

        // B's next death is a new incident: attempt 1.
        callbacks.installed.set(false);
        restarter.onRuntimeDeath(STREAM, new RuntimeException("next death"));
        await().atMost(Duration.ofSeconds(5)).until(() -> restarter.isAwaitingProof(STREAM));
        restarter.onGenerationProven(STREAM, callbacks.generation);
        assertThat(metrics.restarts)
                .containsExactly(STREAM + ":1:later death", STREAM + ":1:next death");
    }

    @Test
    void staleProofBeforeTheInFlightProof_isNotKeptEither() throws Exception {
        // Same two proofs, the other way round: A's stale proof first, then
        // B's real one, both before the commit. The order must not matter —
        // the stale one is never recorded, the real one always is.
        RecordingCallbacks callbacks = new RecordingCallbacks();
        callbacks.policy = FAST_UNBOUNDED;
        callbacks.proveOnInstall = false;
        restarter(callbacks);

        restarter.onRuntimeDeath(STREAM, new RuntimeException("first death"));
        await().atMost(Duration.ofSeconds(5)).until(() -> restarter.isAwaitingProof(STREAM));
        Object generationA = callbacks.generation;

        restarter.cancel(STREAM); // operator stop of A
        callbacks.installed.set(false);
        callbacks.startEntered = new java.util.concurrent.CountDownLatch(1);
        callbacks.startGate = new java.util.concurrent.CountDownLatch(1);
        restarter.onRuntimeDeath(STREAM, new RuntimeException("later death")); // new cycle
        assertThat(callbacks.startEntered.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();

        restarter.onGenerationProven(STREAM, generationA); // A's stale proof first
        restarter.onGenerationProven(STREAM, callbacks.generation); // then B delivers, in flight
        callbacks.startGate.countDown();

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(metrics.restarts).containsExactly(STREAM + ":1:later death"));
        assertThat(restarter.isAwaitingProof(STREAM)).isFalse();
        assertThat(restarter.isRestartPending(STREAM)).isFalse();
    }

    @Test
    void staleProofWhileNothingIsInstalledYet_isIgnored() throws Exception {
        // The attempt is in flight but has not installed anything (the
        // manager reports no generation): a proof carrying any identity is
        // a stray and must not be recorded against this attempt.
        RecordingCallbacks callbacks = new RecordingCallbacks();
        callbacks.policy = FAST_UNBOUNDED;
        callbacks.proveOnInstall = false;
        callbacks.failStartsUntilAttempt = 2; // attempt 1 installs nothing and fails
        callbacks.startEntered = new java.util.concurrent.CountDownLatch(1);
        callbacks.startGate = new java.util.concurrent.CountDownLatch(1);
        restarter(callbacks);

        restarter.onRuntimeDeath(STREAM, new RuntimeException("cursor died"));
        assertThat(callbacks.startEntered.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        assertThat(callbacks.installedGeneration(STREAM)).isNull();
        restarter.onGenerationProven(STREAM, new Object()); // stray, nothing installed
        callbacks.startGate.countDown();

        // Attempt 1 fails; attempt 2 installs — and must be awaiting ITS
        // proof, not already proven by the stray.
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(callbacks.startCalls.get()).isEqualTo(2));
        await().atMost(Duration.ofSeconds(5)).until(() -> restarter.isAwaitingProof(STREAM));
        assertThat(metrics.restarts).isEmpty();

        restarter.onGenerationProven(STREAM, callbacks.generation);
        assertThat(metrics.restarts).containsExactly(STREAM + ":2:cursor died");
    }

    @Test
    void cancel_preventsTheScheduledAttempt() throws Exception {
        RecordingCallbacks callbacks = new RecordingCallbacks();
        restarter(callbacks);

        restarter.onRuntimeDeath(STREAM, new RuntimeException("cursor died"));
        assertThat(restarter.isRestartPending(STREAM)).isTrue();
        restarter.cancel(STREAM);

        Thread.sleep(2_500); // past the attempt-1 delay
        assertThat(callbacks.startCalls.get()).isZero();
        assertThat(restarter.isRestartPending(STREAM)).isFalse();
    }

    private static final class RecordingCallbacks implements StreamRestarter.Callbacks {
        final AtomicInteger startCalls = new AtomicInteger();
        final AtomicBoolean installed = new AtomicBoolean(false);
        final List<String> terminalGiveUps = new CopyOnWriteArrayList<>();
        final List<String> policyLookups = new CopyOnWriteArrayList<>();
        /** Nanotime of each {@code startStream} call, in order. */
        final List<Long> startTimes = new CopyOnWriteArrayList<>();
        volatile RestartPolicyConfig policy = RestartPolicyConfig.DEFAULTS;
        /**
         * Whether an installing start proves its generation before returning
         * (an event delivered on the reading thread during the start). Off,
         * the test reports the proof itself — or never does.
         */
        volatile boolean proveOnInstall = true;
        volatile StreamRestarter restarter;
        /**
         * Identity of the installed generation — what a manager's state
         * object is to the restarter. A manager registers its state inside
         * the start, before the subscription opens, so an installing start
         * sets it on entry: an event delivered before the return already
         * carries it, and {@link #installedGeneration} already returns it.
         */
        volatile Object generation;
        volatile RuntimeException startFailure;
        volatile int failStartsUntilAttempt;
        volatile java.util.concurrent.CountDownLatch startEntered;
        volatile java.util.concurrent.CountDownLatch startGate;

        @Override
        public RestartPolicyConfig policyFor(String streamName) {
            policyLookups.add(streamName);
            return policy;
        }

        @Override
        public void startStream(String streamName) {
            startTimes.add(System.nanoTime());
            int call = startCalls.incrementAndGet();
            boolean willFail = startFailure != null || call < failStartsUntilAttempt;
            if (!willFail) {
                // Installed on entry, like a manager registering its state
                // before the subscription opens: the generation exists — and
                // can deliver — while the start is still in flight.
                generation = new Object();
                installed.set(true);
            }
            if (startEntered != null) {
                startEntered.countDown();
                try {
                    startGate.await(10, java.util.concurrent.TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (startFailure != null) {
                throw startFailure;
            }
            if (call < failStartsUntilAttempt) {
                throw new RuntimeException("server still down");
            }
            if (proveOnInstall && restarter != null) {
                restarter.onGenerationProven(streamName, generation);
            }
        }

        @Override
        public boolean isInstalled(String streamName) {
            return installed.get();
        }

        @Override
        public Object installedGeneration(String streamName) {
            return installed.get() ? generation : null;
        }

        @Override
        public void onTerminalGiveUp(String streamName) {
            terminalGiveUps.add(streamName);
        }
    }

    private static final class RecordingMetrics implements StreamMetricsProvider {
        final List<String> stops = new CopyOnWriteArrayList<>();
        final List<String> restarts = new CopyOnWriteArrayList<>();

        @Override
        public void onStreamStopped(String streamName, StopReason reason, Throwable cause) {
            stops.add(streamName + ":" + reason + ":"
                    + (cause != null ? cause.getClass().getSimpleName() : "null"));
        }

        @Override
        public void onStreamRestarted(String streamName, int attempt, Throwable cause) {
            restarts.add(streamName + ":" + attempt + ":"
                    + (cause != null ? cause.getMessage() : "null"));
        }

        @Override
        public void onStreamStarted(String streamName,
                io.flowwarden.stream.spi.StreamConfiguration config) {
        }

        @Override
        public void onEventReceived(String streamName,
                io.flowwarden.stream.spi.ChangeEventMetadata metadata) {
        }

        @Override
        public void onEventProcessed(String streamName, long durationNanos, boolean success) {
        }

        @Override
        public void onEventError(String streamName, Throwable error, boolean willRetry,
                int attemptNumber, io.flowwarden.stream.spi.ChangeEventMetadata metadata) {
        }

        @Override
        public void onCheckpoint(String streamName, String resumeToken) {
        }

        @Override
        public void onBufferStatus(String streamName, int currentSize, int maxSize) {
        }

        @Override
        public void onBackpressure(String streamName,
                io.flowwarden.stream.spi.BackpressureAction action) {
        }

        @Override
        public void onEventSentToDlq(String streamName) {
        }

        @Override
        public void onOplogStats(double logLengthHours, String status) {
        }
    }
}
