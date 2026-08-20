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
 * Unit tests for the managed resubscription loop: capped exponential
 * backoff, indefinite retry on transient failures, terminal give-up on
 * {@link HistoryLostException}, stand-down when the stream was restarted
 * manually, and cancellation by a graceful stop.
 */
class StreamRestarterTest {

    private static final String STREAM = "restart-test";

    private RecordingMetrics metrics;
    private StreamRestarter restarter;

    @BeforeEach
    void setUp() {
        metrics = new RecordingMetrics();
        FlowWardenMetrics.setProvider(metrics);
    }

    @AfterEach
    void tearDown() {
        if (restarter != null) {
            restarter.shutdown();
        }
        FlowWardenMetrics.setProvider(StreamMetricsProvider.noOp());
    }

    @Test
    void backoff_isExponentialAndCapped() {
        assertThat(StreamRestarter.delaySeconds(1)).isEqualTo(1);
        assertThat(StreamRestarter.delaySeconds(2)).isEqualTo(2);
        assertThat(StreamRestarter.delaySeconds(3)).isEqualTo(4);
        assertThat(StreamRestarter.delaySeconds(6)).isEqualTo(32);
        assertThat(StreamRestarter.delaySeconds(7)).isEqualTo(60);
        assertThat(StreamRestarter.delaySeconds(50)).isEqualTo(60);
    }

    @Test
    void runtimeDeath_restartsTheStream_emitsRestartSignalWithAttemptAndCause() {
        RecordingCallbacks callbacks = new RecordingCallbacks();
        restarter = new StreamRestarter("fw-restart-test", callbacks);
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
        restarter = new StreamRestarter("fw-restart-test", callbacks);

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
        restarter = new StreamRestarter("fw-restart-test", callbacks);

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
        restarter = new StreamRestarter("fw-restart-test", callbacks);

        restarter.onRuntimeDeath(STREAM, new RuntimeException("cursor died"));

        await().atMost(Duration.ofSeconds(5))
                .until(() -> !restarter.isRestartPending(STREAM));
        assertThat(callbacks.startCalls.get())
                .as("the installed stream wins — the loop stands down")
                .isZero();
        assertThat(metrics.restarts).isEmpty();
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
        restarter = new StreamRestarter("fw-restart-test", callbacks);

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
        restarter = new StreamRestarter("fw-restart-test", callbacks);

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
        restarter = new StreamRestarter("fw-restart-test", callbacks);

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

    @Test
    void cancel_preventsTheScheduledAttempt() throws Exception {
        RecordingCallbacks callbacks = new RecordingCallbacks();
        restarter = new StreamRestarter("fw-restart-test", callbacks);

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
        volatile RuntimeException startFailure;
        volatile int failStartsUntilAttempt;
        volatile java.util.concurrent.CountDownLatch startEntered;
        volatile java.util.concurrent.CountDownLatch startGate;

        @Override
        public void startStream(String streamName) {
            int call = startCalls.incrementAndGet();
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
            installed.set(true);
        }

        @Override
        public boolean isInstalled(String streamName) {
            return installed.get();
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
