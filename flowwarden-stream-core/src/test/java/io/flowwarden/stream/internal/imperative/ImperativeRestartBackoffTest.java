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
package io.flowwarden.stream.internal.imperative;

import com.mongodb.MongoException;
import io.flowwarden.stream.DeploymentMode;
import io.flowwarden.stream.FlowWardenMetrics;
import io.flowwarden.stream.FullDocumentBeforeChangeMode;
import io.flowwarden.stream.FullDocumentMode;
import io.flowwarden.stream.annotation.RestartPolicy;
import io.flowwarden.stream.internal.MongoTemplateRegistry;
import io.flowwarden.stream.internal.checkpoint.HeartbeatProbe;
import io.flowwarden.stream.internal.checkpoint.ProbeOutcome;
import io.flowwarden.stream.internal.discovery.ChangeStreamDefinition;
import io.flowwarden.stream.internal.discovery.ErrorHandlerResolver;
import io.flowwarden.stream.internal.discovery.StreamConfig;
import io.flowwarden.stream.internal.discovery.StreamRegistry;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.DlqStore;
import io.flowwarden.stream.spi.StreamMetricsProvider;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.messaging.MessageListenerContainer;
import org.springframework.data.mongodb.core.messaging.Subscription;
import org.springframework.util.ErrorHandler;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Manager-level harness for the restart loop's backoff across generations
 * whose open is refused (imperative), with the main cursor scripted through
 * the injected container exactly as in the refused-open log-throttling
 * harness. The synchronous return of {@code startStream} must not reset
 * the attempt count: three consecutive refused opens are attempts 1, 2 and
 * 3 of one cycle — the delays grow on the policy's cadence and no restart
 * is reported — and only the reading task's completed start (the
 * generation's proof of life) ends the cycle and reports the restart.
 */
class ImperativeRestartBackoffTest {

    private static final String STREAM = "restart-backoff-imperative";
    private static final BsonDocument SEED = BsonDocument.parse("{\"_data\": \"seed\"}");
    private static final BsonDocument PBRT = BsonDocument.parse("{\"_data\": \"pbrt\"}");

    /** One scripted generation: when it registered, the start latch of its reading task, its error handler. */
    private record Generation(long registeredAt, AtomicBoolean started, ErrorHandler errorHandler) {
    }

    private final List<Generation> generations = new CopyOnWriteArrayList<>();
    private final AtomicInteger probes = new AtomicInteger();
    private final List<String> restarts = new CopyOnWriteArrayList<>();
    private ImperativeStreamManager manager;

    @BeforeEach
    void setUp() {
        FlowWardenMetrics.setProvider(new RestartRecorder());
    }

    @AfterEach
    void tearDown() {
        if (manager != null) {
            manager.shutdown();
        }
        FlowWardenMetrics.setProvider(StreamMetricsProvider.noOp());
    }

    @Test
    void refusedOpens_continueTheAttemptCount_delaysGrow_noRestartReported() {
        manager = manager();
        manager.startStream(STREAM);

        // Policy: 200ms × 3 — attempts 1, 2, 3 wait 200ms, 600ms, 1800ms.
        // Each generation's start returns normally; its open is refused
        // afterwards. Measured from the death to the next registration.
        long death1 = refuseOpenAfterCertifyingProbe(1);
        awaitGeneration(2);
        assertThat(sinceDeath(death1, 2))
                .as("attempt 1 waits initialDelay")
                .isGreaterThanOrEqualTo(Duration.ofMillis(190));
        assertThat(manager.isAwaitingProof(STREAM))
                .as("the installed generation's proof of life is awaited — the cycle is open")
                .isTrue();
        assertThat(manager.isRestartPending(STREAM)).isFalse();

        long death2 = refuseOpenAfterCertifyingProbe(2);
        awaitGeneration(3);
        assertThat(sinceDeath(death2, 3))
                .as("attempt 2 waits initialDelay × multiplier — the return of attempt 1 reset nothing")
                .isGreaterThanOrEqualTo(Duration.ofMillis(590));

        long death3 = refuseOpenAfterCertifyingProbe(3);
        awaitGeneration(4);
        assertThat(sinceDeath(death3, 4))
                .as("attempt 3 waits initialDelay × multiplier²")
                .isGreaterThanOrEqualTo(Duration.ofMillis(1_790));

        assertThat(restarts)
                .as("a generation that never proved itself is never reported as a restart")
                .isEmpty();
    }

    @Test
    void provenGeneration_reportsTheRestart_andResetsTheCount() {
        manager = manager();
        manager.startStream(STREAM);

        refuseOpenAfterCertifyingProbe(1);
        refuseOpenAfterCertifyingProbe(2); // attempts 1 and 2 of the cycle
        awaitGeneration(3);
        assertThat(restarts).isEmpty();

        // The third generation's reading task completes its start: the
        // proof of life. The restart is reported with the attempt that
        // succeeded and the cycle is over.
        cursorOpens(3);
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(restarts).containsExactly(STREAM + ":2"));
        assertThat(manager.isAwaitingProof(STREAM)).isFalse();
        assertThat(manager.isRestartPending(STREAM)).isFalse();

        // Its death is a new incident: attempt 1 again, at initialDelay.
        long death = die(3);
        awaitGeneration(4);
        assertThat(sinceDeath(death, 4)).isGreaterThanOrEqualTo(Duration.ofMillis(190));
        cursorOpens(4);
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(restarts).containsExactly(STREAM + ":2", STREAM + ":1"));
    }

    // --- scripted generations ---

    private static MongoException refusedOpen() {
        return new MongoException(286, "ChangeStreamHistoryLost: resume point may no longer be in the oplog");
    }

    private void awaitGeneration(int n) {
        await().atMost(Duration.ofSeconds(15)).until(() -> generations.size() >= n);
    }

    private Duration sinceDeath(long deathNanos, int generation) {
        return Duration.ofNanos(generations.get(generation - 1).registeredAt() - deathNanos);
    }

    /** Waits for generation {@code n} to register and its probe to certify, then refuses its open. */
    private long refuseOpenAfterCertifyingProbe(int n) {
        awaitGeneration(n);
        int before = probes.get();
        await().atMost(Duration.ofSeconds(10)).until(() -> probes.get() > before);
        return die(n);
    }

    /** Generation {@code n}'s reading task completes its start (cursor open); waits for the proof. */
    private void cursorOpens(int n) {
        awaitGeneration(n);
        generations.get(n - 1).started().set(true);
        await().atMost(Duration.ofSeconds(10)).until(() -> manager.isGenerationProven(STREAM));
    }

    /** The reading task of generation {@code n} reports a refused open / dead cursor; returns the death time. */
    private long die(int n) {
        long at = System.nanoTime();
        generations.get(n - 1).errorHandler().handleError(refusedOpen());
        return at;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private ImperativeStreamManager manager() {
        MongoTemplate template = mock(MongoTemplate.class);
        MongoTemplateRegistry templateRegistry = mock(MongoTemplateRegistry.class);
        when(templateRegistry.getDefaultTemplate()).thenReturn(template);
        when(templateRegistry.resolve(anyString())).thenReturn(template);
        StreamRegistry registry = mock(StreamRegistry.class);
        when(registry.findByName(STREAM)).thenReturn(Optional.of(definition()));
        CheckpointStore checkpointStore = mock(CheckpointStore.class);
        when(checkpointStore.findByStreamName(anyString())).thenReturn(Optional.empty());

        MessageListenerContainer container = mock(MessageListenerContainer.class);
        when(container.register(any(), any(Class.class), any(ErrorHandler.class)))
                .thenAnswer(invocation -> {
                    AtomicBoolean started = new AtomicBoolean(false);
                    Subscription subscription = mock(Subscription.class);
                    when(subscription.await(any(Duration.class))).thenAnswer(i -> started.get());
                    generations.add(new Generation(System.nanoTime(), started, invocation.getArgument(2)));
                    return subscription;
                });

        HeartbeatProbe alwaysCertifying = new HeartbeatProbe() {
            @Override
            public ProbeOutcome probe(BsonDocument resumeAfter) {
                probes.incrementAndGet();
                return ProbeOutcome.empty(PBRT);
            }

            @Override
            public ProbeOutcome probeFromOperationTime(BsonTimestamp operationTime) {
                probes.incrementAndGet();
                return ProbeOutcome.empty(PBRT);
            }

            @Override
            public BsonDocument initialPosition() {
                return SEED;
            }
        };
        return new ImperativeStreamManager(
                templateRegistry, registry, checkpointStore, DlqStore.noOp(), null) {
            @Override
            MessageListenerContainer createContainer(MongoTemplate streamTemplate, String name) {
                return container;
            }

            @Override
            HeartbeatProbe createHeartbeatProbe(MongoTemplate streamTemplate, ChangeStreamDefinition def,
                                                List<Document> resolvedPipeline) {
                return alwaysCertifying;
            }
        };
    }

    /** Idle heartbeat on (the startup probe runs for every generation); a fast, steep restart cadence. */
    @io.flowwarden.stream.annotation.Checkpoint(idleHeartbeatIntervalSeconds = 1)
    @RestartPolicy(initialDelay = "200ms", maxDelay = "10s", multiplier = 3.0)
    private static final class AnnotationCarrier { }

    private static ChangeStreamDefinition definition() {
        StreamConfig config = new StreamConfig(true, false, Document.class, "",
                FullDocumentMode.DEFAULT, FullDocumentBeforeChangeMode.OFF,
                DeploymentMode.ALL_INSTANCES);
        return new ChangeStreamDefinition(STREAM, "restart_backoff", "", "",
                new Object(), null, Map.of(), config, null, null, null,
                AnnotationCarrier.class.getAnnotation(io.flowwarden.stream.annotation.Checkpoint.class),
                null,
                AnnotationCarrier.class.getAnnotation(RestartPolicy.class),
                null, null,
                new ErrorHandlerResolver(List.of()), Map.of());
    }

    /** Records the restart signal only; every other callback is a no-op. */
    private final class RestartRecorder implements StreamMetricsProvider {
        @Override
        public void onStreamRestarted(String streamName, int attempt, Throwable cause) {
            restarts.add(streamName + ":" + attempt);
        }

        @Override public void onStreamStarted(String streamName,
                io.flowwarden.stream.spi.StreamConfiguration config) {}
        @Override public void onEventReceived(String streamName,
                io.flowwarden.stream.spi.ChangeEventMetadata metadata) {}
        @Override public void onEventProcessed(String streamName, long durationNanos, boolean success) {}
        @Override public void onEventError(String streamName, Throwable error, boolean willRetry,
                int attemptNumber, io.flowwarden.stream.spi.ChangeEventMetadata metadata) {}
        @Override public void onCheckpoint(String streamName, String resumeToken) {}
        @Override public void onBufferStatus(String streamName, int currentSize, int maxSize) {}
        @Override public void onBackpressure(String streamName,
                io.flowwarden.stream.spi.BackpressureAction action) {}
        @Override public void onEventSentToDlq(String streamName) {}
        @Override public void onOplogStats(double logLengthHours, String status) {}
    }
}
