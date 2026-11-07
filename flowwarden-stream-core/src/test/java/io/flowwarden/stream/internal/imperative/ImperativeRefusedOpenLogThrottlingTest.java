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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.mongodb.MongoException;
import io.flowwarden.stream.DeploymentMode;
import io.flowwarden.stream.FullDocumentBeforeChangeMode;
import io.flowwarden.stream.FullDocumentMode;
import io.flowwarden.stream.internal.MongoTemplateRegistry;
import io.flowwarden.stream.internal.checkpoint.HeartbeatProbe;
import io.flowwarden.stream.internal.checkpoint.ProbeOutcome;
import io.flowwarden.stream.internal.discovery.ChangeStreamDefinition;
import io.flowwarden.stream.internal.discovery.ErrorHandlerResolver;
import io.flowwarden.stream.internal.discovery.StreamConfig;
import io.flowwarden.stream.internal.discovery.StreamRegistry;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.DlqStore;
import io.flowwarden.stream.test.LogCapture;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
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
 * Manager-level harness for the death-log throttling of a restart cycle
 * (imperative), with the main cursor's open scripted through the injected
 * container: a refused open is a generation whose reading task never
 * completes its start and whose error handler fires asynchronously, exactly
 * as Spring's reading task does. The heartbeat probe always certifies its
 * position — it opens its own cursor, and its success must never count as
 * the main cursor's proof of life.
 */
class ImperativeRefusedOpenLogThrottlingTest {

    private static final String STREAM = "refused-open-imperative";
    private static final BsonDocument SEED = BsonDocument.parse("{\"_data\": \"seed\"}");
    private static final BsonDocument PBRT = BsonDocument.parse("{\"_data\": \"pbrt\"}");

    /** One scripted generation: the start latch of its reading task and its error handler. */
    private record Generation(AtomicBoolean started, ErrorHandler errorHandler) {
    }

    private final List<Generation> generations = new CopyOnWriteArrayList<>();
    private final AtomicInteger probes = new AtomicInteger();
    private ImperativeStreamManager manager;

    @AfterEach
    void tearDown() {
        if (manager != null) {
            manager.shutdown();
        }
    }

    @Test
    void refusedOpens_afterCertifyingProbes_logTheStackOnce_untilTheMainCursorOpens() {
        manager = manager();
        try (LogCapture logs = LogCapture.of(ImperativeStreamManager.class)) {
            manager.startStream(STREAM);

            // Two generations: the probe certifies, then the main open is
            // refused with the same cause — one ERROR with the stack trace,
            // then one WARN without. The probe's success closed nothing.
            refuseOpenAfterCertifyingProbe(1);
            refuseOpenAfterCertifyingProbe(2);
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(deathLogs(logs)).hasSize(2));
            List<ILoggingEvent> deaths = deathLogs(logs);
            assertThat(deaths.get(0).getLevel()).isEqualTo(Level.ERROR);
            assertThat(deaths.get(0).getThrowableProxy())
                    .as("first death of the cycle: full stack trace").isNotNull();
            assertThat(deaths.get(1).getLevel()).isEqualTo(Level.WARN);
            assertThat(deaths.get(1).getThrowableProxy())
                    .as("a certifying probe is not the main cursor's proof: still the same cycle")
                    .isNull();

            // The third generation's reading task reports its cursor open:
            // the proof of life. Its death opens a new cycle.
            cursorOpens(3);
            die(3);
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(deathLogs(logs)).hasSize(3));
            assertThat(deathLogs(logs).get(2).getLevel()).isEqualTo(Level.ERROR);
            assertThat(deathLogs(logs).get(2).getThrowableProxy()).isNotNull();

            // Discriminating assertion: a fourth proven generation dying with
            // the SAME cause as the previous death is still a new incident —
            // the reset is the proof of life, not a change of signature.
            cursorOpens(4);
            die(4);
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(deathLogs(logs)).hasSize(4));
            assertThat(deathLogs(logs).get(3).getLevel()).isEqualTo(Level.ERROR);
            assertThat(deathLogs(logs).get(3).getThrowableProxy()).isNotNull();
        }
    }

    @Test
    void cursorReportedOpenAfterTheDeath_doesNotCloseTheCycle() {
        // A refused open throws out of the real reading task's start, which
        // then never completes — but the open watch must be fenced by the
        // termination regardless: a late signal from a terminated generation
        // must not alter the cycle state.
        manager = manager();
        try (LogCapture logs = LogCapture.of(ImperativeStreamManager.class)) {
            manager.startStream(STREAM);
            refuseOpenAfterCertifyingProbe(1);
            generations.get(0).started().set(true); // late, after the death was recorded

            refuseOpenAfterCertifyingProbe(2);
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(deathLogs(logs)).hasSize(2));
            assertThat(deathLogs(logs).get(0).getLevel()).isEqualTo(Level.ERROR);
            assertThat(deathLogs(logs).get(1).getLevel())
                    .as("the late 'active' of the dead generation closed nothing")
                    .isEqualTo(Level.WARN);
            assertThat(deathLogs(logs).get(1).getThrowableProxy()).isNull();
        }
    }

    @Test
    void operatorStop_endsTheCycle() {
        manager = manager();
        try (LogCapture logs = LogCapture.of(ImperativeStreamManager.class)) {
            manager.startStream(STREAM);
            refuseOpenAfterCertifyingProbe(1);
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(deathLogs(logs)).hasSize(1));

            // The stop kills the pending restart and ends the cycle: the
            // next start is a new one, its first death logged in full.
            manager.stopStream(STREAM);
            assertThat(manager.isRestartPending(STREAM)).isFalse();
            manager.startStream(STREAM);
            refuseOpenAfterCertifyingProbe(2);
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(deathLogs(logs)).hasSize(2));
            assertThat(deathLogs(logs).get(1).getLevel()).isEqualTo(Level.ERROR);
            assertThat(deathLogs(logs).get(1).getThrowableProxy()).isNotNull();
        }
    }

    // --- scripted generations ---

    private static MongoException refusedOpen() {
        return new MongoException(286, "ChangeStreamHistoryLost: resume point may no longer be in the oplog");
    }

    /** Waits for generation {@code n} to register and its probe to certify, then refuses its open. */
    private void refuseOpenAfterCertifyingProbe(int n) {
        await().atMost(Duration.ofSeconds(10)).until(() -> generations.size() >= n);
        int before = probes.get();
        await().atMost(Duration.ofSeconds(10)).until(() -> probes.get() > before);
        die(n);
    }

    /** Generation {@code n}'s reading task completes its start (cursor open); waits for the proof. */
    private void cursorOpens(int n) {
        await().atMost(Duration.ofSeconds(10)).until(() -> generations.size() >= n);
        generations.get(n - 1).started().set(true);
        await().atMost(Duration.ofSeconds(10)).until(() -> manager.isGenerationProven(STREAM));
    }

    /** The reading task of generation {@code n} reports a refused open / dead cursor. */
    private void die(int n) {
        generations.get(n - 1).errorHandler().handleError(refusedOpen());
    }

    private static List<ILoggingEvent> deathLogs(LogCapture logs) {
        return logs.events().stream()
                .filter(e -> e.getLevel().isGreaterOrEqual(Level.WARN))
                .filter(e -> e.getFormattedMessage().contains("died")
                        && e.getFormattedMessage().contains("'" + STREAM + "'"))
                .toList();
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
                    // The start latch, read lock-free with a zero timeout. The
                    // locking isActive() must never be consulted on the probe
                    // scheduler: consulting it here kills the watch chain.
                    when(subscription.await(any(Duration.class))).thenAnswer(i -> started.get());
                    when(subscription.isActive()).thenThrow(
                            new AssertionError("isActive() takes the reading task's lock"));
                    generations.add(new Generation(started, invocation.getArgument(2)));
                    return subscription;
                });

        // Every probe certifies: the independent cursor opens fine while
        // the main one is refused.
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

    /** Idle heartbeat on: the startup diagnostic probe runs for every generation. */
    @io.flowwarden.stream.annotation.Checkpoint(idleHeartbeatIntervalSeconds = 1)
    private static final class CheckpointAnnotationCarrier { }

    private static ChangeStreamDefinition definition() {
        StreamConfig config = new StreamConfig(true, false, Document.class, "",
                FullDocumentMode.DEFAULT, FullDocumentBeforeChangeMode.OFF,
                DeploymentMode.ALL_INSTANCES);
        return new ChangeStreamDefinition(STREAM, "refused_open", "", "",
                new Object(), null, Map.of(), config, null, null,
                CheckpointAnnotationCarrier.class.getAnnotation(
                        io.flowwarden.stream.annotation.Checkpoint.class),
                null, null, null, null,
                new ErrorHandlerResolver(List.of()), Map.of());
    }
}
