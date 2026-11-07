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
package io.flowwarden.stream.internal.reactive;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.mongodb.MongoException;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
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
import org.springframework.data.mongodb.core.ChangeStreamEvent;
import org.springframework.data.mongodb.core.ChangeStreamOptions;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Reactive twin of the imperative refused-open harness: each generation's
 * change stream is a controllable {@link Sinks.Many}; a refused open is an
 * error signal on a sink that never emitted, exactly what the driver
 * produces when the aggregate is rejected after {@code subscribe()}
 * returned. The heartbeat probe always certifies its position — it opens
 * its own cursor, and its success must never count as the main cursor's
 * proof of life.
 */
class ReactiveRefusedOpenLogThrottlingTest {

    private static final String STREAM = "refused-open-reactive";
    private static final BsonDocument SEED = BsonDocument.parse("{\"_data\": \"seed\"}");
    private static final BsonDocument PBRT = BsonDocument.parse("{\"_data\": \"pbrt\"}");

    private final List<Sinks.Many<ChangeStreamEvent<Document>>> sinks = new CopyOnWriteArrayList<>();
    private final AtomicInteger probes = new AtomicInteger();
    private ReactiveStreamManager manager;

    @AfterEach
    void tearDown() {
        if (manager != null) {
            manager.shutdown();
        }
    }

    @Test
    void refusedOpens_afterCertifyingProbes_logTheStackOnce_untilTheMainCursorDelivers() {
        manager = manager();
        try (LogCapture logs = LogCapture.of(ReactiveStreamManager.class)) {
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

            // The third generation's own cursor delivers: the proof of life.
            // Its death opens a new cycle.
            cursorDelivers(3);
            die(3);
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(deathLogs(logs)).hasSize(3));
            assertThat(deathLogs(logs).get(2).getLevel()).isEqualTo(Level.ERROR);
            assertThat(deathLogs(logs).get(2).getThrowableProxy()).isNotNull();

            // Discriminating assertion: a fourth proven generation dying with
            // the SAME cause as the previous death is still a new incident.
            cursorDelivers(4);
            die(4);
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(deathLogs(logs)).hasSize(4));
            assertThat(deathLogs(logs).get(3).getLevel()).isEqualTo(Level.ERROR);
            assertThat(deathLogs(logs).get(3).getThrowableProxy()).isNotNull();
        }
    }

    @Test
    void operatorStop_endsTheCycle() {
        manager = manager();
        try (LogCapture logs = LogCapture.of(ReactiveStreamManager.class)) {
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

    /** Waits for generation {@code n} to subscribe and its probe to certify, then refuses its open. */
    private void refuseOpenAfterCertifyingProbe(int n) {
        await().atMost(Duration.ofSeconds(10)).until(() -> sinks.size() >= n);
        int before = probes.get();
        await().atMost(Duration.ofSeconds(10)).until(() -> probes.get() > before);
        die(n);
    }

    /** Generation {@code n}'s own cursor delivers an event; waits for the proof. */
    private void cursorDelivers(int n) {
        await().atMost(Duration.ofSeconds(10)).until(() -> sinks.size() >= n);
        sinks.get(n - 1).tryEmitNext(insertEvent());
        await().atMost(Duration.ofSeconds(10)).until(() -> manager.isGenerationProven(STREAM));
    }

    /** The driver terminates generation {@code n}'s pipeline: refused open / dead cursor. */
    private void die(int n) {
        sinks.get(n - 1).tryEmitError(refusedOpen());
    }

    private static List<ILoggingEvent> deathLogs(LogCapture logs) {
        return logs.events().stream()
                .filter(e -> e.getLevel().isGreaterOrEqual(Level.WARN))
                .filter(e -> e.getFormattedMessage().startsWith("Error in reactive Change Stream")
                        || e.getFormattedMessage().contains("died again"))
                .filter(e -> e.getFormattedMessage().contains("'" + STREAM + "'"))
                .toList();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static ChangeStreamEvent<Document> insertEvent() {
        ChangeStreamDocument<Document> raw = mock(ChangeStreamDocument.class);
        when(raw.getOperationType()).thenReturn(
                com.mongodb.client.model.changestream.OperationType.INSERT);
        when(raw.getResumeToken()).thenReturn(BsonDocument.parse("{\"_data\": \"token-insert\"}"));
        ChangeStreamEvent<Document> event = mock(ChangeStreamEvent.class);
        when(event.getRaw()).thenReturn(raw);
        return event;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private ReactiveStreamManager manager() {
        ReactiveMongoTemplate template = mock(ReactiveMongoTemplate.class);
        MongoTemplateRegistry templateRegistry = mock(MongoTemplateRegistry.class);
        when(templateRegistry.getDefaultReactiveTemplate()).thenReturn(template);
        when(templateRegistry.resolveReactive(anyString())).thenReturn(template);
        StreamRegistry registry = mock(StreamRegistry.class);
        when(registry.findByName(STREAM)).thenReturn(Optional.of(definition()));
        CheckpointStore checkpointStore = mock(CheckpointStore.class);
        when(checkpointStore.findByStreamName(anyString())).thenReturn(Optional.empty());

        when(template.changeStream(anyString(), any(ChangeStreamOptions.class), eq(Document.class)))
                .thenAnswer(invocation -> {
                    Sinks.Many<ChangeStreamEvent<Document>> sink =
                            Sinks.many().unicast().onBackpressureBuffer();
                    sinks.add(sink);
                    return sink.asFlux();
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
        return new ReactiveStreamManager(
                templateRegistry, registry, checkpointStore, DlqStore.noOp(), null) {
            @Override
            ReactiveMongoTemplate stampedForStream(ReactiveMongoTemplate t, String name) {
                return t; // the mock's changeStream() IS the seam under test
            }

            @Override
            HeartbeatProbe createHeartbeatProbe(ReactiveMongoTemplate streamTemplate,
                                                ChangeStreamDefinition def,
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
        return new ChangeStreamDefinition(STREAM, "refused_open_rx", "", "",
                new Object(), null, Map.of(), config, null, null,
                CheckpointAnnotationCarrier.class.getAnnotation(
                        io.flowwarden.stream.annotation.Checkpoint.class),
                null, null, null, null,
                new ErrorHandlerResolver(List.of()), Map.of());
    }
}
