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

import io.flowwarden.stream.DeploymentMode;
import io.flowwarden.stream.FlowWardenMetrics;
import io.flowwarden.stream.FullDocumentBeforeChangeMode;
import io.flowwarden.stream.FullDocumentMode;
import io.flowwarden.stream.StreamStuckException;
import io.flowwarden.stream.internal.MongoTemplateRegistry;
import io.flowwarden.stream.internal.checkpoint.HeartbeatProbe;
import io.flowwarden.stream.internal.checkpoint.ProbeOutcome;
import io.flowwarden.stream.internal.discovery.ChangeStreamDefinition;
import io.flowwarden.stream.internal.discovery.ErrorHandlerResolver;
import io.flowwarden.stream.internal.discovery.StreamConfig;
import io.flowwarden.stream.internal.discovery.StreamRegistry;
import io.flowwarden.stream.spi.BackpressureAction;
import io.flowwarden.stream.spi.ChangeEventMetadata;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.DlqStore;
import io.flowwarden.stream.spi.StopReason;
import io.flowwarden.stream.spi.StreamConfiguration;
import io.flowwarden.stream.spi.StreamMetricsProvider;
import org.bson.BsonDocument;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Manager-level harness for the stuck-stream verdict (reactive): a change
 * stream flux that never emits while the heartbeat probe keeps finding
 * undelivered events is declared stuck, reported {@code CRASHED} with a
 * {@link StreamStuckException}, disposed, and resubscribed by the restart
 * loop.
 */
class ReactiveStuckStreamTest {

    private static final String STREAM = "stuck-reactive";
    private static final BsonDocument SEED = BsonDocument.parse("{\"_data\": \"seed\"}");

    private final RecordingMetrics metrics = new RecordingMetrics();

    @AfterEach
    void tearDown() {
        FlowWardenMetrics.setProvider(StreamMetricsProvider.noOp());
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void hungCursor_isDeclaredStuck_crashedWithVerdict_disposedAndResubscribed() {
        FlowWardenMetrics.setProvider(metrics);

        ReactiveMongoTemplate template = mock(ReactiveMongoTemplate.class);
        MongoTemplateRegistry templateRegistry = mock(MongoTemplateRegistry.class);
        when(templateRegistry.getDefaultReactiveTemplate()).thenReturn(template);
        when(templateRegistry.resolveReactive(anyString())).thenReturn(template);
        StreamRegistry registry = mock(StreamRegistry.class);
        when(registry.findByName(STREAM)).thenReturn(Optional.of(definition()));
        CheckpointStore checkpointStore = mock(CheckpointStore.class);
        when(checkpointStore.findByStreamName(anyString())).thenReturn(Optional.empty());

        // A flux that never emits and never terminates: the reactive shape
        // of a getMore hanging on a half-open connection.
        List<Sinks.Many<ChangeStreamEvent<Document>>> sinks = new CopyOnWriteArrayList<>();
        List<ChangeStreamOptions> options = new CopyOnWriteArrayList<>();
        when(template.changeStream(anyString(), any(ChangeStreamOptions.class), eq(Document.class)))
                .thenAnswer(invocation -> {
                    options.add(invocation.getArgument(1));
                    Sinks.Many<ChangeStreamEvent<Document>> sink =
                            Sinks.many().unicast().onBackpressureBuffer();
                    sinks.add(sink);
                    return sink.asFlux();
                });

        ReactiveStreamManager manager = manager(templateRegistry, registry, checkpointStore);
        manager.startStream(STREAM);
        assertThat(manager.isRunning(STREAM)).isTrue();
        assertThat(sinks).hasSize(1);
        Sinks.Many<ChangeStreamEvent<Document>> hungSink = sinks.get(0);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(metrics.stops).contains(STREAM + ":CRASHED:StreamStuckException"));

        // Disposed: the hung flux lost its subscriber, the state is evicted…
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(hungSink.currentSubscriberCount()).isZero());
        assertThat(manager.isRunning(STREAM)).isFalse();

        // …and the restart loop resubscribes: a second change stream.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(sinks).hasSizeGreaterThanOrEqualTo(2));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(metrics.restarts).contains(STREAM + ":1"));

        // RESUME: the restart generation goes through the cascade and resumes
        // from the certified position — nothing the hung cursor left behind
        // is skipped.
        assertThat(options.get(1).getResumeToken())
                .as("under RESUME the resubscription chains from the persisted position")
                .contains(SEED);

        manager.shutdown();
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void latest_stuckRestartStartsFromNow_theDocumentedLossOfLatest() throws Exception {
        // Review finding (second pass): the stuck restart is an ordinary
        // start. Under StartPosition.LATEST that means "from now", exactly
        // like after a cursor error — the events the hung cursor never
        // delivered are skipped, which is the loss LATEST accepts on every
        // restart. Pinned here so the documentation stays honest.
        FlowWardenMetrics.setProvider(metrics);

        ReactiveMongoTemplate template = mock(ReactiveMongoTemplate.class);
        when(template.getConverter()).thenReturn(mock(
                org.springframework.data.mongodb.core.convert.MongoConverter.class));
        MongoTemplateRegistry templateRegistry = mock(MongoTemplateRegistry.class);
        when(templateRegistry.getDefaultReactiveTemplate()).thenReturn(template);
        when(templateRegistry.resolveReactive(anyString())).thenReturn(template);
        StreamRegistry registry = mock(StreamRegistry.class);
        when(registry.findByName(STREAM)).thenReturn(Optional.of(latestDefinitionWithHandler(new NoopBean())));
        CheckpointStore checkpointStore = mock(CheckpointStore.class);
        when(checkpointStore.findByStreamName(anyString())).thenReturn(Optional.empty());

        List<Sinks.Many<ChangeStreamEvent<Document>>> sinks = new CopyOnWriteArrayList<>();
        List<ChangeStreamOptions> options = new CopyOnWriteArrayList<>();
        when(template.changeStream(anyString(), any(ChangeStreamOptions.class), eq(Document.class)))
                .thenAnswer(invocation -> {
                    options.add(invocation.getArgument(1));
                    Sinks.Many<ChangeStreamEvent<Document>> sink =
                            Sinks.many().unicast().onBackpressureBuffer();
                    sinks.add(sink);
                    return sink.asFlux();
                });

        ReactiveStreamManager manager = manager(templateRegistry, registry, checkpointStore);
        manager.startStream(STREAM);
        assertThat(options.get(0).getResumeToken()).as("LATEST never chains at start").isEmpty();

        // Under LATEST nothing is seeded: the probe has no position until an
        // event settles. One event gives it one, then the cursor "hangs".
        sinks.get(0).tryEmitNext(insertEvent());

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(metrics.stops).contains(STREAM + ":CRASHED:StreamStuckException"));
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(sinks).hasSizeGreaterThanOrEqualTo(2));

        assertThat(options.get(1).getResumeToken())
                .as("under LATEST the resubscription starts from now — the persisted anchors are ignored")
                .isEmpty();

        manager.shutdown();
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void backToBackEvents_gateIsReleasedBeforeTheNextMapping_nothingDropped() throws Exception {
        // Regression: releasing the gate in doFinally — AFTER the terminal
        // signal propagates — let concatMap map the next event while the
        // gate still read HANDLING, and the event was dropped. Release must
        // happen before propagation (doOnTerminate / doOnCancel).
        FlowWardenMetrics.setProvider(metrics);

        ReactiveMongoTemplate template = mock(ReactiveMongoTemplate.class);
        when(template.getConverter()).thenReturn(mock(
                org.springframework.data.mongodb.core.convert.MongoConverter.class));
        MongoTemplateRegistry templateRegistry = mock(MongoTemplateRegistry.class);
        when(templateRegistry.getDefaultReactiveTemplate()).thenReturn(template);
        when(templateRegistry.resolveReactive(anyString())).thenReturn(template);
        CountingBean bean = new CountingBean();
        StreamRegistry registry = mock(StreamRegistry.class);
        when(registry.findByName(STREAM)).thenReturn(Optional.of(definitionWithCountingHandler(bean)));
        CheckpointStore checkpointStore = mock(CheckpointStore.class);
        when(checkpointStore.findByStreamName(anyString())).thenReturn(Optional.empty());

        List<Sinks.Many<ChangeStreamEvent<Document>>> sinks = new CopyOnWriteArrayList<>();
        when(template.changeStream(anyString(), any(ChangeStreamOptions.class), eq(Document.class)))
                .thenAnswer(invocation -> {
                    Sinks.Many<ChangeStreamEvent<Document>> sink =
                            Sinks.many().unicast().onBackpressureBuffer();
                    sinks.add(sink);
                    return sink.asFlux();
                });

        // Stuck detection off: this test is about the gate's release only.
        ReactiveStreamManager manager = manager(templateRegistry, registry, checkpointStore);
        manager.startStream(STREAM);
        Sinks.Many<ChangeStreamEvent<Document>> sink = sinks.get(0);

        for (int i = 0; i < 20; i++) {
            sink.tryEmitNext(insertEvent());
        }

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(bean.handled.get()).as("every back-to-back event is dispatched").isEqualTo(20));
        assertThat(metrics.stops).isEmpty();

        manager.shutdown();
    }

    /** Handler that counts its invocations. */
    public static class CountingBean {
        final java.util.concurrent.atomic.AtomicInteger handled = new java.util.concurrent.atomic.AtomicInteger();

        public void handle(io.flowwarden.stream.ChangeStreamContext<Document> ctx) {
            handled.incrementAndGet();
        }
    }

    @io.flowwarden.stream.annotation.Checkpoint(stuckAfterProbes = 0)
    private static final class NoStuckCheckpointCarrier { }

    private static ChangeStreamDefinition definitionWithCountingHandler(CountingBean bean) throws Exception {
        io.flowwarden.stream.internal.discovery.HandlerMethod onChange =
                new io.flowwarden.stream.internal.discovery.HandlerMethod(
                        CountingBean.class.getMethod("handle", io.flowwarden.stream.ChangeStreamContext.class),
                        io.flowwarden.stream.internal.discovery.HandlerMethod.SignatureStyle.CONTEXT_ONLY,
                        false);
        StreamConfig config = new StreamConfig(true, false, Document.class, "",
                FullDocumentMode.DEFAULT, FullDocumentBeforeChangeMode.OFF,
                DeploymentMode.ALL_INSTANCES);
        return new ChangeStreamDefinition(STREAM, "stuck_rx", "", "",
                bean, onChange, Map.of(), config, null, null,
                NoStuckCheckpointCarrier.class.getAnnotation(
                        io.flowwarden.stream.annotation.Checkpoint.class),
                null, null, null, null,
                new ErrorHandlerResolver(List.of()), Map.of());
    }

    private ReactiveStreamManager manager(MongoTemplateRegistry templateRegistry, StreamRegistry registry,
                                          CheckpointStore checkpointStore) {
        HeartbeatProbe alwaysPending = new HeartbeatProbe() {
            @Override
            public ProbeOutcome probe(BsonDocument resumeAfter) {
                return ProbeOutcome.eventPending();
            }

            @Override
            public ProbeOutcome probeFromOperationTime(org.bson.BsonTimestamp operationTime) {
                return ProbeOutcome.eventPending();
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
                return t;
            }

            @Override
            HeartbeatProbe createHeartbeatProbe(ReactiveMongoTemplate streamTemplate, ChangeStreamDefinition def,
                                                List<Document> resolvedPipeline) {
                return alwaysPending;
            }
        };
    }

    /** Handler that settles immediately. */
    public static class NoopBean {
        public void handle(io.flowwarden.stream.ChangeStreamContext<Document> ctx) {
        }
    }

    @io.flowwarden.stream.annotation.Checkpoint(startPosition = io.flowwarden.stream.StartPosition.LATEST,
            idleHeartbeatIntervalSeconds = 1, stuckAfterProbes = 1)
    private static final class LatestCheckpointCarrier { }

    private static ChangeStreamDefinition latestDefinitionWithHandler(NoopBean bean) throws Exception {
        io.flowwarden.stream.internal.discovery.HandlerMethod onChange =
                new io.flowwarden.stream.internal.discovery.HandlerMethod(
                        NoopBean.class.getMethod("handle", io.flowwarden.stream.ChangeStreamContext.class),
                        io.flowwarden.stream.internal.discovery.HandlerMethod.SignatureStyle.CONTEXT_ONLY,
                        false);
        StreamConfig config = new StreamConfig(true, false, Document.class, "",
                FullDocumentMode.DEFAULT, FullDocumentBeforeChangeMode.OFF,
                DeploymentMode.ALL_INSTANCES);
        return new ChangeStreamDefinition(STREAM, "stuck_rx", "", "",
                bean, onChange, Map.of(), config, null, null,
                LatestCheckpointCarrier.class.getAnnotation(
                        io.flowwarden.stream.annotation.Checkpoint.class),
                null, null, null, null,
                new ErrorHandlerResolver(List.of()), Map.of());
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void handlerInFlight_isNeverDeclaredStuck_verdictWaitsForSettlement() throws Exception {
        // Review finding: the dispatch gate is claimed on receipt, before
        // the boundedElastic hop, and held until settlement — a stream
        // whose handler is running (or waiting for a worker) is lagging,
        // not stuck, however many probes abstain meanwhile.
        FlowWardenMetrics.setProvider(metrics);

        ReactiveMongoTemplate template = mock(ReactiveMongoTemplate.class);
        when(template.getConverter()).thenReturn(mock(
                org.springframework.data.mongodb.core.convert.MongoConverter.class));
        MongoTemplateRegistry templateRegistry = mock(MongoTemplateRegistry.class);
        when(templateRegistry.getDefaultReactiveTemplate()).thenReturn(template);
        when(templateRegistry.resolveReactive(anyString())).thenReturn(template);
        BlockingBean bean = new BlockingBean();
        StreamRegistry registry = mock(StreamRegistry.class);
        when(registry.findByName(STREAM)).thenReturn(Optional.of(definitionWithHandler(bean)));
        CheckpointStore checkpointStore = mock(CheckpointStore.class);
        when(checkpointStore.findByStreamName(anyString())).thenReturn(Optional.empty());

        List<Sinks.Many<ChangeStreamEvent<Document>>> sinks = new CopyOnWriteArrayList<>();
        when(template.changeStream(anyString(), any(ChangeStreamOptions.class), eq(Document.class)))
                .thenAnswer(invocation -> {
                    Sinks.Many<ChangeStreamEvent<Document>> sink =
                            Sinks.many().unicast().onBackpressureBuffer();
                    sinks.add(sink);
                    return sink.asFlux();
                });
        HeartbeatProbe alwaysPending = new HeartbeatProbe() {
            @Override
            public ProbeOutcome probe(BsonDocument resumeAfter) {
                return ProbeOutcome.eventPending();
            }

            @Override
            public ProbeOutcome probeFromOperationTime(org.bson.BsonTimestamp operationTime) {
                return ProbeOutcome.eventPending();
            }

            @Override
            public BsonDocument initialPosition() {
                return SEED;
            }
        };
        ReactiveStreamManager manager = new ReactiveStreamManager(
                templateRegistry, registry, checkpointStore, DlqStore.noOp(), null) {
            @Override
            ReactiveMongoTemplate stampedForStream(ReactiveMongoTemplate t, String name) {
                return t;
            }

            @Override
            HeartbeatProbe createHeartbeatProbe(ReactiveMongoTemplate streamTemplate, ChangeStreamDefinition def,
                                                List<Document> resolvedPipeline) {
                return alwaysPending;
            }
        };
        manager.startStream(STREAM);
        Sinks.Many<ChangeStreamEvent<Document>> sink = sinks.get(0);

        // One event whose handler blocks: the gate is HANDLING.
        sink.tryEmitNext(insertEvent());
        assertThat(bean.entered.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();

        // Well past stuckAfterProbes × idle (1 × 1s): probes abstain, the
        // verdict is deferred, the stream keeps running.
        Thread.sleep(4_000);
        assertThat(metrics.stops).as("a handler in flight is lagging, never stuck").isEmpty();
        assertThat(manager.isRunning(STREAM)).isTrue();

        // Settlement releases the gate AND resets the streak (new settled
        // snapshot). With nothing delivered afterwards, the next idle
        // abstentions build a fresh streak and the verdict falls.
        bean.gate.countDown();
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(metrics.stops).contains(STREAM + ":CRASHED:StreamStuckException"));

        manager.shutdown();
    }

    /** Handler that blocks until released — a dispatch in flight. */
    public static class BlockingBean {
        final java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
        final java.util.concurrent.CountDownLatch gate = new java.util.concurrent.CountDownLatch(1);

        public void handle(io.flowwarden.stream.ChangeStreamContext<Document> ctx) {
            entered.countDown();
            try {
                gate.await(30, java.util.concurrent.TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static ChangeStreamEvent<Document> insertEvent() {
        com.mongodb.client.model.changestream.ChangeStreamDocument<Document> raw =
                mock(com.mongodb.client.model.changestream.ChangeStreamDocument.class);
        when(raw.getOperationType()).thenReturn(
                com.mongodb.client.model.changestream.OperationType.INSERT);
        when(raw.getResumeToken()).thenReturn(BsonDocument.parse("{\"_data\": \"token-insert\"}"));
        ChangeStreamEvent<Document> event = mock(ChangeStreamEvent.class);
        when(event.getRaw()).thenReturn(raw);
        return event;
    }

    @io.flowwarden.stream.annotation.Checkpoint(idleHeartbeatIntervalSeconds = 1, stuckAfterProbes = 1)
    private static final class CheckpointAnnotationCarrier { }

    private static ChangeStreamDefinition definition() {
        return definition(null, new Object());
    }

    private static ChangeStreamDefinition definitionWithHandler(BlockingBean bean) throws Exception {
        io.flowwarden.stream.internal.discovery.HandlerMethod onChange =
                new io.flowwarden.stream.internal.discovery.HandlerMethod(
                        BlockingBean.class.getMethod("handle", io.flowwarden.stream.ChangeStreamContext.class),
                        io.flowwarden.stream.internal.discovery.HandlerMethod.SignatureStyle.CONTEXT_ONLY,
                        false);
        return definition(onChange, bean);
    }

    private static ChangeStreamDefinition definition(
            io.flowwarden.stream.internal.discovery.HandlerMethod onChange, Object bean) {
        StreamConfig config = new StreamConfig(true, false, Document.class, "",
                FullDocumentMode.DEFAULT, FullDocumentBeforeChangeMode.OFF,
                DeploymentMode.ALL_INSTANCES);
        return new ChangeStreamDefinition(STREAM, "stuck_rx", "", "",
                bean, onChange, Map.of(), config, null, null,
                CheckpointAnnotationCarrier.class.getAnnotation(
                        io.flowwarden.stream.annotation.Checkpoint.class),
                null, null, null, null,
                new ErrorHandlerResolver(List.of()), Map.of());
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
            restarts.add(streamName + ":" + attempt);
        }

        @Override public void onStreamStarted(String streamName, StreamConfiguration config) {}
        @Override public void onEventReceived(String streamName, ChangeEventMetadata metadata) {}
        @Override public void onEventProcessed(String streamName, long durationNanos, boolean success) {}
        @Override public void onEventError(String streamName, Throwable error, boolean willRetry,
                                           int attemptNumber, ChangeEventMetadata metadata) {}
        @Override public void onCheckpoint(String streamName, String resumeToken) {}
        @Override public void onBufferStatus(String streamName, int currentSize, int maxSize) {}
        @Override public void onBackpressure(String streamName, BackpressureAction action) {}
        @Override public void onEventSentToDlq(String streamName) {}
        @Override public void onOplogStats(double logLengthHours, String status) {}
    }
}
