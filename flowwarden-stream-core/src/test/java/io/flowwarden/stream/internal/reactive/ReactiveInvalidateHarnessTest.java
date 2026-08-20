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

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.flowwarden.stream.ChangeStreamContext;
import io.flowwarden.stream.DeploymentMode;
import io.flowwarden.stream.FlowWardenMetrics;
import io.flowwarden.stream.FullDocumentBeforeChangeMode;
import io.flowwarden.stream.FullDocumentMode;
import io.flowwarden.stream.OnHistoryLost;
import io.flowwarden.stream.OperationType;
import io.flowwarden.stream.StartPosition;
import io.flowwarden.stream.internal.MongoTemplateRegistry;
import io.flowwarden.stream.internal.discovery.ChangeStreamDefinition;
import io.flowwarden.stream.internal.discovery.ErrorHandlerResolver;
import io.flowwarden.stream.internal.discovery.HandlerMethod;
import io.flowwarden.stream.internal.discovery.StreamConfig;
import io.flowwarden.stream.internal.discovery.StreamRegistry;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.DlqStore;
import io.flowwarden.stream.spi.StopReason;
import io.flowwarden.stream.spi.StreamMetricsProvider;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.ChangeStreamEvent;
import org.springframework.data.mongodb.core.ChangeStreamOptions;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import reactor.core.publisher.Sinks;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Reactive twin of the imperative invalidate harness tests: the change
 * stream Flux is a controllable {@link Sinks.Many}, events are emitted on
 * demand like the captured listener on the imperative side. Covers the
 * manual-checkpoint refusal of lifecycle tokens, the TOCTOU fence of the
 * post-invalidate repair, and the immediate RUNNING exit at invalidate
 * recognition.
 */
class ReactiveInvalidateHarnessTest {

    private static final String STREAM = "rx-harness";

    private final RecordingMetrics metrics = new RecordingMetrics();

    @AfterEach
    void tearDown() {
        FlowWardenMetrics.setProvider(StreamMetricsProvider.noOp());
    }

    private record Harness(ReactiveStreamManager manager,
                           CheckpointStore checkpointStore,
                           List<Sinks.Many<ChangeStreamEvent<Document>>> sinks) {
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Harness harness(ChangeStreamDefinition def) {
        FlowWardenMetrics.setProvider(metrics);
        ReactiveMongoTemplate template = mock(ReactiveMongoTemplate.class);
        MongoTemplateRegistry templateRegistry = mock(MongoTemplateRegistry.class);
        when(templateRegistry.getDefaultReactiveTemplate()).thenReturn(template);
        when(templateRegistry.resolveReactive(anyString())).thenReturn(template);
        StreamRegistry registry = mock(StreamRegistry.class);
        when(registry.findByName(STREAM)).thenReturn(Optional.of(def));
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

        // Bypass the comment-stamping template rebuild: the harness template
        // is a mock whose changeStream() IS the seam under test.
        ReactiveStreamManager manager = new ReactiveStreamManager(
                templateRegistry, registry, checkpointStore, DlqStore.noOp(), null) {
            @Override
            ReactiveMongoTemplate stampedForStream(ReactiveMongoTemplate t, String name) {
                return t;
            }
        };
        return new Harness(manager, checkpointStore, sinks);
    }

    private static ChangeStreamEvent<Document> event(
            com.mongodb.client.model.changestream.OperationType driverType) {
        ChangeStreamDocument<Document> raw = mock(ChangeStreamDocument.class);
        when(raw.getOperationType()).thenReturn(driverType);
        when(raw.getResumeToken()).thenReturn(
                BsonDocument.parse("{\"_data\": \"token-" + driverType.getValue() + "\"}"));
        ChangeStreamEvent<Document> event = mock(ChangeStreamEvent.class);
        when(event.getRaw()).thenReturn(raw);
        return event;
    }

    @Test
    void manualCheckpoint_refusesLifecycleEventTokens() throws Exception {
        Harness h = harness(definitionWithManualSaver());
        h.manager().startStream(STREAM);
        assertThat(h.manager().isRunning(STREAM)).isTrue();
        Sinks.Many<ChangeStreamEvent<Document>> sink = h.sinks().get(0);

        sink.tryEmitNext(event(com.mongodb.client.model.changestream.OperationType.INSERT));
        await().atMost(java.time.Duration.ofSeconds(5)).untilAsserted(() ->
                verify(h.checkpointStore(), org.mockito.Mockito.atLeastOnce()).save(any()));
        clearInvocations(h.checkpointStore());

        sink.tryEmitNext(event(com.mongodb.client.model.changestream.OperationType.DROP));
        sink.tryEmitNext(event(com.mongodb.client.model.changestream.OperationType.RENAME));
        Thread.sleep(1_000);
        verify(h.checkpointStore(), never()).save(any());
        verify(h.checkpointStore(), never()).saveProcessed(anyString(), any(), any());
        verify(h.checkpointStore(), never()).saveSeen(anyString(), any(), any());
        verify(h.checkpointStore(), never()).saveSeen(anyString(), any(), any(), any());

        h.manager().shutdown();
    }

    @Test
    void invalidateRepairRacingStopStart_staleGenerationNeverTouchesTheNewOne() throws Exception {
        Harness h = harness(definitionWithCheckpoint());
        h.manager().startStream(STREAM);
        Sinks.Many<ChangeStreamEvent<Document>> oldSink = h.sinks().get(0);

        CountDownLatch hookEntered = new CountDownLatch(1);
        CountDownLatch hookGate = new CountDownLatch(1);
        h.manager().invalidateRepairTestHook = () -> {
            hookEntered.countDown();
            try {
                hookGate.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        // Generation A's INVALIDATE: the sync part runs, the repair blocks
        // in the TOCTOU window on boundedElastic.
        oldSink.tryEmitNext(event(com.mongodb.client.model.changestream.OperationType.INVALIDATE));
        assertThat(hookEntered.await(5, TimeUnit.SECONDS)).isTrue();
        h.manager().invalidateRepairTestHook = null;

        // Stop/start installs generation B while A's repair is suspended.
        h.manager().stopStream(STREAM);
        h.manager().startStream(STREAM);
        assertThat(h.manager().isRunning(STREAM)).isTrue();
        clearInvocations(h.checkpointStore());

        hookGate.countDown();
        Thread.sleep(1_000);

        // A discarded its repair: nothing touched B's checkpoint or armed a
        // restart over the living generation.
        verifyNoInteractions(h.checkpointStore());
        assertThat(h.manager().isRunning(STREAM)).isTrue();
        assertThat(h.manager().isRestartPending(STREAM)).isFalse();

        h.manager().shutdown();
    }

    @Test
    void invalidateRecognition_exitsRunningBeforeAnyCallbackOrRepairIo() throws Exception {
        metrics.invalidationsEntered = new CountDownLatch(1);
        metrics.invalidationsGate = new CountDownLatch(1);
        Harness h = harness(definitionWithCheckpoint());
        h.manager().startStream(STREAM);
        assertThat(h.manager().isRunning(STREAM)).isTrue();
        Sinks.Many<ChangeStreamEvent<Document>> sink = h.sinks().get(0);

        Thread emitThread = new Thread(() -> sink.tryEmitNext(
                event(com.mongodb.client.model.changestream.OperationType.INVALIDATE)));
        emitThread.start();

        // The provider is blocked inside onStreamInvalidated: the RUNNING
        // exit already happened — before callbacks and before the repair.
        assertThat(metrics.invalidationsEntered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(h.manager().isRunning(STREAM))
                .as("the health transition precedes callbacks and repair I/O")
                .isFalse();

        metrics.invalidationsGate.countDown();
        emitThread.join(5_000);
        h.manager().shutdown();
    }

    /** Bean whose catch-all handler manually checkpoints every event. */
    public static class ManualSaverBean {
        public void handle(ChangeStreamContext<Document> ctx) {
            ctx.saveCheckpointNow();
        }
    }

    @io.flowwarden.stream.annotation.Checkpoint(startPosition = StartPosition.LATEST,
            onHistoryLost = OnHistoryLost.RESUME_FROM_NOW)
    private static final class CheckpointAnnotationCarrier { }

    private static ChangeStreamDefinition definitionWithManualSaver() throws Exception {
        HandlerMethod onChange = new HandlerMethod(
                ManualSaverBean.class.getMethod("handle", ChangeStreamContext.class),
                HandlerMethod.SignatureStyle.CONTEXT_ONLY, false);
        return definition(onChange, new ManualSaverBean());
    }

    private static ChangeStreamDefinition definitionWithCheckpoint() {
        return definition(null, new Object());
    }

    private static ChangeStreamDefinition definition(HandlerMethod onChange, Object bean) {
        StreamConfig config = new StreamConfig(true, false, Document.class, "",
                FullDocumentMode.DEFAULT, FullDocumentBeforeChangeMode.OFF,
                DeploymentMode.ALL_INSTANCES);
        return new ChangeStreamDefinition(STREAM, "rx_harness", "", "",
                bean, onChange, Map.of(), config, null, null,
                CheckpointAnnotationCarrier.class.getAnnotation(
                        io.flowwarden.stream.annotation.Checkpoint.class),
                null, null, null,
                new ErrorHandlerResolver(List.of()), Map.of());
    }

    private static final class RecordingMetrics implements StreamMetricsProvider {
        final List<String> invalidations = new CopyOnWriteArrayList<>();
        volatile CountDownLatch invalidationsEntered;
        volatile CountDownLatch invalidationsGate;

        @Override
        public void onStreamInvalidated(String streamName, OperationType cause) {
            invalidations.add(streamName + ":" + cause);
            if (invalidationsEntered != null) {
                invalidationsEntered.countDown();
                try {
                    invalidationsGate.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        @Override
        public void onStreamStopped(String streamName, StopReason reason, Throwable cause) {
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
