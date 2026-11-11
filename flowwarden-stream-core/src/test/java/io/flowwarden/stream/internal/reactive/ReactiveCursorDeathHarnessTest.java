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

import com.mongodb.MongoException;
import io.flowwarden.stream.DeploymentMode;
import io.flowwarden.stream.FlowWardenMetrics;
import io.flowwarden.stream.FullDocumentBeforeChangeMode;
import io.flowwarden.stream.FullDocumentMode;
import io.flowwarden.stream.internal.MongoTemplateRegistry;
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
 * Metrics contract of a reactive cursor death, on a controllable
 * {@link Sinks.Many} standing in for the driver's change stream Flux. The
 * death of the cursor is a stream-level failure: it is reported once, as
 * {@code onStreamStopped(CRASHED, cause)}, and never as
 * {@code onEventError} — that callback is a handler failure with a 1-based
 * attempt and non-null event metadata, and a provider written against its
 * Javadoc used to get {@code attemptNumber = 0} and {@code metadata = null}
 * from the reactive pipeline.
 */
class ReactiveCursorDeathHarnessTest {

    private static final String STREAM = "rx-death-harness";

    private final RecordingMetrics metrics = new RecordingMetrics();
    private final List<Sinks.Many<ChangeStreamEvent<Document>>> sinks = new CopyOnWriteArrayList<>();
    private ReactiveStreamManager manager;

    @AfterEach
    void tearDown() {
        // The manager is shut down whatever the test's outcome: a restart
        // armed by the death under test must not outlive the test and run
        // against the restored no-op provider.
        if (manager != null) {
            manager.shutdown();
        }
        FlowWardenMetrics.setProvider(StreamMetricsProvider.noOp());
    }

    @Test
    void cursorDeath_reportsStreamStoppedCrashed_andNeverOnEventError() {
        FlowWardenMetrics.setProvider(metrics);
        manager = manager(sinks);

        manager.startStream(STREAM);
        assertThat(manager.isRunning(STREAM)).isTrue();
        assertThat(sinks).hasSize(1);

        MongoException cause = new MongoException(286, "ChangeStreamHistoryLost (harness)");
        sinks.get(0).tryEmitError(cause);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(metrics.stops)
                        .as("a cursor death is surfaced as exactly one CRASHED stop")
                        .hasSize(1));
        Stop stop = metrics.stops.get(0);
        assertThat(stop.streamName()).isEqualTo(STREAM);
        assertThat(stop.reason()).isEqualTo(StopReason.CRASHED);
        assertThat(stop.cause())
                .as("the stop carries the driver's own exception, not a wrapper")
                .isSameAs(cause);

        assertThat(metrics.eventErrors)
                .as("a stream-level failure is never reported as a handler failure")
                .isEmpty();
        assertThat(manager.isRunning(STREAM)).isFalse();
        assertThat(manager.isRestartPending(STREAM))
                .as("the death is handed to the managed restart loop")
                .isTrue();
    }

    @SuppressWarnings({"unchecked"})
    private static ReactiveStreamManager manager(List<Sinks.Many<ChangeStreamEvent<Document>>> sinks) {
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

        // Bypass the comment-stamping template rebuild: the harness template
        // is a mock whose changeStream() IS the seam under test.
        return new ReactiveStreamManager(
                templateRegistry, registry, checkpointStore, DlqStore.noOp(), null) {
            @Override
            ReactiveMongoTemplate stampedForStream(ReactiveMongoTemplate t, String name) {
                return t;
            }
        };
    }

    /** No checkpoint: the death takes the plain runtime-death path. */
    private static ChangeStreamDefinition definition() {
        StreamConfig config = new StreamConfig(true, false, Document.class, "",
                FullDocumentMode.DEFAULT, FullDocumentBeforeChangeMode.OFF,
                DeploymentMode.ALL_INSTANCES);
        return new ChangeStreamDefinition(STREAM, "rx_death_harness", "", "",
                new Object(), null, Map.of(), config, null, null,
                null, null, null, null, null,
                new ErrorHandlerResolver(List.of()), Map.of());
    }

    private record Stop(String streamName, StopReason reason, Throwable cause) { }

    private static final class RecordingMetrics implements StreamMetricsProvider {
        final List<Stop> stops = new CopyOnWriteArrayList<>();
        final List<String> eventErrors = new CopyOnWriteArrayList<>();

        @Override
        public void onStreamStopped(String streamName, StopReason reason, Throwable cause) {
            stops.add(new Stop(streamName, reason, cause));
        }

        @Override
        public void onEventError(String streamName, Throwable error, boolean willRetry,
                                 int attemptNumber, ChangeEventMetadata metadata) {
            eventErrors.add(streamName + ":" + attemptNumber + ":" + metadata);
        }

        @Override public void onStreamStarted(String streamName, StreamConfiguration config) {}
        @Override public void onEventReceived(String streamName, ChangeEventMetadata metadata) {}
        @Override public void onEventProcessed(String streamName, long durationNanos, boolean success) {}
        @Override public void onCheckpoint(String streamName, String resumeToken) {}
        @Override public void onBufferStatus(String streamName, int currentSize, int maxSize) {}
        @Override public void onBackpressure(String streamName, BackpressureAction action) {}
        @Override public void onEventSentToDlq(String streamName) {}
        @Override public void onOplogStats(double logLengthHours, String status) {}
    }
}
