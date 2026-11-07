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

import io.flowwarden.stream.DeploymentMode;
import io.flowwarden.stream.FlowWardenMetrics;
import io.flowwarden.stream.FullDocumentBeforeChangeMode;
import io.flowwarden.stream.FullDocumentMode;
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
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The open watch must never take the reading task's lock on the probe
 * scheduler. Spring's {@code Subscription.isActive()} reads the task state
 * under the lock the reading thread holds across every {@code tryNext()}:
 * a getMore hanging on a half-open connection (unbounded with the driver's
 * default socket timeout) would hold the single probe thread with it,
 * starving every stream's idle probes — including the stuck verdict meant
 * for that very hang. Here every subscription models that hang: its
 * {@code isActive()} blocks until the test's cleanup, while its start latch
 * reports the completed start. The idle generation must still prove itself,
 * and a second stream on the same manager must keep probing all the way to
 * its stuck verdict.
 */
class ImperativeOpenWatchIsolationTest {

    private static final String IDLE = "open-watch-idle";
    private static final String STUCK = "open-watch-stuck";
    private static final BsonDocument SEED = BsonDocument.parse("{\"_data\": \"seed\"}");
    private static final BsonDocument PBRT = BsonDocument.parse("{\"_data\": \"pbrt\"}");

    /** The reading task's lock, "released" only by the cleanup. */
    private final CountDownLatch taskLock = new CountDownLatch(1);
    private final RecordingMetrics metrics = new RecordingMetrics();
    private ImperativeStreamManager manager;

    @AfterEach
    void tearDown() {
        taskLock.countDown();
        if (manager != null) {
            manager.shutdown();
        }
        FlowWardenMetrics.setProvider(StreamMetricsProvider.noOp());
    }

    @Test
    void hungReadingTasks_neverBlockTheProbeScheduler() throws Exception {
        FlowWardenMetrics.setProvider(metrics);
        Subscription hungIdle = hungSubscription();
        Subscription hungStuck = hungSubscription();
        manager = manager(Map.of(IDLE, hungIdle, STUCK, hungStuck));

        manager.startStream(IDLE);
        manager.startStream(STUCK);

        // The idle generation proves itself through its start latch alone…
        await().atMost(Duration.ofSeconds(10)).until(() -> manager.isGenerationProven(IDLE));

        // …and the other stream's probes keep flowing on the shared probe
        // thread, up to the stuck verdict its own hang deserves.
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(metrics.stops).contains(STUCK + ":CRASHED:StreamStuckException"));

        verify(hungIdle, never()).isActive();
        verify(hungStuck, never()).isActive();
    }

    /** Start completed, then hung inside the driver with the task's lock held. */
    private Subscription hungSubscription() throws InterruptedException {
        Subscription subscription = mock(Subscription.class);
        when(subscription.await(any(Duration.class))).thenReturn(true);
        when(subscription.isActive()).thenAnswer(invocation -> {
            taskLock.await();
            return true;
        });
        return subscription;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private ImperativeStreamManager manager(Map<String, Subscription> subscriptions) {
        MongoTemplate template = mock(MongoTemplate.class);
        MongoTemplateRegistry templateRegistry = mock(MongoTemplateRegistry.class);
        when(templateRegistry.getDefaultTemplate()).thenReturn(template);
        when(templateRegistry.resolve(anyString())).thenReturn(template);
        StreamRegistry registry = mock(StreamRegistry.class);
        when(registry.findByName(IDLE)).thenReturn(Optional.of(
                definition(IDLE, "open_watch_idle", IdleCarrier.class)));
        when(registry.findByName(STUCK)).thenReturn(Optional.of(
                definition(STUCK, "open_watch_stuck", StuckCarrier.class)));

        // The idle stream's probe certifies an empty window; the other
        // stream's probe keeps finding events its cursor never delivers.
        HeartbeatProbe empty = probe(ProbeOutcome.empty(PBRT));
        HeartbeatProbe pending = probe(ProbeOutcome.eventPending());

        return new ImperativeStreamManager(
                templateRegistry, registry, CheckpointStore.noOp(), DlqStore.noOp(), null) {
            @Override
            MessageListenerContainer createContainer(MongoTemplate streamTemplate, String name) {
                MessageListenerContainer container = mock(MessageListenerContainer.class);
                when(container.register(any(), any(Class.class), any(ErrorHandler.class)))
                        .thenReturn(subscriptions.get(name));
                return container;
            }

            @Override
            HeartbeatProbe createHeartbeatProbe(MongoTemplate streamTemplate, ChangeStreamDefinition def,
                                                List<Document> resolvedPipeline) {
                return STUCK.equals(def.streamName()) ? pending : empty;
            }
        };
    }

    private static HeartbeatProbe probe(ProbeOutcome outcome) {
        return new HeartbeatProbe() {
            @Override
            public ProbeOutcome probe(BsonDocument resumeAfter) {
                return outcome;
            }

            @Override
            public ProbeOutcome probeFromOperationTime(BsonTimestamp operationTime) {
                return outcome;
            }

            @Override
            public BsonDocument initialPosition() {
                return SEED;
            }
        };
    }

    @io.flowwarden.stream.annotation.Checkpoint(idleHeartbeatIntervalSeconds = 1)
    private static final class IdleCarrier { }

    @io.flowwarden.stream.annotation.Checkpoint(idleHeartbeatIntervalSeconds = 1, stuckAfterProbes = 1)
    private static final class StuckCarrier { }

    private static ChangeStreamDefinition definition(String name, String collection, Class<?> carrier) {
        StreamConfig config = new StreamConfig(true, false, Document.class, "",
                FullDocumentMode.DEFAULT, FullDocumentBeforeChangeMode.OFF,
                DeploymentMode.ALL_INSTANCES);
        return new ChangeStreamDefinition(name, collection, "", "",
                new Object(), null, Map.of(), config, null, null,
                carrier.getAnnotation(io.flowwarden.stream.annotation.Checkpoint.class),
                null, null, null, null,
                new ErrorHandlerResolver(List.of()), Map.of());
    }

    private static final class RecordingMetrics implements StreamMetricsProvider {
        final List<String> stops = new CopyOnWriteArrayList<>();

        @Override
        public void onStreamStopped(String streamName, StopReason reason, Throwable cause) {
            stops.add(streamName + ":" + reason + ":"
                    + (cause != null ? cause.getClass().getSimpleName() : "null"));
        }

        @Override public void onStreamStarted(String streamName, StreamConfiguration config) {}
        @Override public void onStreamRestarted(String streamName, int attempt, Throwable cause) {}
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
