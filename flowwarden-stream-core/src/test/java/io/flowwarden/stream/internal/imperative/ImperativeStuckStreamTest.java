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
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.messaging.MessageListenerContainer;
import org.springframework.data.mongodb.core.messaging.Subscription;
import org.springframework.util.ErrorHandler;

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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Manager-level harness for the stuck-stream verdict (imperative): a live
 * subscription that never delivers while the heartbeat probe keeps finding
 * undelivered events is declared stuck, reported {@code CRASHED} with a
 * {@link StreamStuckException}, cancelled, and resubscribed by the restart
 * loop.
 */
class ImperativeStuckStreamTest {

    private static final String STREAM = "stuck-imperative";
    private static final BsonDocument SEED = BsonDocument.parse("{\"_data\": \"seed\"}");

    private final RecordingMetrics metrics = new RecordingMetrics();

    @AfterEach
    void tearDown() {
        FlowWardenMetrics.setProvider(StreamMetricsProvider.noOp());
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void hungCursor_isDeclaredStuck_crashedWithVerdict_cancelledAndResubscribed() {
        FlowWardenMetrics.setProvider(metrics);

        MongoTemplate template = mock(MongoTemplate.class);
        MongoTemplateRegistry templateRegistry = mock(MongoTemplateRegistry.class);
        when(templateRegistry.getDefaultTemplate()).thenReturn(template);
        when(templateRegistry.resolve(anyString())).thenReturn(template);
        StreamRegistry registry = mock(StreamRegistry.class);
        when(registry.findByName(STREAM)).thenReturn(Optional.of(definition()));

        // The cursor is "alive" from the container's point of view and never
        // delivers anything: the shape of a getMore hanging on a half-open
        // connection.
        Subscription hungSubscription = mock(Subscription.class);
        when(hungSubscription.isActive()).thenReturn(true);
        MessageListenerContainer container = mock(MessageListenerContainer.class);
        List<Object> requests = new CopyOnWriteArrayList<>();
        when(container.register(any(), any(Class.class), any(ErrorHandler.class)))
                .thenAnswer(invocation -> {
                    requests.add(invocation.getArgument(0));
                    return hungSubscription;
                });

        AtomicInteger probes = new AtomicInteger();
        ImperativeStreamManager manager = manager(templateRegistry, registry, container, probes);
        manager.startStream(STREAM);
        assertThat(manager.isRunning(STREAM)).isTrue();

        // stuckAfterProbes = 1 on a 1s idle threshold: the first IDLE probe
        // abstains and the verdict falls (the startup diagnostic probe
        // abstained too, and did not count).
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(metrics.stops).contains(STREAM + ":CRASHED:StreamStuckException"));
        assertThat(probes.get()).isGreaterThanOrEqualTo(1);

        // The hung subscription is cancelled best-effort, the state evicted…
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                verify(hungSubscription).cancel());
        assertThat(manager.isRunning(STREAM)).isFalse();

        // …and the restart loop resubscribes: a second registration.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                verify(container, times(2)).register(any(), any(Class.class), any(ErrorHandler.class)));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(metrics.restarts).contains(STREAM + ":1"));

        // RESUME: the restart generation goes through the cascade and resumes
        // from the certified position — nothing the hung cursor left behind
        // is skipped.
        assertThat(resumeTokenOf(requests.get(1)))
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

        MongoTemplate template = mock(MongoTemplate.class);
        when(template.getConverter()).thenReturn(mock(
                org.springframework.data.mongodb.core.convert.MongoConverter.class));
        MongoTemplateRegistry templateRegistry = mock(MongoTemplateRegistry.class);
        when(templateRegistry.getDefaultTemplate()).thenReturn(template);
        when(templateRegistry.resolve(anyString())).thenReturn(template);
        StreamRegistry registry = mock(StreamRegistry.class);
        when(registry.findByName(STREAM)).thenReturn(Optional.of(latestDefinitionWithHandler(new NoopBean())));

        Subscription hungSubscription = mock(Subscription.class);
        when(hungSubscription.isActive()).thenReturn(true);
        MessageListenerContainer container = mock(MessageListenerContainer.class);
        List<Object> requests = new CopyOnWriteArrayList<>();
        when(container.register(any(), any(Class.class), any(ErrorHandler.class)))
                .thenAnswer(invocation -> {
                    requests.add(invocation.getArgument(0));
                    return hungSubscription;
                });

        ImperativeStreamManager manager = manager(templateRegistry, registry, container, new AtomicInteger());
        manager.startStream(STREAM);
        assertThat(resumeTokenOf(requests.get(0))).as("LATEST never chains at start").isEmpty();

        // Under LATEST nothing is seeded: the probe has no position until an
        // event settles. One event gives it one, then the cursor "hangs".
        org.springframework.data.mongodb.core.messaging.MessageListener listener =
                ((org.springframework.data.mongodb.core.messaging.SubscriptionRequest) requests.get(0))
                        .getMessageListener();
        listener.onMessage(insertMessage());

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(metrics.stops).contains(STREAM + ":CRASHED:StreamStuckException"));
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(requests).hasSizeGreaterThanOrEqualTo(2));

        assertThat(resumeTokenOf(requests.get(1)))
                .as("under LATEST the resubscription starts from now — the persisted anchors are ignored")
                .isEmpty();

        manager.shutdown();
    }

    private ImperativeStreamManager manager(MongoTemplateRegistry templateRegistry, StreamRegistry registry,
                                            MessageListenerContainer container, AtomicInteger probes) {
        // Every probe finds matching events the stream has not delivered.
        HeartbeatProbe alwaysPending = new HeartbeatProbe() {
            @Override
            public ProbeOutcome probe(BsonDocument resumeAfter) {
                probes.incrementAndGet();
                return ProbeOutcome.eventPending();
            }

            @Override
            public ProbeOutcome probeFromOperationTime(org.bson.BsonTimestamp operationTime) {
                return ProbeOutcome.eventPending();
            }

            @Override
            public BsonDocument initialPosition() {
                return SEED; // the bootstrap position the idle probes chain from
            }
        };
        return new ImperativeStreamManager(
                templateRegistry, registry, CheckpointStore.noOp(), DlqStore.noOp(), null) {
            @Override
            MessageListenerContainer createContainer(MongoTemplate streamTemplate, String name) {
                return container;
            }

            @Override
            HeartbeatProbe createHeartbeatProbe(MongoTemplate streamTemplate, ChangeStreamDefinition def,
                                                List<Document> resolvedPipeline) {
                return alwaysPending;
            }
        };
    }

    private static java.util.Optional<org.bson.BsonValue> resumeTokenOf(Object request) {
        return ((org.springframework.data.mongodb.core.messaging.ChangeStreamRequest<?>) request)
                .getRequestOptions().getChangeStreamOptions().getResumeToken();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static org.springframework.data.mongodb.core.messaging.Message insertMessage() {
        com.mongodb.client.model.changestream.ChangeStreamDocument<Document> raw =
                mock(com.mongodb.client.model.changestream.ChangeStreamDocument.class);
        when(raw.getOperationType()).thenReturn(
                com.mongodb.client.model.changestream.OperationType.INSERT);
        when(raw.getResumeToken()).thenReturn(BsonDocument.parse("{\"_data\": \"token-insert\"}"));
        org.springframework.data.mongodb.core.messaging.Message message =
                mock(org.springframework.data.mongodb.core.messaging.Message.class);
        when(message.getRaw()).thenReturn(raw);
        return message;
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
        return new ChangeStreamDefinition(STREAM, "stuck_test", "", "",
                bean, onChange, Map.of(), config, null, null,
                LatestCheckpointCarrier.class.getAnnotation(
                        io.flowwarden.stream.annotation.Checkpoint.class),
                null, null, null, null,
                new ErrorHandlerResolver(List.of()), Map.of());
    }

    @io.flowwarden.stream.annotation.Checkpoint(idleHeartbeatIntervalSeconds = 1, stuckAfterProbes = 1)
    private static final class CheckpointAnnotationCarrier { }

    private static ChangeStreamDefinition definition() {
        StreamConfig config = new StreamConfig(true, false, Document.class, "",
                FullDocumentMode.DEFAULT, FullDocumentBeforeChangeMode.OFF,
                DeploymentMode.ALL_INSTANCES);
        return new ChangeStreamDefinition(STREAM, "stuck_test", "", "",
                new Object(), null, Map.of(), config, null, null,
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
