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
import io.flowwarden.stream.internal.discovery.ChangeStreamDefinition;
import io.flowwarden.stream.internal.discovery.ErrorHandlerResolver;
import io.flowwarden.stream.internal.discovery.StreamConfig;
import io.flowwarden.stream.internal.discovery.StreamRegistry;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.DlqStore;
import io.flowwarden.stream.spi.StopReason;
import io.flowwarden.stream.spi.StreamMetricsProvider;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Round-3 review coverage for the registration handshake: the container
 * submits the reading task before {@code register()} returns, so the
 * {@code ErrorHandler} can fire while {@code startStream} is still inside
 * {@code register()}. This unit forces exactly that window with an injected
 * container (undoable with a real Mongo): the handler must find the
 * pre-installed state, mark the generation terminated, and
 * {@code startStream} must then neither report the ghost stream as started
 * nor leave any state or schedule behind — only the pending restart.
 */
class ImperativeStartRegistrationHandshakeTest {

    private static final String STREAM = "handshake-test";

    private RecordingMetrics metrics = new RecordingMetrics();

    @AfterEach
    void tearDown() {
        FlowWardenMetrics.setProvider(StreamMetricsProvider.noOp());
    }

    @Test
    void asyncDeathBeforeRegisterReturns_noGhostStream_restartPending() throws Exception {
        // Spring's real path: register() submits the reading task to its
        // executor and only then returns — the ErrorHandler can fire on the
        // TASK thread while startStream still holds the lifecycle lock. The
        // termination signal is lock-free, so the startup observes it after
        // register() and publishes nothing; the death COMMIT (eviction +
        // restart hand-off) completes asynchronously under the lock once the
        // startup releases it.
        FlowWardenMetrics.setProvider(metrics);

        MongoTemplate template = mock(MongoTemplate.class);
        MongoTemplateRegistry templateRegistry = mock(MongoTemplateRegistry.class);
        when(templateRegistry.getDefaultTemplate()).thenReturn(template);
        when(templateRegistry.resolve(anyString())).thenReturn(template);

        ChangeStreamDefinition def = definition();
        StreamRegistry registry = mock(StreamRegistry.class);
        when(registry.findByName(STREAM)).thenReturn(Optional.of(def));

        RuntimeException openFailure = new RuntimeException("cursor open failed");
        Subscription deadSubscription = mock(Subscription.class);
        when(deadSubscription.isActive()).thenReturn(false);

        MessageListenerContainer container = mock(MessageListenerContainer.class);
        ImperativeStreamManager manager = new ImperativeStreamManager(
                templateRegistry, registry, CheckpointStore.noOp(), DlqStore.noOp(), null) {
            @Override
            MessageListenerContainer createContainer(MongoTemplate streamTemplate) {
                return container;
            }
        };
        java.util.List<Thread> taskThreads = new CopyOnWriteArrayList<>();
        when(container.register(any(), any(Class.class), any(ErrorHandler.class)))
                .thenAnswer(invocation -> {
                    ErrorHandler handler = invocation.getArgument(2);
                    // Second thread = the reading task; it publishes the
                    // lock-free CRASHED signal, then blocks on the lifecycle
                    // lock held by this very startStream.
                    Thread taskThread = new Thread(() -> handler.handleError(openFailure));
                    taskThreads.add(taskThread);
                    taskThread.start();
                    // Wait for the lock-free part of the death (the CRASHED
                    // emission) BEFORE letting register() return.
                    await().atMost(Duration.ofSeconds(5))
                            .until(() -> !metrics.stops.isEmpty());
                    return deadSubscription;
                });

        manager.startStream(STREAM);

        // The startup observed the lock-free signal: nothing published.
        assertThat(manager.isRunning(STREAM)).isFalse();
        assertThat(manager.hasLatestToken(STREAM)).isFalse();
        assertThat(manager.hasIntervalTask(STREAM)).isFalse();
        assertThat(manager.hasHeartbeat(STREAM)).isFalse();
        assertThat(metrics.started)
                .as("a ghost stream must never be reported as started")
                .isEmpty();
        assertThat(metrics.stops)
                .containsExactly(STREAM + ":CRASHED:" + openFailure.getClass().getSimpleName());

        // The asynchronous death commit completes and arms the restart.
        for (Thread t : taskThreads) {
            t.join(5_000);
        }
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(manager.isRestartPending(STREAM))
                        .as("the death was observed and handed to the restart loop")
                        .isTrue());

        manager.shutdown();
    }

    @Test
    void deathHandoffRacingOperatorStop_stopIsTheLastLifecycleOwner() throws Exception {
        // Round 4 blocker: a death thread suspended between its WON
        // termination claim and the restarter hand-off used to let a
        // concurrent stopStream complete (its cancel finding nothing), then
        // arm a restart after the stop. The whole death transition now runs
        // inside the lifecycle lock: the stop waits for it and kills the
        // just-armed restart.
        FlowWardenMetrics.setProvider(metrics);

        MongoTemplate template = mock(MongoTemplate.class);
        MongoTemplateRegistry templateRegistry = mock(MongoTemplateRegistry.class);
        when(templateRegistry.getDefaultTemplate()).thenReturn(template);
        when(templateRegistry.resolve(anyString())).thenReturn(template);
        StreamRegistry registry = mock(StreamRegistry.class);
        when(registry.findByName(STREAM)).thenReturn(Optional.of(definition()));

        Subscription liveSubscription = mock(Subscription.class);
        when(liveSubscription.isActive()).thenReturn(true);
        MessageListenerContainer container = mock(MessageListenerContainer.class);
        java.util.concurrent.atomic.AtomicReference<ErrorHandler> capturedHandler =
                new java.util.concurrent.atomic.AtomicReference<>();
        when(container.register(any(), any(Class.class), any(ErrorHandler.class)))
                .thenAnswer(invocation -> {
                    capturedHandler.set(invocation.getArgument(2));
                    return liveSubscription;
                });

        ImperativeStreamManager manager = new ImperativeStreamManager(
                templateRegistry, registry, CheckpointStore.noOp(), DlqStore.noOp(), null) {
            @Override
            MessageListenerContainer createContainer(MongoTemplate streamTemplate) {
                return container;
            }
        };
        manager.startStream(STREAM);
        assertThat(manager.isRunning(STREAM)).isTrue();

        java.util.concurrent.CountDownLatch hookEntered = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch hookGate = new java.util.concurrent.CountDownLatch(1);
        manager.deathHandoffTestHook = () -> {
            hookEntered.countDown();
            try {
                hookGate.await(10, java.util.concurrent.TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        Thread deathThread = new Thread(() ->
                capturedHandler.get().handleError(new RuntimeException("cursor died")));
        deathThread.start();
        assertThat(hookEntered.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();

        // The operator stop must block on the lifecycle lock held by the
        // suspended death transition…
        Thread stopThread = new Thread(() -> manager.stopStream(STREAM));
        stopThread.start();
        Thread.sleep(300);
        assertThat(stopThread.isAlive())
                .as("the stop waits for the death transition instead of racing it")
                .isTrue();

        // …and once released, kill the restart the hand-off just armed.
        hookGate.countDown();
        deathThread.join(5_000);
        stopThread.join(5_000);
        manager.deathHandoffTestHook = null;

        assertThat(manager.isRestartPending(STREAM))
                .as("the operator stop is the last lifecycle owner")
                .isFalse();
        assertThat(manager.isRunning(STREAM)).isFalse();
        Thread.sleep(1_500); // past the would-be attempt-1 backoff
        org.mockito.Mockito.verify(container, org.mockito.Mockito.times(1))
                .register(any(), any(Class.class), any(ErrorHandler.class));

        manager.shutdown();
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void listenerCrash_blockedMetricsProvider_subscriptionCancelledBeforeProviderReturns()
            throws Exception {
        // Round 6: a BLOCKED provider (not just a throwing one) must not
        // retain the fail-stop — the subscription cancel happens in onCrash,
        // under the lifecycle lock, BEFORE the wrapper enters the provider.
        metrics.stopsEntered = new java.util.concurrent.CountDownLatch(1);
        metrics.stopsGate = new java.util.concurrent.CountDownLatch(1);
        FlowWardenMetrics.setProvider(metrics);

        MongoTemplate template = mock(MongoTemplate.class);
        MongoTemplateRegistry templateRegistry = mock(MongoTemplateRegistry.class);
        when(templateRegistry.getDefaultTemplate()).thenReturn(template);
        when(templateRegistry.resolve(anyString())).thenReturn(template);
        StreamRegistry registry = mock(StreamRegistry.class);
        when(registry.findByName(STREAM)).thenReturn(Optional.of(definition()));

        Subscription liveSubscription = mock(Subscription.class);
        when(liveSubscription.isActive()).thenReturn(true);
        MessageListenerContainer container = mock(MessageListenerContainer.class);
        java.util.concurrent.atomic.AtomicReference<Object> capturedRequest =
                new java.util.concurrent.atomic.AtomicReference<>();
        when(container.register(any(), any(Class.class), any(ErrorHandler.class)))
                .thenAnswer(invocation -> {
                    capturedRequest.set(invocation.getArgument(0));
                    return liveSubscription;
                });

        ImperativeStreamManager manager = new ImperativeStreamManager(
                templateRegistry, registry, CheckpointStore.noOp(), DlqStore.noOp(), null) {
            @Override
            MessageListenerContainer createContainer(MongoTemplate streamTemplate) {
                return container;
            }
        };
        manager.startStream(STREAM);
        assertThat(manager.isRunning(STREAM)).isTrue();

        // Inject a listener crash: a message whose getRaw() throws escapes
        // the listener (wrapper path).
        org.springframework.data.mongodb.core.messaging.MessageListener listener =
                ((org.springframework.data.mongodb.core.messaging.SubscriptionRequest) capturedRequest.get())
                        .getMessageListener();
        org.springframework.data.mongodb.core.messaging.Message poison =
                mock(org.springframework.data.mongodb.core.messaging.Message.class);
        when(poison.getRaw()).thenThrow(new RuntimeException("listener boom"));
        Thread listenerThread = new Thread(() -> {
            try {
                listener.onMessage(poison);
            } catch (Throwable expected) {
                // the wrapper rethrows the marker — Spring would route it to
                // the ErrorHandler; irrelevant here, the wrapper is blocked
                // in the provider until the gate opens anyway.
            }
        });
        listenerThread.start();

        // The wrapper is now BLOCKED inside the provider. Before releasing
        // it, the fail-stop must already be complete.
        assertThat(metrics.stopsEntered.await(5, java.util.concurrent.TimeUnit.SECONDS))
                .isTrue();
        org.mockito.Mockito.verify(liveSubscription)
                .cancel();
        assertThat(manager.isRunning(STREAM)).isFalse();
        assertThat(manager.hasIntervalTask(STREAM)).isFalse();

        metrics.stopsGate.countDown();
        listenerThread.join(5_000);
        assertThat(manager.isRestartPending(STREAM))
                .as("listener crashes keep their fail-stop semantics — no restart")
                .isFalse();

        manager.shutdown();
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void throwingSubscriptionCancel_neverBlocksEvictionNorRestartability() throws Exception {
        // Round 7: Subscription.cancel() declares
        // DataAccessResourceFailureException — a failing physical close must
        // not skip the logical cleanup, or a ghost entry stays in the map
        // and blocks every later startStream ("already running").
        FlowWardenMetrics.setProvider(metrics);

        MongoTemplate template = mock(MongoTemplate.class);
        MongoTemplateRegistry templateRegistry = mock(MongoTemplateRegistry.class);
        when(templateRegistry.getDefaultTemplate()).thenReturn(template);
        when(templateRegistry.resolve(anyString())).thenReturn(template);
        StreamRegistry registry = mock(StreamRegistry.class);
        when(registry.findByName(STREAM)).thenReturn(Optional.of(definition()));

        Subscription failingSubscription = mock(Subscription.class);
        when(failingSubscription.isActive()).thenReturn(true);
        org.mockito.Mockito.doThrow(
                new org.springframework.dao.DataAccessResourceFailureException("close failed"))
                .when(failingSubscription).cancel();
        MessageListenerContainer container = mock(MessageListenerContainer.class);
        java.util.concurrent.atomic.AtomicReference<Object> capturedRequest =
                new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicReference<ErrorHandler> capturedHandler =
                new java.util.concurrent.atomic.AtomicReference<>();
        when(container.register(any(), any(Class.class), any(ErrorHandler.class)))
                .thenAnswer(invocation -> {
                    capturedRequest.set(invocation.getArgument(0));
                    capturedHandler.set(invocation.getArgument(2));
                    return failingSubscription;
                });

        ImperativeStreamManager manager = new ImperativeStreamManager(
                templateRegistry, registry, CheckpointStore.noOp(), DlqStore.noOp(), null) {
            @Override
            MessageListenerContainer createContainer(MongoTemplate streamTemplate) {
                return container;
            }
        };
        manager.startStream(STREAM);
        assertThat(manager.isRunning(STREAM)).isTrue();

        // Listener crash, marker routed to the ErrorHandler as Spring's
        // emitMessage would.
        org.springframework.data.mongodb.core.messaging.MessageListener listener =
                ((org.springframework.data.mongodb.core.messaging.SubscriptionRequest) capturedRequest.get())
                        .getMessageListener();
        org.springframework.data.mongodb.core.messaging.Message poison =
                mock(org.springframework.data.mongodb.core.messaging.Message.class);
        when(poison.getRaw()).thenThrow(new RuntimeException("listener boom"));
        Thread listenerThread = new Thread(() -> {
            try {
                listener.onMessage(poison);
            } catch (Throwable marker) {
                capturedHandler.get().handleError(marker);
            }
        });
        listenerThread.start();
        listenerThread.join(5_000);

        // The failing close never blocked the logical cleanup.
        assertThat(manager.hasIntervalTask(STREAM)).isFalse();
        assertThat(manager.hasLatestToken(STREAM)).isFalse();
        assertThat(manager.isRestartPending(STREAM)).isFalse();
        assertThat(manager.isRunning(STREAM)).isFalse();

        // Restartability is the discriminating assertion: a ghost entry
        // would make this second start refuse with "already running".
        manager.startStream(STREAM);
        org.mockito.Mockito.verify(container, org.mockito.Mockito.times(2))
                .register(any(), any(Class.class), any(ErrorHandler.class));
        assertThat(manager.isRunning(STREAM)).isTrue();

        manager.shutdown();
    }

    private static ChangeStreamDefinition definition() {
        StreamConfig config = new StreamConfig(true, false, Document.class, "",
                FullDocumentMode.DEFAULT, FullDocumentBeforeChangeMode.OFF,
                DeploymentMode.ALL_INSTANCES);
        return new ChangeStreamDefinition(STREAM, "handshake_test", "", "",
                new Object(), null, Map.of(), config, null, null,
                null, null, null, null,
                new ErrorHandlerResolver(List.of()), Map.of());
    }

    private static final class RecordingMetrics implements StreamMetricsProvider {
        final List<String> stops = new CopyOnWriteArrayList<>();
        final List<String> started = new CopyOnWriteArrayList<>();
        volatile java.util.concurrent.CountDownLatch stopsEntered;
        volatile java.util.concurrent.CountDownLatch stopsGate;

        @Override
        public void onStreamStopped(String streamName, StopReason reason, Throwable cause) {
            stops.add(streamName + ":" + reason + ":"
                    + (cause != null ? cause.getClass().getSimpleName() : "null"));
            if (stopsEntered != null) {
                stopsEntered.countDown();
                try {
                    stopsGate.await(10, java.util.concurrent.TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        @Override
        public void onStreamStarted(String streamName,
                io.flowwarden.stream.spi.StreamConfiguration cfg) {
            started.add(streamName);
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
