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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.flowwarden.stream.DeploymentMode;
import io.flowwarden.stream.FullDocumentBeforeChangeMode;
import io.flowwarden.stream.FullDocumentMode;
import io.flowwarden.stream.annotation.DeadLetterQueue;
import io.flowwarden.stream.annotation.MongoDlqOptions;
import io.flowwarden.stream.internal.discovery.ChangeStreamDefinition;
import io.flowwarden.stream.internal.discovery.ErrorHandlerResolver;
import io.flowwarden.stream.internal.discovery.StreamConfig;
import io.flowwarden.stream.internal.discovery.StreamRegistry;
import io.flowwarden.stream.internal.imperative.ImperativeStreamManager;
import io.flowwarden.stream.internal.reactive.ReactiveStreamManager;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.DlqPolicy;
import io.flowwarden.stream.spi.DlqStore;
import io.flowwarden.stream.spi.DlqStreamRegistration;
import io.flowwarden.stream.spi.FailedEvent;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Both managers announce every DLQ-enabled stream to the {@link DlqStore}
 * bean through {@link DlqStore#onStreamRegistered} — whatever the bean's
 * concrete type, not only the core's Mongo stores — and a hook that throws
 * is logged, not propagated.
 */
class DlqStreamRegistrationHookTest {

    @Test
    void imperativeManagerAnnouncesDlqStreamsToAnyStore() {
        RecordingDlqStore store = new RecordingDlqStore();
        MongoTemplateRegistry templateRegistry = mock(MongoTemplateRegistry.class);
        when(templateRegistry.getDefaultTemplate()).thenReturn(mock(MongoTemplate.class));
        when(templateRegistry.resolve(anyString())).thenReturn(mock(MongoTemplate.class));

        ImperativeStreamManager manager = new ImperativeStreamManager(
                templateRegistry, registry(), CheckpointStore.noOp(), store, null);
        try {
            manager.onApplicationReady();
        } finally {
            manager.shutdown();
        }

        assertRegistrations(store);
    }

    @Test
    void reactiveManagerAnnouncesDlqStreamsToAnyStore() {
        RecordingDlqStore store = new RecordingDlqStore();
        MongoTemplateRegistry templateRegistry = mock(MongoTemplateRegistry.class);
        when(templateRegistry.getDefaultReactiveTemplate()).thenReturn(mock(ReactiveMongoTemplate.class));
        when(templateRegistry.resolveReactive(anyString())).thenReturn(mock(ReactiveMongoTemplate.class));

        ReactiveStreamManager manager = new ReactiveStreamManager(
                templateRegistry, registry(), CheckpointStore.noOp(), store, null);
        try {
            manager.onApplicationReady();
        } finally {
            manager.shutdown();
        }

        assertRegistrations(store);
    }

    @Test
    void aThrowingHookIsLoggedAndTheOtherStreamsAreStillAnnounced() {
        RecordingDlqStore store = new RecordingDlqStore() {
            @Override
            public void onStreamRegistered(DlqStreamRegistration registration) {
                super.onStreamRegistered(registration);
                if ("with-collection".equals(registration.streamName())) {
                    throw new IllegalStateException("cannot provision");
                }
            }
        };
        MongoTemplateRegistry templateRegistry = mock(MongoTemplateRegistry.class);
        when(templateRegistry.getDefaultTemplate()).thenReturn(mock(MongoTemplate.class));

        Logger managerLogger = (Logger) LoggerFactory.getLogger(ImperativeStreamManager.class);
        ListAppender<ILoggingEvent> logs = new ListAppender<>();
        logs.start();
        managerLogger.addAppender(logs);

        ImperativeStreamManager manager = new ImperativeStreamManager(
                templateRegistry, registry(), CheckpointStore.noOp(), store, null);
        try {
            assertThatCode(manager::onApplicationReady).doesNotThrowAnyException();
        } finally {
            manager.shutdown();
            managerLogger.detachAppender(logs);
        }

        assertThat(store.registrations)
                .extracting(DlqStreamRegistration::streamName)
                .containsExactly("with-collection", "default-collection");

        // The failure is not swallowed silently: one WARN naming the stream,
        // the store and the cause, with the exception attached.
        assertThat(logs.list)
                .filteredOn(event -> event.getLevel() == Level.WARN
                        && event.getFormattedMessage().contains("failed to register stream"))
                .singleElement()
                .satisfies(event -> {
                    assertThat(event.getFormattedMessage())
                            .contains(store.getClass().getName())
                            .contains("with-collection")
                            .contains("orders_dlq")
                            .contains("cannot provision");
                    assertThat(event.getThrowableProxy()).isNotNull();
                    assertThat(event.getThrowableProxy().getClassName())
                            .isEqualTo(IllegalStateException.class.getName());
                });
    }

    private static void assertRegistrations(RecordingDlqStore store) {
        assertThat(store.registrations)
                .as("one registration per DLQ-enabled stream, none for the stream without DLQ")
                .extracting(DlqStreamRegistration::streamName)
                .containsExactly("with-collection", "default-collection");

        DlqStreamRegistration custom = store.registrations.get(0);
        assertThat(custom.policy()).isEqualTo(new DlqPolicy(7, false, true));
        assertThat(custom.mongoCollection()).isEqualTo("orders_dlq");
        assertThat(custom.hasMongoCollection()).isTrue();

        DlqStreamRegistration defaults = store.registrations.get(1);
        assertThat(defaults.policy()).isEqualTo(new DlqPolicy(30, true, true));
        assertThat(defaults.mongoCollection()).isEmpty();
        assertThat(defaults.hasMongoCollection()).isFalse();
    }

    /** Three streams: custom collection, default collection, no DLQ at all. */
    private static StreamRegistry registry() {
        DeadLetterQueue custom = AnnotationUtils.synthesizeAnnotation(
                Map.of("retentionDays", 7, "includeOriginalDocument", false),
                DeadLetterQueue.class, null);
        MongoDlqOptions options = AnnotationUtils.synthesizeAnnotation(
                Map.of("collection", "orders_dlq"), MongoDlqOptions.class, null);
        DeadLetterQueue defaults = AnnotationUtils.synthesizeAnnotation(
                Map.of(), DeadLetterQueue.class, null);

        List<ChangeStreamDefinition> defs = List.of(
                definition("with-collection", custom, options),
                definition("default-collection", defaults, null),
                definition("no-dlq", null, null));
        StreamRegistry registry = mock(StreamRegistry.class);
        when(registry.getDefinitions()).thenReturn(defs);
        // Registration only — no stream is actually started.
        when(registry.isAutoStartEnabled()).thenReturn(false);
        return registry;
    }

    private static ChangeStreamDefinition definition(String name, DeadLetterQueue dlq,
                                                     MongoDlqOptions options) {
        StreamConfig config = new StreamConfig(true, false, Document.class, "",
                FullDocumentMode.DEFAULT, FullDocumentBeforeChangeMode.OFF,
                DeploymentMode.ALL_INSTANCES);
        return new ChangeStreamDefinition(name, name + "_coll", "", "",
                new Object(), null, Map.of(), config, null, null,
                null, null, null, dlq, options,
                new ErrorHandlerResolver(List.of()), Map.of());
    }

    /** A non-Mongo store: the hook must reach it as it reaches any bean. */
    private static class RecordingDlqStore implements DlqStore {
        final List<DlqStreamRegistration> registrations = new CopyOnWriteArrayList<>();

        @Override
        public void onStreamRegistered(DlqStreamRegistration registration) {
            registrations.add(registration);
        }

        @Override
        public void save(FailedEvent event, DlqPolicy policy) {
        }
    }
}
