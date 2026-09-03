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
package io.flowwarden.stream.registration;

import io.flowwarden.stream.DeploymentMode;
import io.flowwarden.stream.FullDocumentBeforeChangeMode;
import io.flowwarden.stream.FullDocumentMode;
import io.flowwarden.stream.OperationType;
import io.flowwarden.stream.core.ContextHandler;
import io.flowwarden.stream.core.DocumentHandler;
import io.flowwarden.stream.core.ReactiveContextHandler;
import io.flowwarden.stream.core.ReactiveDocumentHandler;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Declarative specification of a Change Stream, contributed programmatically instead of
 * via {@link io.flowwarden.stream.annotation.ChangeStream @ChangeStream}.
 *
 * <p>Immutable and built only through {@link Builder}, obtained from
 * {@link StreamRegistration#stream(String, Class)}. Covers a deliberate <strong>subset</strong>
 * of the annotation model — not every {@code @ChangeStream} class can be expressed as a
 * {@code StreamSpec} yet:</p>
 *
 * <ul>
 *   <li>Covered, with the same defaults and handler shapes as the corresponding annotation:
 *       {@code @Checkpoint} ({@link CheckpointSpec}), {@code @RetryPolicy}
 *       ({@link RetryPolicySpec}), {@code @DeadLetterQueue} ({@link DeadLetterQueueSpec}),
 *       {@code @MongoDlqOptions} ({@link MongoDlqOptionsSpec}), and the typed handlers
 *       ({@code @OnInsert}/{@code @OnUpdate}/{@code @OnDelete}/{@code @OnReplace}/{@code @OnChange}).</li>
 *   <li><strong>Not covered</strong> (no equivalent on {@code StreamSpec} yet): a
 *       {@code @Pipeline}, a {@code @Filter}, {@code @OnError} handlers, or a {@code zone}.
 *       A stream declared with any of these can only be expressed via annotations.</li>
 * </ul>
 *
 * <p>What is covered goes through the exact same validation once contributed — see
 * {@link StreamDefinitionContributor}.</p>
 *
 * @param <T> the document type
 */
public final class StreamSpec<T> {

    private final String name;
    private final Class<T> documentType;
    private final String collection;
    private final String database;
    private final boolean enabled;
    private final boolean autoStart;
    private final FullDocumentMode fullDocument;
    private final FullDocumentBeforeChangeMode fullDocumentBeforeChange;
    private final DeploymentMode deploymentMode;
    private final String mongoTemplateRef;
    private final CheckpointSpec checkpoint;
    private final RetryPolicySpec retryPolicy;
    private final DeadLetterQueueSpec deadLetterQueue;
    private final MongoDlqOptionsSpec mongoDlqOptions;
    private final Map<OperationType, TypedHandler<T>> typedHandlers;
    private final TypedHandler<T> onChangeHandler;

    private StreamSpec(Builder<T> builder) {
        this.name = builder.name;
        this.documentType = builder.documentType;
        this.collection = builder.collection;
        this.database = builder.database;
        this.enabled = builder.enabled;
        this.autoStart = builder.autoStart;
        this.fullDocument = builder.fullDocument;
        this.fullDocumentBeforeChange = builder.fullDocumentBeforeChange;
        this.deploymentMode = builder.deploymentMode;
        this.mongoTemplateRef = builder.mongoTemplateRef;
        this.checkpoint = builder.checkpoint;
        this.retryPolicy = builder.retryPolicy;
        this.deadLetterQueue = builder.deadLetterQueue;
        this.mongoDlqOptions = builder.mongoDlqOptions;
        this.typedHandlers = new EnumMap<>(builder.typedHandlers);
        this.onChangeHandler = builder.onChangeHandler;
    }

    public String name() {
        return name;
    }

    public Class<T> documentType() {
        return documentType;
    }

    public String collection() {
        return collection;
    }

    public String database() {
        return database;
    }

    public boolean enabled() {
        return enabled;
    }

    public boolean autoStart() {
        return autoStart;
    }

    public FullDocumentMode fullDocument() {
        return fullDocument;
    }

    public FullDocumentBeforeChangeMode fullDocumentBeforeChange() {
        return fullDocumentBeforeChange;
    }

    public DeploymentMode deploymentMode() {
        return deploymentMode;
    }

    public String mongoTemplateRef() {
        return mongoTemplateRef;
    }

    public Optional<CheckpointSpec> checkpoint() {
        return Optional.ofNullable(checkpoint);
    }

    public Optional<RetryPolicySpec> retryPolicy() {
        return Optional.ofNullable(retryPolicy);
    }

    public Optional<DeadLetterQueueSpec> deadLetterQueue() {
        return Optional.ofNullable(deadLetterQueue);
    }

    public Optional<MongoDlqOptionsSpec> mongoDlqOptions() {
        return Optional.ofNullable(mongoDlqOptions);
    }

    /** The typed handler bound to {@code operationType} (insert/update/delete/replace), if any. */
    public Optional<TypedHandler<T>> handler(OperationType operationType) {
        return Optional.ofNullable(typedHandlers.get(operationType));
    }

    /** All typed handlers, keyed by operation type (unmodifiable). */
    public Map<OperationType, TypedHandler<T>> typedHandlers() {
        return Map.copyOf(typedHandlers);
    }

    /** The catch-all handler, if any. Always a {@link TypedHandler.Context} or {@link TypedHandler.ReactiveContext}. */
    public Optional<TypedHandler<T>> onChangeHandler() {
        return Optional.ofNullable(onChangeHandler);
    }

    /**
     * Starts building a {@link StreamSpec}. Prefer {@link StreamRegistration#stream(String, Class)}
     * over calling this directly — it tracks the built spec for you.
     */
    public static <T> Builder<T> builder(String name, Class<T> documentType) {
        return new Builder<>(name, documentType);
    }

    /** Builder for {@link StreamSpec}. Not thread-safe; build and discard within one contributor call. */
    public static final class Builder<T> {

        private final String name;
        private final Class<T> documentType;
        private String collection = "";
        private String database = "";
        private boolean enabled = true;
        private boolean autoStart = true;
        private FullDocumentMode fullDocument = FullDocumentMode.DEFAULT;
        private FullDocumentBeforeChangeMode fullDocumentBeforeChange = FullDocumentBeforeChangeMode.OFF;
        private DeploymentMode deploymentMode = DeploymentMode.ALL_INSTANCES;
        private String mongoTemplateRef = "";
        private CheckpointSpec checkpoint;
        private RetryPolicySpec retryPolicy;
        private DeadLetterQueueSpec deadLetterQueue;
        private MongoDlqOptionsSpec mongoDlqOptions;
        private final Map<OperationType, TypedHandler<T>> typedHandlers = new EnumMap<>(OperationType.class);
        private TypedHandler<T> onChangeHandler;

        Builder(String name, Class<T> documentType) {
            this.name = Objects.requireNonNull(name, "name must not be null");
            this.documentType = Objects.requireNonNull(documentType, "documentType must not be null");
            if (name.isBlank()) {
                throw new IllegalArgumentException("name must not be blank");
            }
        }

        public Builder<T> collection(String collection) {
            this.collection = Objects.requireNonNull(collection, "collection must not be null");
            return this;
        }

        public Builder<T> database(String database) {
            this.database = Objects.requireNonNull(database, "database must not be null");
            return this;
        }

        public Builder<T> mongoTemplateRef(String mongoTemplateRef) {
            this.mongoTemplateRef = Objects.requireNonNull(mongoTemplateRef, "mongoTemplateRef must not be null");
            return this;
        }

        public Builder<T> enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder<T> autoStart(boolean autoStart) {
            this.autoStart = autoStart;
            return this;
        }

        public Builder<T> fullDocument(FullDocumentMode fullDocument) {
            this.fullDocument = Objects.requireNonNull(fullDocument, "fullDocument must not be null");
            return this;
        }

        public Builder<T> fullDocumentBeforeChange(FullDocumentBeforeChangeMode fullDocumentBeforeChange) {
            this.fullDocumentBeforeChange = Objects.requireNonNull(fullDocumentBeforeChange,
                    "fullDocumentBeforeChange must not be null");
            return this;
        }

        public Builder<T> deploymentMode(DeploymentMode deploymentMode) {
            this.deploymentMode = Objects.requireNonNull(deploymentMode, "deploymentMode must not be null");
            return this;
        }

        public Builder<T> checkpoint(CheckpointSpec checkpoint) {
            this.checkpoint = Objects.requireNonNull(checkpoint, "checkpoint must not be null");
            return this;
        }

        public Builder<T> retryPolicy(RetryPolicySpec retryPolicy) {
            this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy must not be null");
            return this;
        }

        public Builder<T> deadLetterQueue(DeadLetterQueueSpec deadLetterQueue) {
            this.deadLetterQueue = Objects.requireNonNull(deadLetterQueue, "deadLetterQueue must not be null");
            return this;
        }

        public Builder<T> mongoDlqOptions(MongoDlqOptionsSpec mongoDlqOptions) {
            this.mongoDlqOptions = Objects.requireNonNull(mongoDlqOptions, "mongoDlqOptions must not be null");
            return this;
        }

        public Builder<T> onInsert(ContextHandler<T> handler) {
            return setTypedHandler(OperationType.INSERT, new TypedHandler.Context<>(handler));
        }

        public Builder<T> onInsert(DocumentHandler<T> handler) {
            return setTypedHandler(OperationType.INSERT, new TypedHandler.Document<>(handler));
        }

        public Builder<T> onInsertReactive(ReactiveContextHandler<T> handler) {
            return setTypedHandler(OperationType.INSERT, new TypedHandler.ReactiveContext<>(handler));
        }

        public Builder<T> onInsertReactive(ReactiveDocumentHandler<T> handler) {
            return setTypedHandler(OperationType.INSERT, new TypedHandler.ReactiveDocument<>(handler));
        }

        public Builder<T> onUpdate(ContextHandler<T> handler) {
            return setTypedHandler(OperationType.UPDATE, new TypedHandler.Context<>(handler));
        }

        public Builder<T> onUpdate(DocumentHandler<T> handler) {
            return setTypedHandler(OperationType.UPDATE, new TypedHandler.Document<>(handler));
        }

        public Builder<T> onUpdateReactive(ReactiveContextHandler<T> handler) {
            return setTypedHandler(OperationType.UPDATE, new TypedHandler.ReactiveContext<>(handler));
        }

        public Builder<T> onUpdateReactive(ReactiveDocumentHandler<T> handler) {
            return setTypedHandler(OperationType.UPDATE, new TypedHandler.ReactiveDocument<>(handler));
        }

        public Builder<T> onDelete(ContextHandler<T> handler) {
            return setTypedHandler(OperationType.DELETE, new TypedHandler.Context<>(handler));
        }

        public Builder<T> onDelete(DocumentHandler<T> handler) {
            return setTypedHandler(OperationType.DELETE, new TypedHandler.Document<>(handler));
        }

        public Builder<T> onDeleteReactive(ReactiveContextHandler<T> handler) {
            return setTypedHandler(OperationType.DELETE, new TypedHandler.ReactiveContext<>(handler));
        }

        public Builder<T> onDeleteReactive(ReactiveDocumentHandler<T> handler) {
            return setTypedHandler(OperationType.DELETE, new TypedHandler.ReactiveDocument<>(handler));
        }

        public Builder<T> onReplace(ContextHandler<T> handler) {
            return setTypedHandler(OperationType.REPLACE, new TypedHandler.Context<>(handler));
        }

        public Builder<T> onReplace(DocumentHandler<T> handler) {
            return setTypedHandler(OperationType.REPLACE, new TypedHandler.Document<>(handler));
        }

        public Builder<T> onReplaceReactive(ReactiveContextHandler<T> handler) {
            return setTypedHandler(OperationType.REPLACE, new TypedHandler.ReactiveContext<>(handler));
        }

        public Builder<T> onReplaceReactive(ReactiveDocumentHandler<T> handler) {
            return setTypedHandler(OperationType.REPLACE, new TypedHandler.ReactiveDocument<>(handler));
        }

        /** Catch-all handler, invoked for any operation type without a more specific typed handler. */
        public Builder<T> onChange(ContextHandler<T> handler) {
            requireOnChangeUnset();
            this.onChangeHandler = new TypedHandler.Context<>(Objects.requireNonNull(handler, "handler must not be null"));
            return this;
        }

        /** Reactive variant of {@link #onChange(ContextHandler)}. */
        public Builder<T> onChangeReactive(ReactiveContextHandler<T> handler) {
            requireOnChangeUnset();
            this.onChangeHandler = new TypedHandler.ReactiveContext<>(
                    Objects.requireNonNull(handler, "handler must not be null"));
            return this;
        }

        public StreamSpec<T> build() {
            return new StreamSpec<>(this);
        }

        private Builder<T> setTypedHandler(OperationType operationType, TypedHandler<T> handler) {
            if (typedHandlers.containsKey(operationType)) {
                throw new IllegalStateException(
                        "A handler for " + operationType + " is already registered on stream '" + name + "'");
            }
            typedHandlers.put(operationType, handler);
            return this;
        }

        private void requireOnChangeUnset() {
            if (onChangeHandler != null) {
                throw new IllegalStateException("An onChange handler is already registered on stream '" + name + "'");
            }
        }
    }
}
