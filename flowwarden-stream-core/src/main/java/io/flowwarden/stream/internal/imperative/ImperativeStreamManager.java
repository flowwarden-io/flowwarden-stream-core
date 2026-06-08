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

import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import io.flowwarden.stream.DeploymentMode;
import io.flowwarden.stream.ErrorAction;
import io.flowwarden.stream.FlowWardenMetrics;
import io.flowwarden.stream.FullDocumentBeforeChangeMode;
import io.flowwarden.stream.FullDocumentMode;
import io.flowwarden.stream.HistoryLostException;
import io.flowwarden.stream.OnHistoryLost;
import io.flowwarden.stream.OperationType;
import io.flowwarden.stream.ResumeStrategy;
import io.flowwarden.stream.StartPosition;
import io.flowwarden.stream.core.FlowWardenStreamManager;
import io.flowwarden.stream.annotation.DeadLetterQueue;
import io.flowwarden.stream.annotation.MongoDlqOptions;
import io.flowwarden.stream.internal.DefaultChangeStreamContext;
import io.flowwarden.stream.internal.MongoTemplateRegistry;
import io.flowwarden.stream.internal.discovery.ChangeStreamDefinition;
import io.flowwarden.stream.internal.dlq.MongoDlqStore;
import io.flowwarden.stream.internal.retry.RetryPolicyConfig;
import io.flowwarden.stream.internal.discovery.StreamRegistry;
import io.flowwarden.stream.internal.discovery.HandlerMethod;
import io.flowwarden.stream.internal.discovery.PipelineMethod;
import io.flowwarden.stream.internal.lock.LeaderElectionCoordinator;
import io.flowwarden.stream.spi.ChangeEventMetadata;
import io.flowwarden.stream.spi.StopReason;
import io.flowwarden.stream.spi.StreamConfiguration;
import io.flowwarden.stream.internal.checkpoint.TokenSnapshot;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.DlqPolicy;
import io.flowwarden.stream.spi.DlqStore;
import io.flowwarden.stream.spi.FailedEvent;
import jakarta.annotation.PreDestroy;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.messaging.ChangeStreamRequest;
import org.springframework.data.mongodb.core.messaging.DefaultMessageListenerContainer;
import org.springframework.data.mongodb.core.messaging.Message;
import org.springframework.data.mongodb.core.messaging.MessageListener;
import org.springframework.data.mongodb.core.messaging.MessageListenerContainer;
import org.springframework.data.mongodb.core.messaging.Subscription;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages Change Stream subscriptions in IMPERATIVE mode using
 * Spring Data MongoDB's {@link MessageListenerContainer}.
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public class ImperativeStreamManager implements FlowWardenStreamManager {

    private static final Logger log = LoggerFactory.getLogger(ImperativeStreamManager.class);

    private final MongoTemplateRegistry templateRegistry;
    private final MongoTemplate defaultTemplate;
    private final StreamRegistry registry;
    private final CheckpointStore checkpointStore;
    private final DlqStore dlqStore;
    private final LeaderElectionCoordinator leaderElection; // nullable
    private final Map<String, StreamState> streams = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> eventCounters = new ConcurrentHashMap<>();
    private final Map<String, AtomicReference<TokenSnapshot>> latestTokens = new ConcurrentHashMap<>();
    private final Map<String, Instant> lastActivityTimes = new ConcurrentHashMap<>();
    private final Map<String, ScheduledFuture<?>> intervalTasks = new ConcurrentHashMap<>();
    private final ScheduledExecutorService intervalScheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "fw-checkpoint-interval");
                t.setDaemon(true);
                return t;
            });

    private record StreamState(
            MessageListenerContainer container,
            Subscription subscription,
            ChangeStreamDefinition definition) {
    }

    public ImperativeStreamManager(MongoTemplateRegistry templateRegistry,
                                   StreamRegistry registry,
                                   CheckpointStore checkpointStore,
                                   DlqStore dlqStore,
                                   LeaderElectionCoordinator leaderElection) {
        this.templateRegistry = templateRegistry;
        this.defaultTemplate = templateRegistry.getDefaultTemplate();
        this.registry = registry;
        this.checkpointStore = checkpointStore;
        this.dlqStore = dlqStore;
        this.leaderElection = leaderElection;
    }

    private MongoTemplate templateFor(ChangeStreamDefinition def) {
        return templateRegistry.resolve(def.config().mongoTemplateRef());
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        registerDlqCollections();
        for (ChangeStreamDefinition def : registry.getDefinitions()) {
            if (!def.config().autoStart() || !def.config().enabled()) {
                continue;
            }
            if (def.config().deploymentMode() == DeploymentMode.SINGLE_LEADER && leaderElection != null) {
                String name = def.streamName();
                leaderElection.startElection(name,
                        () -> startStream(name),
                        () -> stopStream(name));
            } else {
                startStream(def.streamName());
            }
        }
        collectOplogStats();
        scheduleOplogStatsRefresh();
    }

    @Override
    public void startStream(String streamName) {
        if (streams.containsKey(streamName)) {
            log.warn("Stream '{}' is already running", streamName);
            return;
        }

        ChangeStreamDefinition def = findDefinition(streamName);

        MongoTemplate streamTemplate = templateFor(def);
        MessageListenerContainer container = new DefaultMessageListenerContainer(streamTemplate);

        MessageListener<ChangeStreamDocument<Document>, Document> listener =
                new FlowWardenMessageListenerWrapper(
                        message -> handleMessage(message, def), def.streamName());
        ChangeStreamRequest.ChangeStreamRequestBuilder<Document> builder = ChangeStreamRequest.builder()
                .collection(def.collection())
                .publishTo(listener);

        if (def.checkpointAnnotation() != null
                && def.checkpointAnnotation().startPosition() == StartPosition.RESUME) {
            applyResumeCascade(streamName, def, builder, streamTemplate);
        }

        if (def.config().fullDocument() != FullDocumentMode.DEFAULT) {
            builder.fullDocumentLookup(FullDocument.valueOf(def.config().fullDocument().name()));
        }
        if (def.config().fullDocumentBeforeChange() != FullDocumentBeforeChangeMode.OFF) {
            builder.fullDocumentBeforeChangeLookup(
                    FullDocumentBeforeChange.valueOf(def.config().fullDocumentBeforeChange().name()));
        }

        if (def.pipelineMethod() != null) {
            List<Document> pipeline = def.pipelineMethod().resolve(def.bean());
            builder.filter(pipeline.toArray(new Document[0]));
            log.debug("Applied @Pipeline with {} stages to stream '{}'", pipeline.size(), streamName);
        }

        ChangeStreamRequest<Document> request = builder.build();

        container.start();
        Subscription subscription = container.register(request, Document.class);

        streams.put(streamName, new StreamState(container, subscription, def));
        scheduleIntervalCheckpoint(def);
        log.info("Started Change Stream '{}' on collection '{}'", streamName, def.collection());

        FlowWardenMetrics.get().onStreamStarted(streamName, buildStreamConfiguration(def, "IMPERATIVE"));
    }

    @Override
    public void stopStream(String streamName) {
        ScheduledFuture<?> task = intervalTasks.remove(streamName);
        if (task != null) {
            task.cancel(false);
        }
        latestTokens.remove(streamName);

        StreamState state = streams.remove(streamName);
        if (state == null) {
            log.warn("Stream '{}' is not running", streamName);
            return;
        }

        try {
            state.subscription().cancel();
            state.container().stop();
        } catch (Exception e) {
            log.warn("Error stopping stream '{}'", streamName, e);
        }

        FlowWardenMetrics.get().onStreamStopped(streamName, StopReason.GRACEFUL, null);
        log.info("Stopped Change Stream '{}'", streamName);
    }

    @Override
    public boolean isRunning(String streamName) {
        StreamState state = streams.get(streamName);
        return state != null && state.subscription().isActive();
    }

    @Override
    public Instant getLastEventTime(String streamName) {
        return lastActivityTimes.get(streamName);
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down FlowWarden ImperativeStreamManager ({} streams)", streams.size());
        if (leaderElection != null) {
            leaderElection.shutdown();
        }
        for (String streamName : streams.keySet()) {
            stopStream(streamName);
        }
        intervalScheduler.shutdownNow();
    }

    @SuppressWarnings("unchecked")
    private void handleMessage(Message<ChangeStreamDocument<Document>, Document> message,
                               ChangeStreamDefinition def) {
        ChangeStreamDocument<Document> raw = message.getRaw();
        if (raw == null) {
            return;
        }

        // Track the latest token for periodic checkpoint timer
        if (def.checkpointAnnotation() != null && raw.getResumeToken() != null) {
            latestTokens.computeIfAbsent(def.streamName(), k -> new AtomicReference<>())
                    .set(new TokenSnapshot(raw.getResumeToken(), Instant.now()));
        }

        DefaultChangeStreamContext<Document> ctx = new DefaultChangeStreamContext<>(
                raw, def.streamName(), DefaultChangeStreamContext.NOOP_ACTIONS,
                templateFor(def).getConverter());

        DefaultChangeStreamContext.ContextActions actions = def.checkpointAnnotation() != null
                ? new DefaultChangeStreamContext.ContextActions() {
                    @Override
                    public void sendToDlq(String reason) {
                        DeadLetterQueue dlqAnn = def.deadLetterQueueAnnotation();
                        DlqPolicy policy = dlqAnn != null
                                ? DlqPolicy.fromAnnotation(dlqAnn)
                                : new DlqPolicy(0, true, true);
                        Document fullDocument = policy.includeOriginalDocument()
                                ? raw.getFullDocument() : null;
                        Instant expiresAt = policy.computeExpiresAt(Instant.now());
                        try {
                            dlqStore.save(new FailedEvent(
                                    ctx.getEventId(), def.streamName(),
                                    ctx.getOperationType() != null ? ctx.getOperationType().name() : null,
                                    raw.getDocumentKey(), fullDocument,
                                    raw.getResumeToken(),
                                    new FailedEvent.ErrorInfo("ManualDlq", reason, null),
                                    ctx.getAttemptNumber(), FailedEvent.STATUS_PENDING,
                                    Instant.now(), Instant.now(), Instant.now(),
                                    expiresAt, ctx.getAllMetadata()), policy);
                            FlowWardenMetrics.get().onEventSentToDlq(def.streamName());
                        } catch (RuntimeException e) {
                            FlowWardenMetrics.get().onEventDlqFailed(def.streamName(), e);
                            log.warn("Failed to send manual DLQ event for stream '{}': {}",
                                    def.streamName(), e.getMessage(), e);
                            throw e;
                        }
                    }

                    @Override
                    public void saveCheckpointNow() {
                        BsonDocument token = raw.getResumeToken();
                        if (token != null) {
                            try {
                                checkpointStore.save(new io.flowwarden.stream.spi.Checkpoint(
                                        def.streamName(), null, token, Instant.now(),
                                        token, Instant.now(), Collections.emptyMap()));
                                FlowWardenMetrics.get().onCheckpoint(def.streamName(), token.toJson());
                            } catch (RuntimeException e) {
                                FlowWardenMetrics.get().onCheckpointFailed(def.streamName(), e);
                                log.warn("Failed to save manual checkpoint for stream '{}': {}",
                                        def.streamName(), e.getMessage(), e);
                            }
                        }
                    }
                }
                : DefaultChangeStreamContext.NOOP_ACTIONS;

        ctx.setActions(actions);

        // Metrics: event received
        ChangeEventMetadata eventMetadata = new ChangeEventMetadata(
                ctx.getEventId(),
                ctx.getOperationType(),
                ctx.getCollectionName(),
                ctx.getDocumentKey() != null ? ctx.getDocumentKey().toString() : null,
                ctx.getClusterTime(),
                ctx.getWallTime());
        FlowWardenMetrics.get().onEventReceived(def.streamName(), eventMetadata);

        HandlerMethod handler = def.resolveHandler(ctx.getOperationType());
        if (handler == null) {
            log.debug("No handler for {} in stream '{}'", ctx.getOperationType(), def.streamName());
            return;
        }

        if (def.filterMethod() != null) {
            if (!def.filterMethod().evaluate(def.bean(), ctx)) {
                log.debug("Event filtered by @Filter in stream '{}'", def.streamName());
                return;
            }
        }

        RetryPolicyConfig retryConfig = def.retryPolicyAnnotation() != null
                ? RetryPolicyConfig.fromAnnotation(def.retryPolicyAnnotation()) : null;

        long startNanos = System.nanoTime();
        boolean success = false;
        int attempt = 1;

        outerLoop:
        while (true) {
            ctx.setAttemptNumber(attempt);
            try {
                Document rawDoc = raw.getFullDocument();
                handler.invoke(def.bean(), ctx, rawDoc, templateFor(def).getConverter(),
                        def.config().documentType());
                success = true;
                if (!ctx.isCheckpointSavedManually()) {
                    saveCheckpointIfNeeded(def, raw.getResumeToken(), ctx.getClusterTime());
                }
                break;
            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause() != null ? e.getCause() : e;

                // REQ-015: consult @OnError handlers before standard retry/DLQ
                ErrorAction action = resolveErrorAction(def, cause, ctx);

                switch (action) {
                    case SKIP -> {
                        log.warn("@OnError SKIP for stream '{}': {}", def.streamName(), cause.getMessage());
                        FlowWardenMetrics.get().onEventError(def.streamName(), cause, false, attempt, eventMetadata);
                        break outerLoop;
                    }
                    case DLQ -> {
                        log.warn("@OnError DLQ for stream '{}': {}", def.streamName(), cause.getMessage());
                        FlowWardenMetrics.get().onEventError(def.streamName(), cause, false, attempt, eventMetadata);
                        sendToDlqAfterExhaustion(def, ctx, raw, cause, attempt);
                        break outerLoop;
                    }
                    case RETRY -> {
                        if (retryConfig != null && attempt < retryConfig.maxAttempts()) {
                            FlowWardenMetrics.get().onEventError(def.streamName(), cause, true, attempt, eventMetadata);
                            long delayMs = retryConfig.computeDelayMillis(attempt);
                            try {
                                Thread.sleep(delayMs);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                log.error("Retry interrupted for stream '{}'", def.streamName());
                                FlowWardenMetrics.get().onEventError(def.streamName(), cause, false, attempt, eventMetadata);
                                break outerLoop;
                            }
                            attempt++;
                            continue outerLoop;
                        }
                        // maxAttempts exhausted or no @RetryPolicy → DLQ
                        log.warn("@OnError RETRY exhausted for stream '{}': {}", def.streamName(), cause.getMessage());
                        FlowWardenMetrics.get().onEventError(def.streamName(), cause, false, attempt, eventMetadata);
                        sendToDlqAfterExhaustion(def, ctx, raw, cause, attempt);
                        break outerLoop;
                    }
                    case RETHROW -> {
                        // Fall through to standard retry/DLQ logic
                    }
                }

                // Standard retry/DLQ logic (RETHROW or no @OnError match)
                if (retryConfig != null && retryConfig.shouldRetry(cause, attempt)) {
                    FlowWardenMetrics.get().onEventError(def.streamName(), cause, true, attempt, eventMetadata);
                    long delayMs = retryConfig.computeDelayMillis(attempt);
                    log.warn("Retry {}/{} for stream '{}' after {}ms: {}",
                            attempt, retryConfig.maxAttempts(), def.streamName(),
                            delayMs, cause.getMessage());
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        log.error("Retry interrupted for stream '{}'", def.streamName());
                        FlowWardenMetrics.get().onEventError(def.streamName(), cause, false, attempt, eventMetadata);
                        break;
                    }
                    attempt++;
                } else {
                    log.error("Error in handler {} for stream '{}': {}",
                            handler, def.streamName(), cause.getMessage(), cause);
                    FlowWardenMetrics.get().onEventError(def.streamName(), cause, false, attempt, eventMetadata);
                    sendToDlqAfterExhaustion(def, ctx, raw, cause, attempt);
                    break;
                }
            } catch (IllegalAccessException e) {
                log.error("Cannot invoke handler for stream '{}'", def.streamName(), e);
                FlowWardenMetrics.get().onEventError(def.streamName(), e, false, attempt, eventMetadata);
                sendToDlqAfterExhaustion(def, ctx, raw, e, attempt);
                break;
            }
        }

        long durationNanos = System.nanoTime() - startNanos;
        FlowWardenMetrics.get().onEventProcessed(def.streamName(), durationNanos, success);
        lastActivityTimes.put(def.streamName(), Instant.now());
    }

    private ErrorAction resolveErrorAction(ChangeStreamDefinition def, Throwable cause,
                                           DefaultChangeStreamContext<?> ctx) {
        if (def.errorHandlerResolver().isEmpty()) {
            return ErrorAction.RETHROW;
        }
        return def.errorHandlerResolver()
                .resolveAndInvoke(def.bean(), cause, ctx)
                .orElse(ErrorAction.RETHROW);
    }

    private void sendToDlqAfterExhaustion(ChangeStreamDefinition def,
                                             DefaultChangeStreamContext<Document> ctx,
                                             ChangeStreamDocument<Document> raw,
                                             Throwable cause,
                                             int attempt) {
        DeadLetterQueue dlqAnn = def.deadLetterQueueAnnotation();
        if (dlqAnn == null || !dlqAnn.enabled()) {
            return;
        }
        try {
            DlqPolicy policy = DlqPolicy.fromAnnotation(dlqAnn);
            Instant now = Instant.now();
            Document fullDocument = policy.includeOriginalDocument() ? raw.getFullDocument() : null;
            String stackTrace = policy.includeStackTrace() ? getStackTrace(cause) : null;

            FailedEvent failedEvent = new FailedEvent(
                    ctx.getEventId(), def.streamName(),
                    ctx.getOperationType() != null ? ctx.getOperationType().name() : null,
                    raw.getDocumentKey(), fullDocument,
                    raw.getResumeToken(),
                    new FailedEvent.ErrorInfo(cause.getClass().getName(), cause.getMessage(), stackTrace),
                    attempt, FailedEvent.STATUS_PENDING,
                    now, now, now,
                    policy.computeExpiresAt(now), ctx.getAllMetadata());

            dlqStore.save(failedEvent, policy);
            FlowWardenMetrics.get().onEventSentToDlq(def.streamName());
            log.info("Event sent to DLQ for stream '{}'", def.streamName());
        } catch (Exception e) {
            FlowWardenMetrics.get().onEventDlqFailed(def.streamName(), e);
            log.error("Failed to send event to DLQ for stream '{}': {}", def.streamName(), e.getMessage(), e);
        }
    }

    private void registerDlqCollections() {
        if (!(dlqStore instanceof MongoDlqStore mongoStore)) {
            return;
        }
        for (ChangeStreamDefinition def : registry.getDefinitions()) {
            if (def.deadLetterQueueAnnotation() == null) {
                continue;
            }
            MongoDlqOptions opts = def.mongoDlqOptionsAnnotation();
            String collection = opts != null ? opts.collection() : "";
            mongoStore.registerStream(def.streamName(), collection);
        }
    }

    private static String getStackTrace(Throwable t) {
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    private void scheduleIntervalCheckpoint(ChangeStreamDefinition def) {
        if (def.checkpointAnnotation() == null) {
            return;
        }
        int intervalSeconds = def.checkpointAnnotation().saveIntervalSeconds();
        if (intervalSeconds <= 0) {
            return;
        }
        String streamName = def.streamName();
        ScheduledFuture<?> future = intervalScheduler.scheduleAtFixedRate(() -> {
            try {
                AtomicReference<TokenSnapshot> ref = latestTokens.get(streamName);
                if (ref == null) {
                    return;
                }
                TokenSnapshot snapshot = ref.get();
                if (snapshot == null) {
                    return;
                }
                // The timer advances ONLY lastSeenToken. lastProcessedToken is
                // managed by saveCheckpointIfNeeded after confirmed handler success.
                checkpointStore.saveSeen(streamName, snapshot.token(), snapshot.timestamp());
                FlowWardenMetrics.get().onCheckpoint(streamName, snapshot.token().toJson());
                log.debug("Periodic lastSeenToken update for stream '{}'", streamName);
            } catch (Exception e) {
                FlowWardenMetrics.get().onCheckpointFailed(streamName, e);
                log.warn("Failed to save periodic checkpoint for stream '{}': {}",
                        streamName, e.getMessage(), e);
            }
        }, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
        intervalTasks.put(streamName, future);
    }

    private void saveCheckpointIfNeeded(ChangeStreamDefinition def, BsonDocument token, Instant timestamp) {
        if (def.checkpointAnnotation() == null || token == null) {
            return;
        }
        int count = eventCounters.computeIfAbsent(def.streamName(), k -> new AtomicInteger(0))
                .incrementAndGet();
        if (count % def.checkpointAnnotation().saveEveryN() == 0) {
            try {
                checkpointStore.saveProcessed(def.streamName(), token, timestamp);
                FlowWardenMetrics.get().onCheckpoint(def.streamName(), token.toJson());
            } catch (RuntimeException e) {
                FlowWardenMetrics.get().onCheckpointFailed(def.streamName(), e);
                log.warn("Failed to save checkpoint for stream '{}': {}",
                        def.streamName(), e.getMessage(), e);
            }
        }
    }

    boolean isTokenValid(String collection, BsonDocument token, MongoTemplate template) {
        try (MongoCursor<ChangeStreamDocument<Document>> cursor =
                     template.getCollection(collection).watch()
                             .resumeAfter(token)
                             .batchSize(1)
                             .maxAwaitTime(1, TimeUnit.MILLISECONDS)
                             .cursor()) {
            // Must call tryNext() to trigger the actual oplog lookup (getMore);
            // cursor creation alone doesn't validate the resume token.
            cursor.tryNext();
            return true;
        } catch (MongoCommandException e) {
            // Any error from the resume probe means the token is not usable.
            // Common codes: 286 (ChangeStreamHistoryLost), 136 (CappedPositionLost),
            //               280 (ChangeStreamFatalError)
            log.debug("Resume token validation failed for collection '{}' with error code {}: {}",
                    collection, e.getErrorCode(), e.getMessage());
            return false;
        }
    }

    /**
     * Resume cascade: try the primary token chosen by {@link ResumeStrategy}
     * (level 1), fall back to the secondary if the primary has aged out
     * (level 2), apply onHistoryLost strategy if both have aged out (level 3).
     */
    private void applyResumeCascade(String streamName,
                                    ChangeStreamDefinition def,
                                    ChangeStreamRequest.ChangeStreamRequestBuilder<Document> builder,
                                    MongoTemplate streamTemplate) {
        java.util.Optional<io.flowwarden.stream.spi.Checkpoint> cpOpt =
                checkpointStore.findByStreamName(streamName);
        if (cpOpt.isEmpty()) {
            return; // no prior checkpoint → stream starts from now
        }
        io.flowwarden.stream.spi.Checkpoint cp = cpOpt.get();
        BsonDocument processedToken = cp.lastProcessedToken();
        BsonDocument seenToken = cp.lastSeenToken();
        ResumeStrategy strategy = def.checkpointAnnotation().resumeStrategy();

        BsonDocument primary;
        BsonDocument secondary;
        String primaryLabel;
        String secondaryLabel;
        Runnable onFallback;
        switch (strategy) {
            case SEEN_FIRST -> {
                primary = seenToken;
                secondary = processedToken;
                primaryLabel = "lastSeenToken";
                secondaryLabel = "lastProcessedToken";
                onFallback = () -> FlowWardenMetrics.get().onResumeFallbackToProcessed(streamName);
            }
            case PROCESSED_FIRST -> {
                primary = processedToken;
                secondary = seenToken;
                primaryLabel = "lastProcessedToken";
                secondaryLabel = "lastSeenToken";
                onFallback = () -> FlowWardenMetrics.get().onResumeFallbackToSeen(streamName);
            }
            default -> throw new IllegalStateException("Unknown ResumeStrategy: " + strategy);
        }

        // Level 1: try the primary token
        if (primary != null && isTokenValid(def.collection(), primary, streamTemplate)) {
            builder.resumeAfter(primary);
            log.info("Resuming stream '{}' from {}", streamName, primaryLabel);
            return;
        }

        // Level 2: fallback to the secondary if it's distinct and still valid
        if (secondary != null
                && !secondary.equals(primary)
                && isTokenValid(def.collection(), secondary, streamTemplate)) {
            builder.resumeAfter(secondary);
            log.warn("Resuming stream '{}' from {}: {} aged out of oplog", streamName, secondaryLabel, primaryLabel);
            onFallback.run();
            return;
        }

        // Level 3: both tokens unusable → apply onHistoryLost strategy
        if (processedToken != null || seenToken != null) {
            FlowWardenMetrics.get().onResumeHistoryLost(streamName);
            handleHistoryLost(streamName, def, builder, cp.lastProcessedTimestamp());
        }
        // else: checkpoint document exists but both tokens are null → start from now
    }

    private void handleHistoryLost(String streamName,
                                   ChangeStreamDefinition def,
                                   ChangeStreamRequest.ChangeStreamRequestBuilder<Document> builder,
                                   java.time.Instant lastCheckpointTimestamp) {
        OnHistoryLost strategy = def.checkpointAnnotation().onHistoryLost();
        log.warn("Resume token expired for stream '{}' (last checkpoint: {}). Applying strategy: {}",
                streamName, lastCheckpointTimestamp, strategy);

        switch (strategy) {
            case FAIL -> throw new HistoryLostException(streamName, lastCheckpointTimestamp);
            case RESUME_FROM_NOW -> {
                // Don't set resumeAfter — stream starts from current moment.
                // The stale checkpoint will be overwritten once the stream processes its first event.
                log.info("Stream '{}' will start from current moment", streamName);
            }
            case RESUME_FROM_OPLOG_START -> {
                try {
                    Instant oldestTs = getOldestOplogTimestamp();
                    builder.resumeAt(oldestTs);
                    log.info("Stream '{}' will resume from oldest oplog entry at {}", streamName, oldestTs);
                } catch (Exception e) {
                    log.warn("Failed to read oplog for stream '{}': {}. Falling back to RESUME_FROM_NOW.",
                            streamName, e.getMessage());
                    // No resumeAt set → starts from now. Checkpoint preserved for next retry.
                }
            }
        }
    }

    private Instant getOldestOplogTimestamp() {
        Document oldestEntry = defaultTemplate.getMongoDatabaseFactory()
                .getMongoDatabase("local")
                .getCollection("oplog.rs")
                .find()
                .sort(new Document("$natural", 1))
                .limit(1)
                .first();
        if (oldestEntry == null) {
            throw new IllegalStateException("oplog.rs is empty or not accessible");
        }
        org.bson.BsonTimestamp ts = oldestEntry.get("ts", org.bson.BsonTimestamp.class);
        return Instant.ofEpochSecond(ts.getTime());
    }

    private ChangeStreamDefinition findDefinition(String streamName) {
        return registry.findByName(streamName)
                .orElseThrow(() -> new IllegalArgumentException(
                        "No @ChangeStream definition found for stream: " + streamName));
    }

    private StreamConfiguration buildStreamConfiguration(ChangeStreamDefinition def, String mode) {
        StreamConfiguration.CheckpointConfig cpConfig = null;
        if (def.checkpointAnnotation() != null) {
            var cp = def.checkpointAnnotation();
            cpConfig = new StreamConfiguration.CheckpointConfig(
                    cp.saveEveryN(), cp.saveIntervalSeconds(),
                    cp.startPosition().name(), cp.onHistoryLost().name());
        }

        StreamConfiguration.RetryConfig retryConfig = null;
        if (def.retryPolicyAnnotation() != null) {
            var rp = def.retryPolicyAnnotation();
            retryConfig = new StreamConfiguration.RetryConfig(
                    rp.maxAttempts(), rp.initialDelay(), rp.maxDelay(),
                    rp.multiplier(), rp.jitter());
        }

        StreamConfiguration.DlqConfig dlqConfig = null;
        if (def.deadLetterQueueAnnotation() != null) {
            var dlq = def.deadLetterQueueAnnotation();
            dlqConfig = new StreamConfiguration.DlqConfig(
                    dlq.enabled(), dlq.retentionDays(),
                    dlq.includeOriginalDocument(), dlq.includeStackTrace());
        }

        // External modules (e.g., flowwarden-javers) can override handler names via metadata
        Object metadataHandlers = def.metadata() != null ? def.metadata().get("handlers") : null;
        java.util.List<String> handlers;
        if (metadataHandlers instanceof java.util.List<?> mh) {
            handlers = mh.stream().map(Object::toString).toList();
        } else {
            handlers = new java.util.ArrayList<>(def.typedHandlers().keySet().stream()
                    .map(Enum::name).sorted().toList());
            if (def.onChangeHandler() != null) {
                handlers.add("ON_CHANGE");
            }
        }

        // Resolve the actual database name: annotation value takes precedence,
        // otherwise resolve from the MongoTemplate bean
        String database = def.database();
        String templateRef = def.config().mongoTemplateRef();
        if ((database == null || database.isEmpty()) && templateRef != null && !templateRef.isEmpty()) {
            database = templateFor(def).getDb().getName();
        }

        return new StreamConfiguration(
                def.streamName(), def.collection(), database, mode, def.zone(),
                templateRef,
                cpConfig, retryConfig, dlqConfig, handlers,
                def.config().deploymentMode().name());
    }

    private static final long OPLOG_REFRESH_INTERVAL_SECONDS = 60;

    /**
     * Schedules periodic refresh of oplog stats (every 60s).
     */
    private void scheduleOplogStatsRefresh() {
        intervalScheduler.scheduleAtFixedRate(() -> {
            try {
                collectOplogStats();
            } catch (Exception e) {
                log.debug("Periodic oplog stats refresh failed: {}", e.getMessage());
            }
        }, OPLOG_REFRESH_INTERVAL_SECONDS, OPLOG_REFRESH_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Reads the first and last timestamps from {@code local.oplog.rs} to compute
     * the oplog window size, then pushes the result to the metrics provider.
     */
    private void collectOplogStats() {
        try {
            var localDb = defaultTemplate.getMongoDatabaseFactory().getMongoDatabase("local");
            var oplogCollection = localDb.getCollection("oplog.rs");

            Document oldest = oplogCollection.find()
                    .sort(new Document("$natural", 1))
                    .limit(1)
                    .first();
            Document newest = oplogCollection.find()
                    .sort(new Document("$natural", -1))
                    .limit(1)
                    .first();

            if (oldest != null && newest != null) {
                BsonTimestamp oldestTs = oldest.get("ts", BsonTimestamp.class);
                BsonTimestamp newestTs = newest.get("ts", BsonTimestamp.class);
                double logLengthHours = (newestTs.getTime() - oldestTs.getTime()) / 3600.0;
                FlowWardenMetrics.get().onOplogStats(Math.max(0, logLengthHours), "OK");
                log.debug("Oplog window: {}h", String.format("%.1f", logLengthHours));
            } else {
                FlowWardenMetrics.get().onOplogStats(0.0, "UNAVAILABLE");
            }
        } catch (Exception e) {
            log.warn("Oplog stats unavailable: {} ({})", e.getMessage(), e.getClass().getSimpleName());
            FlowWardenMetrics.get().onOplogStats(0.0, "UNAVAILABLE");
        }
    }
}
