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

import com.mongodb.MongoCommandException;
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
import io.flowwarden.stream.ResumeStrategy;
import io.flowwarden.stream.StartPosition;
import io.flowwarden.stream.core.FlowWardenStreamManager;
import io.flowwarden.stream.annotation.DeadLetterQueue;
import io.flowwarden.stream.annotation.MongoDlqOptions;
import io.flowwarden.stream.internal.DefaultChangeStreamContext;
import io.flowwarden.stream.internal.MongoTemplateRegistry;
import io.flowwarden.stream.internal.discovery.ChangeStreamDefinition;
import io.flowwarden.stream.internal.dlq.ReactiveMongoDlqStore;
import io.flowwarden.stream.internal.retry.RetryPolicyConfig;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import io.flowwarden.stream.internal.discovery.StreamRegistry;
import io.flowwarden.stream.internal.discovery.HandlerMethod;
import io.flowwarden.stream.internal.discovery.PipelineMethod;
import io.flowwarden.stream.internal.lock.LeaderElectionCoordinator;
import io.flowwarden.stream.spi.ChangeEventMetadata;
import io.flowwarden.stream.internal.checkpoint.TokenSnapshot;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.DlqPolicy;
import io.flowwarden.stream.spi.DlqStore;
import io.flowwarden.stream.spi.FailedEvent;
import io.flowwarden.stream.spi.StopReason;
import io.flowwarden.stream.spi.StreamConfiguration;
import jakarta.annotation.PreDestroy;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.mongodb.core.ChangeStreamOptions;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import reactor.core.Disposable;

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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages Change Stream subscriptions in REACTIVE mode using
 * {@link ReactiveMongoTemplate} and Project Reactor {@code Flux}.
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public class ReactiveStreamManager implements FlowWardenStreamManager {

    private static final Logger log = LoggerFactory.getLogger(ReactiveStreamManager.class);

    private final MongoTemplateRegistry templateRegistry;
    private final ReactiveMongoTemplate defaultReactiveTemplate;
    private final StreamRegistry registry;
    private final CheckpointStore checkpointStore;
    private final DlqStore dlqStore;
    private final LeaderElectionCoordinator leaderElection; // nullable
    private final Map<String, ReactiveStreamState> streams = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> eventCounters = new ConcurrentHashMap<>();
    private final Map<String, AtomicReference<TokenSnapshot>> latestTokens = new ConcurrentHashMap<>();
    private final Map<String, Instant> lastActivityTimes = new ConcurrentHashMap<>();
    private final Map<String, ScheduledFuture<?>> intervalTasks = new ConcurrentHashMap<>();
    private final ScheduledExecutorService intervalScheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "fw-checkpoint-interval-reactive");
                t.setDaemon(true);
                return t;
            });

    private record ReactiveStreamState(
            Disposable disposable,
            ChangeStreamDefinition definition,
            AtomicBoolean gracefulStop,
            AtomicReference<Throwable> lastError) {
    }

    public ReactiveStreamManager(MongoTemplateRegistry templateRegistry,
                                  StreamRegistry registry,
                                  CheckpointStore checkpointStore,
                                  DlqStore dlqStore,
                                  LeaderElectionCoordinator leaderElection) {
        this.templateRegistry = templateRegistry;
        this.defaultReactiveTemplate = templateRegistry.getDefaultReactiveTemplate();
        this.registry = registry;
        this.checkpointStore = checkpointStore;
        this.dlqStore = dlqStore;
        this.leaderElection = leaderElection;
    }

    private ReactiveMongoTemplate templateFor(ChangeStreamDefinition def) {
        return templateRegistry.resolveReactive(def.config().mongoTemplateRef());
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

        ChangeStreamOptions.ChangeStreamOptionsBuilder optionsBuilder = ChangeStreamOptions.builder();

        if (def.checkpointAnnotation() != null
                && def.checkpointAnnotation().startPosition() == StartPosition.RESUME) {
            applyResumeCascade(streamName, def, optionsBuilder);
        }

        if (def.config().fullDocument() != FullDocumentMode.DEFAULT) {
            optionsBuilder.fullDocumentLookup(FullDocument.valueOf(def.config().fullDocument().name()));
        }
        if (def.config().fullDocumentBeforeChange() != FullDocumentBeforeChangeMode.OFF) {
            optionsBuilder.fullDocumentBeforeChangeLookup(
                    FullDocumentBeforeChange.valueOf(def.config().fullDocumentBeforeChange().name()));
        }

        if (def.pipelineMethod() != null) {
            List<Document> pipeline = def.pipelineMethod().resolve(def.bean());
            List<AggregationOperation> ops = pipeline.stream()
                    .<AggregationOperation>map(doc -> ctx -> doc)
                    .toList();
            optionsBuilder.filter(Aggregation.newAggregation(ops));
            log.debug("Applied @Pipeline with {} stages to stream '{}'", pipeline.size(), streamName);
        }

        ChangeStreamOptions options = optionsBuilder.build();

        ReactiveMongoTemplate streamTemplate = templateFor(def);
        AtomicBoolean gracefulStop = new AtomicBoolean(false);
        AtomicReference<Throwable> lastError = new AtomicReference<>();
        Disposable disposable = streamTemplate
                .changeStream(def.collection(), options, Document.class)
                .concatMap(event -> {
                    ChangeStreamDocument<Document> raw = event.getRaw();
                    if (raw != null) {
                        return handleEventReactive(raw, def);
                    }
                    return Mono.empty();
                })
                .doOnError(e -> {
                    lastError.set(e);
                    log.error("Error in reactive Change Stream '{}': {}",
                            streamName, e.getMessage(), e);
                })
                .onErrorResume(e -> {
                    FlowWardenMetrics.get().onEventError(streamName, e, true, 0, null);
                    return Mono.empty();
                })
                .doFinally(signal -> {
                    if (gracefulStop.get()) {
                        FlowWardenMetrics.get().onStreamStopped(
                                streamName, StopReason.GRACEFUL, null);
                    } else {
                        FlowWardenMetrics.get().onStreamStopped(
                                streamName, StopReason.CRASHED, lastError.get());
                    }
                    // Evict per-stream state so isRunning / getLastEventTime
                    // stop lying once the pipeline has terminated.
                    streams.remove(streamName);
                    lastActivityTimes.remove(streamName);
                    eventCounters.remove(streamName);
                })
                .subscribe();

        streams.put(streamName, new ReactiveStreamState(disposable, def, gracefulStop, lastError));
        scheduleIntervalCheckpoint(def);
        log.info("Started reactive Change Stream '{}' on collection '{}'",
                streamName, def.collection());

        FlowWardenMetrics.get().onStreamStarted(streamName, buildStreamConfiguration(def, "REACTIVE"));
    }

    @Override
    public void stopStream(String streamName) {
        ScheduledFuture<?> task = intervalTasks.remove(streamName);
        if (task != null) {
            task.cancel(false);
        }
        latestTokens.remove(streamName);

        ReactiveStreamState state = streams.remove(streamName);
        if (state == null) {
            log.warn("Stream '{}' is not running", streamName);
            return;
        }

        lastActivityTimes.remove(streamName);
        eventCounters.remove(streamName);

        state.gracefulStop().set(true);
        try {
            state.disposable().dispose();
        } catch (Exception e) {
            log.warn("Error stopping stream '{}'", streamName, e);
        }

        // onStreamStopped(GRACEFUL) is emitted by the .doFinally hook on the pipeline
        // once the dispose() above triggers the CANCEL signal. The doFinally also
        // re-runs the state evictions above (idempotent) for the crash path.
        log.info("Stopped reactive Change Stream '{}'", streamName);
    }

    @Override
    public boolean isRunning(String streamName) {
        ReactiveStreamState state = streams.get(streamName);
        return state != null && !state.disposable().isDisposed();
    }

    @Override
    public Instant getLastEventTime(String streamName) {
        return lastActivityTimes.get(streamName);
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down FlowWarden ReactiveStreamManager ({} streams)", streams.size());
        if (leaderElection != null) {
            leaderElection.shutdown();
        }
        for (String streamName : streams.keySet()) {
            stopStream(streamName);
        }
        intervalScheduler.shutdownNow();
    }

    private Mono<Void> handleEventReactive(ChangeStreamDocument<Document> raw,
                                                                     ChangeStreamDefinition def) {
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
            return Mono.empty();
        }

        if (def.filterMethod() != null) {
            if (!def.filterMethod().evaluate(def.bean(), ctx)) {
                log.debug("Event filtered by @Filter in stream '{}'", def.streamName());
                return Mono.empty();
            }
        }

        RetryPolicyConfig retryConfig = def.retryPolicyAnnotation() != null
                ? RetryPolicyConfig.fromAnnotation(def.retryPolicyAnnotation()) : null;

        long startNanos = System.nanoTime();
        AtomicInteger attempt = new AtomicInteger(1);

        Mono<Void> pipeline = Mono.defer(() -> {
            ctx.setAttemptNumber(attempt.get());
            try {
                Document rawDoc = raw.getFullDocument();
                Object result = handler.invoke(def.bean(), ctx, rawDoc,
                        templateFor(def).getConverter(), def.config().documentType());
                if (result instanceof Mono<?> mono) {
                    return mono.then();
                }
                return Mono.empty();
            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                return Mono.<Void>error(cause);
            } catch (IllegalAccessException e) {
                return Mono.<Void>error(e);
            }
        });

        // REQ-015: wrap retryWhen to consult @OnError before standard retry
        if (retryConfig != null || !def.errorHandlerResolver().isEmpty()) {
            RetryPolicyConfig rc = retryConfig;
            pipeline = pipeline.retryWhen(Retry.from(signals -> signals.flatMap(signal -> {
                Throwable ex = signal.failure();

                // Consult @OnError handler
                ErrorAction action = resolveErrorAction(def, ex, ctx);

                switch (action) {
                    case SKIP -> {
                        log.warn("@OnError SKIP for stream '{}': {}", def.streamName(), ex.getMessage());
                        FlowWardenMetrics.get().onEventError(def.streamName(), ex, false, attempt.get(), eventMetadata);
                        return Mono.<Long>error(new SkipEventException(ex));
                    }
                    case DLQ -> {
                        log.warn("@OnError DLQ for stream '{}': {}", def.streamName(), ex.getMessage());
                        FlowWardenMetrics.get().onEventError(def.streamName(), ex, false, attempt.get(), eventMetadata);
                        return Mono.<Long>error(new DlqEventException(ex));
                    }
                    case RETRY -> {
                        int current = attempt.incrementAndGet();
                        if (rc != null && current - 1 < rc.maxAttempts()) {
                            long delayMs = rc.computeDelayMillis(current - 1);
                            FlowWardenMetrics.get().onEventError(def.streamName(), ex, true, current - 1, eventMetadata);
                            return Mono.delay(java.time.Duration.ofMillis(delayMs));
                        }
                        // maxAttempts exhausted or no @RetryPolicy
                        log.warn("@OnError RETRY exhausted for stream '{}': {}", def.streamName(), ex.getMessage());
                        FlowWardenMetrics.get().onEventError(def.streamName(), ex, false, current - 1, eventMetadata);
                        return Mono.<Long>error(new DlqEventException(ex));
                    }
                    default -> {
                        // RETHROW: fall through to standard retry logic
                    }
                }

                // Standard retry logic
                int current = attempt.incrementAndGet();
                if (rc != null && rc.shouldRetry(ex, current - 1)) {
                    long delayMs = rc.computeDelayMillis(current - 1);
                    FlowWardenMetrics.get().onEventError(def.streamName(), ex, true, current - 1, eventMetadata);
                    log.warn("Retry {}/{} for stream '{}' after {}ms: {}",
                            current - 1, rc.maxAttempts(), def.streamName(),
                            delayMs, ex.getMessage());
                    return Mono.delay(java.time.Duration.ofMillis(delayMs));
                }
                return Mono.error(ex);
            })));
        }


        return pipeline.doOnSuccess(v -> {
            long durationNanos = System.nanoTime() - startNanos;
            FlowWardenMetrics.get().onEventProcessed(def.streamName(), durationNanos, true);
            lastActivityTimes.put(def.streamName(), Instant.now());
            if (!ctx.isCheckpointSavedManually()) {
                saveCheckpointIfNeeded(def, raw.getResumeToken(), ctx.getClusterTime());
            }
        }).onErrorResume(e -> {
            long durationNanos = System.nanoTime() - startNanos;

            // REQ-015: handle sentinel exceptions from @OnError decisions
            if (e instanceof SkipEventException) {
                FlowWardenMetrics.get().onEventProcessed(def.streamName(), durationNanos, false);
                return Mono.empty();
            }
            if (e instanceof DlqEventException) {
                FlowWardenMetrics.get().onEventProcessed(def.streamName(), durationNanos, false);
                int actualAttempts = retryConfig != null ? attempt.get() - 1 : attempt.get();
                sendToDlqAfterExhaustion(def, ctx, raw, e.getCause(), actualAttempts);
                return Mono.empty();
            }

            log.error("Error in handler {} for stream '{}': {}",
                    handler, def.streamName(), e.getMessage(), e);
            int actualAttempts = retryConfig != null ? attempt.get() - 1 : attempt.get();
            FlowWardenMetrics.get().onEventError(def.streamName(), e, false, actualAttempts, eventMetadata);
            FlowWardenMetrics.get().onEventProcessed(def.streamName(), durationNanos, false);
            sendToDlqAfterExhaustion(def, ctx, raw, e, actualAttempts);
            return Mono.empty();
        });
    }

    // --- REQ-015: sentinel exceptions for @OnError decisions in reactive pipeline ---

    private static final class SkipEventException extends RuntimeException {
        SkipEventException(Throwable cause) { super(cause); }
    }

    private static final class DlqEventException extends RuntimeException {
        DlqEventException(Throwable cause) { super(cause); }
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

            // DlqStore.save() uses blocking I/O, so schedule it off the reactive thread
            Mono.fromRunnable(() -> dlqStore.save(failedEvent, policy))
                    .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
                    .doOnSuccess(v -> {
                        FlowWardenMetrics.get().onEventSentToDlq(def.streamName());
                        log.info("Event sent to DLQ for stream '{}'", def.streamName());
                    })
                    .doOnError(e -> {
                        FlowWardenMetrics.get().onEventDlqFailed(def.streamName(), e);
                        log.error("Failed to send event to DLQ for stream '{}': {}",
                                def.streamName(), e.getMessage(), e);
                    })
                    .subscribe();
        } catch (Exception e) {
            log.error("Failed to send event to DLQ for stream '{}': {}", def.streamName(), e.getMessage(), e);
        }
    }

    private void registerDlqCollections() {
        if (!(dlqStore instanceof ReactiveMongoDlqStore reactiveStore)) {
            return;
        }
        for (ChangeStreamDefinition def : registry.getDefinitions()) {
            if (def.deadLetterQueueAnnotation() == null) {
                continue;
            }
            MongoDlqOptions opts = def.mongoDlqOptionsAnnotation();
            String collection = opts != null ? opts.collection() : "";
            reactiveStore.registerStream(def.streamName(), collection);
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

    boolean isTokenValid(String collection, BsonDocument token, ReactiveMongoTemplate template) {
        try {
            template
                    .changeStream(collection,
                            ChangeStreamOptions.builder().resumeAfter(token).build(),
                            Document.class)
                    .next()
                    .timeout(java.time.Duration.ofMillis(500))
                    .onErrorResume(java.util.concurrent.TimeoutException.class, e -> Mono.empty())
                    .block(java.time.Duration.ofSeconds(2));
            return true;
        } catch (Exception e) {
            // Any error from the resume probe means the token is not usable.
            // Common codes: 286 (ChangeStreamHistoryLost), 136 (CappedPositionLost),
            //               280 (ChangeStreamFatalError)
            Throwable cause = e;
            while (cause != null) {
                if (cause instanceof MongoCommandException mce) {
                    log.debug("Resume token validation failed for collection '{}' with error code {}: {}",
                            collection, mce.getErrorCode(), mce.getMessage());
                    return false;
                }
                cause = cause.getCause();
            }
            // Not a MongoCommandException — re-throw (transient network error, timeout, etc.)
            throw e instanceof RuntimeException re ? re : new RuntimeException(e);
        }
    }

    /**
     * Resume cascade: try the primary token chosen by {@link ResumeStrategy}
     * (level 1), fall back to the secondary if the primary has aged out
     * (level 2), apply onHistoryLost strategy if both have aged out (level 3).
     */
    private void applyResumeCascade(String streamName,
                                    ChangeStreamDefinition def,
                                    ChangeStreamOptions.ChangeStreamOptionsBuilder optionsBuilder) {
        java.util.Optional<io.flowwarden.stream.spi.Checkpoint> cpOpt =
                checkpointStore.findByStreamName(streamName);
        if (cpOpt.isEmpty()) {
            return; // no prior checkpoint → stream starts from now
        }
        io.flowwarden.stream.spi.Checkpoint cp = cpOpt.get();
        BsonDocument processedToken = cp.lastProcessedToken();
        BsonDocument seenToken = cp.lastSeenToken();
        ReactiveMongoTemplate streamTemplate = templateFor(def);
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
            optionsBuilder.resumeAfter(primary);
            log.info("Resuming stream '{}' from {}", streamName, primaryLabel);
            return;
        }

        // Level 2: fallback to the secondary if it's distinct and still valid
        if (secondary != null
                && !secondary.equals(primary)
                && isTokenValid(def.collection(), secondary, streamTemplate)) {
            optionsBuilder.resumeAfter(secondary);
            log.warn("Resuming stream '{}' from {}: {} aged out of oplog", streamName, secondaryLabel, primaryLabel);
            onFallback.run();
            return;
        }

        // Level 3: both tokens unusable → apply onHistoryLost strategy
        if (processedToken != null || seenToken != null) {
            FlowWardenMetrics.get().onResumeHistoryLost(streamName);
            handleHistoryLost(streamName, def, optionsBuilder, cp.lastProcessedTimestamp());
        }
        // else: checkpoint document exists but both tokens are null → start from now
    }

    private void handleHistoryLost(String streamName,
                                   ChangeStreamDefinition def,
                                   ChangeStreamOptions.ChangeStreamOptionsBuilder optionsBuilder,
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
                    BsonTimestamp oldestTs = getOldestOplogTimestamp();
                    optionsBuilder.resumeAt(oldestTs);
                    log.info("Stream '{}' will resume from oldest oplog entry at {}", streamName, oldestTs);
                } catch (Exception e) {
                    log.warn("Failed to read oplog for stream '{}': {}. Falling back to RESUME_FROM_NOW.",
                            streamName, e.getMessage());
                    // No resumeAt set → starts from now. Checkpoint preserved for next retry.
                }
            }
        }
    }

    private BsonTimestamp getOldestOplogTimestamp() {
        Document oldestEntry = defaultReactiveTemplate.getMongoDatabaseFactory()
                .getMongoDatabase("local")
                .flatMapMany(db -> db.getCollection("oplog.rs")
                        .find()
                        .sort(new Document("$natural", 1))
                        .limit(1))
                .next()
                .block(java.time.Duration.ofSeconds(5));
        if (oldestEntry == null) {
            throw new IllegalStateException("oplog.rs is empty or not accessible");
        }
        return oldestEntry.get("ts", BsonTimestamp.class);
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
        // otherwise resolve from the ReactiveMongoTemplate bean
        String database = def.database();
        String templateRef = def.config().mongoTemplateRef();
        if ((database == null || database.isEmpty()) && templateRef != null && !templateRef.isEmpty()) {
            database = templateFor(def).getMongoDatabaseFactory()
                    .getMongoDatabase().map(db -> db.getName()).block();
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
            defaultReactiveTemplate.getMongoDatabaseFactory()
                    .getMongoDatabase("local")
                    .flatMap(db -> {
                        var collection = db.getCollection("oplog.rs");
                        var oldestMono = Mono.from(collection.find()
                                .sort(new Document("$natural", 1))
                                .limit(1).first());
                        var newestMono = Mono.from(collection.find()
                                .sort(new Document("$natural", -1))
                                .limit(1).first());
                        return Mono.zip(oldestMono, newestMono);
                    })
                    .subscribe(
                            tuple -> {
                                BsonTimestamp oldestTs = tuple.getT1().get("ts", BsonTimestamp.class);
                                BsonTimestamp newestTs = tuple.getT2().get("ts", BsonTimestamp.class);
                                double logLengthHours = (newestTs.getTime() - oldestTs.getTime()) / 3600.0;
                                FlowWardenMetrics.get().onOplogStats(Math.max(0, logLengthHours), "OK");
                                log.debug("Oplog window: {}h", String.format("%.1f", logLengthHours));
                            },
                            e -> {
                                log.debug("Oplog stats unavailable: {}", e.getMessage());
                                FlowWardenMetrics.get().onOplogStats(0.0, "UNAVAILABLE");
                            }
                    );
        } catch (Exception e) {
            log.debug("Oplog stats unavailable: {}", e.getMessage());
            FlowWardenMetrics.get().onOplogStats(0.0, "UNAVAILABLE");
        }
    }
}
