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
import io.flowwarden.stream.internal.checkpoint.CheckpointHeartbeat;
import io.flowwarden.stream.internal.checkpoint.ResumeContext;
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
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
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
    private final Map<String, ScheduledFuture<?>> idleProbeTasks = new ConcurrentHashMap<>();
    private final Map<String, CheckpointHeartbeat> heartbeats = new ConcurrentHashMap<>();
    private final ScheduledExecutorService intervalScheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "fw-checkpoint-interval");
                t.setDaemon(true);
                return t;
            });
    /**
     * Dedicated single thread for heartbeat probes: bounds the concurrent
     * probe count to one per instance, prevents two probes of the same
     * stream from overlapping, provides natural backpressure (excess probes
     * queue among themselves) — and keeps blocking probe I/O off the flush
     * scheduler, so one slow probe never delays other streams' flush cadence.
     */
    private final ScheduledExecutorService probeScheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "fw-heartbeat-probe");
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
                        message -> handleMessage(message, def),
                        def.streamName(),
                        () -> clearStreamState(def.streamName()));
        ChangeStreamRequest.ChangeStreamRequestBuilder<Document> builder = ChangeStreamRequest.builder()
                .collection(def.collection())
                .publishTo(listener);

        // Resolve the @Pipeline stages once: the main stream and the heartbeat
        // probe MUST observe the exact same pipeline documents.
        List<Document> resolvedPipeline = def.pipelineMethod() != null
                ? def.pipelineMethod().resolve(def.bean())
                : List.of();
        ImperativeHeartbeatProbe probe =
                new ImperativeHeartbeatProbe(streamTemplate, def, resolvedPipeline);

        ResumeContext resumeContext = ResumeContext.NONE;
        if (def.checkpointAnnotation() != null
                && def.checkpointAnnotation().startPosition() == StartPosition.RESUME) {
            resumeContext = applyResumeCascade(streamName, def, builder, streamTemplate, probe);
        }
        BsonDocument seedToken = resumeContext.seedToken();

        if (def.config().fullDocument() != FullDocumentMode.DEFAULT) {
            builder.fullDocumentLookup(FullDocument.valueOf(def.config().fullDocument().name()));
        }
        if (def.config().fullDocumentBeforeChange() != FullDocumentBeforeChangeMode.OFF) {
            builder.fullDocumentBeforeChangeLookup(
                    FullDocumentBeforeChange.valueOf(def.config().fullDocumentBeforeChange().name()));
        }

        if (!resolvedPipeline.isEmpty()) {
            builder.filter(resolvedPipeline.toArray(new Document[0]));
            log.debug("Applied @Pipeline with {} stages to stream '{}'",
                    resolvedPipeline.size(), streamName);
        }

        ChangeStreamRequest<Document> request = builder.build();

        container.start();
        Subscription subscription = container.register(request, Document.class);

        // Seed the in-memory snapshot with the resume position so the first
        // heartbeat probe chains from it instead of waiting for an event.
        // SEED, not EVENT: a resume position (possibly the older processed
        // token under PROCESSED_FIRST) must never be persisted as a newly
        // delivered seen token.
        if (seedToken != null) {
            latestTokens.computeIfAbsent(streamName, k -> new AtomicReference<>())
                    .set(new TokenSnapshot(seedToken, Instant.now(), TokenSnapshot.Source.SEED));
        }

        streams.put(streamName, new StreamState(container, subscription, def));
        scheduleIntervalCheckpoint(def, probe, resumeContext);
        log.info("Started Change Stream '{}' on collection '{}'", streamName, def.collection());

        FlowWardenMetrics.get().onStreamStarted(streamName, buildStreamConfiguration(def, "IMPERATIVE"));
    }

    @Override
    public void stopStream(String streamName) {
        // Invalidate the heartbeat BEFORE cancelling its task: cancel(false)
        // lets an in-flight tick finish, and it must not write for a stream
        // being stopped.
        CheckpointHeartbeat heartbeat = heartbeats.remove(streamName);
        if (heartbeat != null) {
            heartbeat.cancel();
        }
        ScheduledFuture<?> task = intervalTasks.remove(streamName);
        if (task != null) {
            task.cancel(false);
        }
        ScheduledFuture<?> idleTask = idleProbeTasks.remove(streamName);
        if (idleTask != null) {
            idleTask.cancel(false);
        }
        latestTokens.remove(streamName);

        StreamState state = streams.remove(streamName);
        if (state == null) {
            log.warn("Stream '{}' is not running", streamName);
            return;
        }

        lastActivityTimes.remove(streamName);
        eventCounters.remove(streamName);

        try {
            state.subscription().cancel();
            state.container().stop();
        } catch (Exception e) {
            log.warn("Error stopping stream '{}'", streamName, e);
        }

        FlowWardenMetrics.get().onStreamStopped(streamName, StopReason.GRACEFUL, null);
        log.info("Stopped Change Stream '{}'", streamName);
    }

    /**
     * Cleanup invoked by {@link FlowWardenMessageListenerWrapper} when the
     * container thread dies on an uncaught throwable. Evicts the per-stream
     * state so {@link #isRunning} and {@link #getLastEventTime} stop lying
     * once the worker is dead.
     */
    private void clearStreamState(String streamName) {
        streams.remove(streamName);
        lastActivityTimes.remove(streamName);
        eventCounters.remove(streamName);
        latestTokens.remove(streamName);
        CheckpointHeartbeat heartbeat = heartbeats.remove(streamName);
        if (heartbeat != null) {
            heartbeat.cancel();
        }
        ScheduledFuture<?> task = intervalTasks.remove(streamName);
        if (task != null) {
            task.cancel(false);
        }
        ScheduledFuture<?> idleTask = idleProbeTasks.remove(streamName);
        if (idleTask != null) {
            idleTask.cancel(false);
        }
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
        probeScheduler.shutdownNow();
    }

    @SuppressWarnings("unchecked")
    private void handleMessage(Message<ChangeStreamDocument<Document>, Document> message,
                               ChangeStreamDefinition def) {
        ChangeStreamDocument<Document> raw = message.getRaw();
        if (raw == null) {
            return;
        }

        // The resume token is published to the heartbeat only once the event
        // reaches a terminal-safe outcome (handler success after
        // saveProcessed, filter rejection, no handler, terminal SKIP/DLQ) —
        // see publishSettledToken. Publishing at receipt would let a recovery
        // certify past an event whose handler is still in flight, and the
        // history-lost reset removes the processed anchor that would
        // otherwise guarantee its redelivery.

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
                                Instant now = Instant.now();
                                checkpointStore.save(new io.flowwarden.stream.spi.Checkpoint(
                                        def.streamName(), null, token, now,
                                        token, now, now, Collections.emptyMap()));
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
            publishSettledToken(def, raw);
            return;
        }

        if (def.filterMethod() != null) {
            if (!def.filterMethod().evaluate(def.bean(), ctx)) {
                log.debug("Event filtered by @Filter in stream '{}'", def.streamName());
                publishSettledToken(def, raw);
                return;
            }
        }

        RetryPolicyConfig retryConfig = def.retryPolicyAnnotation() != null
                ? RetryPolicyConfig.fromAnnotation(def.retryPolicyAnnotation()) : null;

        long startNanos = System.nanoTime();
        boolean success = false;
        // Terminal-outcome marker: only handler success, SKIP or a DLQ
        // decision settle the event. An interrupted retry backoff exits the
        // loop with the outcome still undecided — publishing its token would
        // let a probe certify past a delivery that must be replayed.
        boolean settled = false;
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
                settled = true;
                break;
            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause() != null ? e.getCause() : e;

                // REQ-015: consult @OnError handlers before standard retry/DLQ
                ErrorAction action = resolveErrorAction(def, cause, ctx);

                switch (action) {
                    case SKIP -> {
                        log.warn("@OnError SKIP for stream '{}': {}", def.streamName(), cause.getMessage());
                        FlowWardenMetrics.get().onEventError(def.streamName(), cause, false, attempt, eventMetadata);
                        settled = true;
                        break outerLoop;
                    }
                    case DLQ -> {
                        log.warn("@OnError DLQ for stream '{}': {}", def.streamName(), cause.getMessage());
                        FlowWardenMetrics.get().onEventError(def.streamName(), cause, false, attempt, eventMetadata);
                        sendToDlqAfterExhaustion(def, ctx, raw, cause, attempt);
                        settled = true;
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
                        settled = true;
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
                    settled = true;
                    break;
                }
            } catch (IllegalAccessException e) {
                log.error("Cannot invoke handler for stream '{}'", def.streamName(), e);
                FlowWardenMetrics.get().onEventError(def.streamName(), e, false, attempt, eventMetadata);
                sendToDlqAfterExhaustion(def, ctx, raw, e, attempt);
                settled = true;
                break;
            }
        }

        long durationNanos = System.nanoTime() - startNanos;
        FlowWardenMetrics.get().onEventProcessed(def.streamName(), durationNanos, success);
        lastActivityTimes.put(def.streamName(), Instant.now());
        // Publish only on terminal outcomes (success after saveProcessed,
        // SKIP, DLQ decision, exhaustion). An interrupted retry backoff
        // leaves the outcome undecided — the event must be replayable, its
        // token must never become a certification chain source.
        if (settled) {
            publishSettledToken(def, raw);
        }
    }

    /**
     * Publishes a resume token to the heartbeat's shared snapshot. Only
     * called on terminal-safe outcomes — a token observed at receipt is
     * never certifiable while its handler outcome is undecided.
     */
    private void publishSettledToken(ChangeStreamDefinition def,
                                     ChangeStreamDocument<Document> raw) {
        if (def.checkpointAnnotation() != null && raw.getResumeToken() != null) {
            latestTokens.computeIfAbsent(def.streamName(), k -> new AtomicReference<>())
                    .set(new TokenSnapshot(raw.getResumeToken(), Instant.now()));
        }
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

    private void scheduleIntervalCheckpoint(ChangeStreamDefinition def,
                                            ImperativeHeartbeatProbe probe,
                                            ResumeContext resumeContext) {
        if (def.checkpointAnnotation() == null) {
            return;
        }
        int flushSeconds = def.checkpointAnnotation().saveIntervalSeconds();
        int idleSeconds = def.checkpointAnnotation().idleHeartbeatIntervalSeconds();
        boolean needsEstablishmentChain =
                resumeContext.startInCatchUp() || resumeContext.initialOperationTime() != null;
        if (flushSeconds <= 0 && idleSeconds <= 0 && !needsEstablishmentChain) {
            // No periodic policy AND no pending recovery/catch-up transition:
            // nothing to schedule. The establishment chain, when needed, runs
            // even with both periodic policies opted out.
            return;
        }
        String streamName = def.streamName();
        // Two independent cadences on two separate threads:
        // - flush (saveIntervalSeconds, flush scheduler): dirty-only write
        //   coalescing, no cursor, never blocking;
        // - idle heartbeat (idleHeartbeatIntervalSeconds, probe scheduler):
        //   probe-based oplog rollover protection when the main cursor stalls.
        // Both advance ONLY lastSeenToken (+ lastHeartbeatTimestamp);
        // lastProcessedToken is managed by saveCheckpointIfNeeded after
        // confirmed handler success. Everything derives from the cascade's
        // ResumeContext — no store re-read, so scheduling cannot fail after
        // the cursor is registered.
        CheckpointHeartbeat heartbeat = new CheckpointHeartbeat(
                streamName, checkpointStore, probe,
                () -> latestTokens.computeIfAbsent(streamName, k -> new AtomicReference<>()),
                resumeContext.allowPersistedFallback(),
                Duration.ofSeconds(idleSeconds),
                resumeContext.startInCatchUp(),
                resumeContext.initialOperationTime(),
                resumeContext.deadProcessedToken());
        heartbeats.put(streamName, heartbeat);
        if (flushSeconds > 0) {
            intervalTasks.put(streamName, intervalScheduler.scheduleAtFixedRate(
                    heartbeat::flushTick, flushSeconds, flushSeconds, TimeUnit.SECONDS));
        }
        if (idleSeconds > 0) {
            // Short check cadence keeps the configured threshold an actual
            // bound (the probe fires at most a few seconds after idleness
            // elapses); the heartbeat itself spaces probes a full interval
            // apart. Jittered initial delay (within one check period, never
            // beyond the bound) desynchronizes streams started together.
            long checkSeconds = Math.min(idleSeconds, IDLE_CHECK_PERIOD_SECONDS);
            long initialDelay = 1 + ThreadLocalRandom.current().nextLong(0, checkSeconds);
            idleProbeTasks.put(streamName, probeScheduler.scheduleAtFixedRate(
                    heartbeat::idleTick, initialDelay, checkSeconds, TimeUnit.SECONDS));
        }
        if (needsEstablishmentChain) {
            // Transient establishment chain: immediate certification attempt,
            // then bounded retries until the server certifies the backlog
            // consumed (catch-up) or a durable position exists (OPLOG_START
            // recovery). Independent of both periodic policies — opting out
            // of them must never disable a correction the flush depends on.
            scheduleCatchUpAttempt(heartbeat, 0);
        } else if (idleSeconds > 0) {
            // Async diagnostic probe: an incompatible probe pipeline surfaces
            // shortly after start (WARN + onHeartbeatProbeFailed) instead of
            // an idle-interval later.
            probeScheduler.execute(heartbeat::probeNow);
        }
    }

    private static final long IDLE_CHECK_PERIOD_SECONDS = 5;
    private static final long CATCH_UP_RETRY_SECONDS = 5;

    private void scheduleCatchUpAttempt(CheckpointHeartbeat heartbeat, long delaySeconds) {
        try {
            probeScheduler.schedule(() -> {
                heartbeat.probeNow();
                if (heartbeat.isActive() && heartbeat.needsEstablishment()) {
                    scheduleCatchUpAttempt(heartbeat, CATCH_UP_RETRY_SECONDS);
                }
            }, delaySeconds, TimeUnit.SECONDS);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // Scheduler shut down — the stream is going away with it.
        }
    }

    private static Instant mostRecent(Instant... candidates) {
        Instant best = null;
        for (Instant candidate : candidates) {
            if (candidate != null && (best == null || candidate.isAfter(best))) {
                best = candidate;
            }
        }
        return best;
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
     *
     * @return the immutable resume outcome (seed + persisted seen position),
     *         built from this single checkpoint read — heartbeat setup
     *         derives everything from it without re-reading the store
     */
    private ResumeContext applyResumeCascade(String streamName,
                                    ChangeStreamDefinition def,
                                    ChangeStreamRequest.ChangeStreamRequestBuilder<Document> builder,
                                    MongoTemplate streamTemplate,
                                    ImperativeHeartbeatProbe probe) {
        java.util.Optional<io.flowwarden.stream.spi.Checkpoint> cpOpt =
                checkpointStore.findByStreamName(streamName);
        if (cpOpt.isEmpty()) {
            // No prior checkpoint → bootstrap: capture an initial PBRT and
            // start the main stream from it, so no window is ever unprotected
            // and the heartbeat always has a position to chain from.
            return bootstrapInitialPosition(streamName, builder, probe, null);
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
            return new ResumeContext(primary, seenToken);
        }

        // Level 2: the secondary, validated exactly once. With a null primary
        // (never recorded — typical after a history-lost self-repair) this is
        // not a degradation: INFO, no "aged out" warning, no fallback metric.
        if (secondary != null
                && (primary == null || !secondary.equals(primary))
                && isTokenValid(def.collection(), secondary, streamTemplate)) {
            builder.resumeAfter(secondary);
            if (primary == null) {
                log.info("Resuming stream '{}' from {} ({} not recorded)",
                        streamName, secondaryLabel, primaryLabel);
            } else {
                log.warn("Resuming stream '{}' from {}: {} aged out of oplog",
                        streamName, secondaryLabel, primaryLabel);
                onFallback.run();
            }
            return new ResumeContext(secondary, seenToken);
        }

        // Level 3: both tokens unusable → apply onHistoryLost strategy
        if (processedToken != null || seenToken != null) {
            FlowWardenMetrics.get().onResumeHistoryLost(streamName);
            return handleHistoryLost(streamName, def, builder,
                    mostRecent(cp.lastProcessedTimestamp(), cp.lastSeenTimestamp(),
                            cp.lastHeartbeatTimestamp()),
                    processedToken, probe);
        }
        // Checkpoint document exists but both tokens are null → bootstrap,
        // same as a stream with no prior checkpoint.
        return bootstrapInitialPosition(streamName, builder, probe, null);
    }

    /**
     * Bootstrap for a stream with no usable prior position: capture the
     * server's current position from the change stream's <em>initial</em>
     * reply (before any event can be returned — no cursor hand-off window),
     * persist it, and resume the main stream after it. Both the capture and
     * the persistence are startup preconditions: a failure propagates instead
     * of silently starting an unprotected, non-durable stream.
     */
    private ResumeContext bootstrapInitialPosition(String streamName,
                                                  ChangeStreamRequest.ChangeStreamRequestBuilder<Document> builder,
                                                  ImperativeHeartbeatProbe probe,
                                                  BsonDocument deadProcessedToken) {
        BsonDocument pbrt = probe.initialPosition();
        Instant now = Instant.now();
        try {
            if (deadProcessedToken != null) {
                // History-lost recovery: the history is explicitly abandoned,
                // so the dead processed pair is removed along with installing
                // the fresh position — atomically, guarded on the value read
                // at detection, preserving instanceId, metadata and any
                // fields unknown to the SPI.
                checkpointStore.resetAfterHistoryLost(streamName, pbrt, deadProcessedToken, now);
            } else {
                checkpointStore.saveSeen(streamName, pbrt, now, now);
            }
        } catch (RuntimeException e) {
            // A non-durable position cannot guarantee at-least-once: a crash
            // before the next checkpoint would silently restart from a newer
            // position. Fail the startup.
            FlowWardenMetrics.get().onCheckpointFailed(streamName, e);
            throw e;
        }
        FlowWardenMetrics.get().onCheckpoint(streamName, pbrt.toJson());
        builder.resumeAfter(pbrt);
        log.info("Bootstrapped stream '{}' from an initial server-certified position", streamName);
        // seed == freshly persisted seen: never a phantom catch-up.
        return new ResumeContext(pbrt, pbrt);
    }

    private ResumeContext handleHistoryLost(String streamName,
                                   ChangeStreamDefinition def,
                                   ChangeStreamRequest.ChangeStreamRequestBuilder<Document> builder,
                                   java.time.Instant lastCheckpointTimestamp,
                                   BsonDocument deadProcessedToken,
                                   ImperativeHeartbeatProbe probe) {
        OnHistoryLost strategy = def.checkpointAnnotation().onHistoryLost();
        log.warn("Resume token expired for stream '{}' (last checkpoint: {}). Applying strategy: {}",
                streamName, lastCheckpointTimestamp, strategy);

        switch (strategy) {
            case FAIL -> throw new HistoryLostException(streamName, lastCheckpointTimestamp);
            case RESUME_FROM_NOW -> {
                // This strategy explicitly accepts abandoning history, so a
                // fresh server-certified position is strictly better than an
                // implicit non-durable "from now": the checkpoint self-repairs
                // immediately and the heartbeat never consults the expired
                // token again.
                log.info("Stream '{}' will start from a fresh certified position", streamName);
                return bootstrapInitialPosition(streamName, builder, probe, deadProcessedToken);
            }
            case RESUME_FROM_OPLOG_START -> {
                BsonTimestamp oldestTs;
                try {
                    // The stream's own template: on multi-cluster setups the
                    // default template's oplog may be a different cluster's.
                    oldestTs = getOldestOplogTimestamp(templateFor(def));
                } catch (Exception e) {
                    // Same self-repair as RESUME_FROM_NOW (matching the logged
                    // fallback): a fresh certified position instead of an
                    // implicit non-durable "from now" with expired tokens
                    // lingering. A bootstrap failure propagates — no path
                    // starts on a non-durable position.
                    log.warn("Failed to read oplog for stream '{}': {}. Falling back to RESUME_FROM_NOW.",
                            streamName, e.getMessage());
                    return bootstrapInitialPosition(streamName, builder, probe, deadProcessedToken);
                }
                // The dead tokens deliberately STAY in the checkpoint: they
                // are the only durable marker that a recovery is due. A crash
                // before the establishment write re-enters this recovery on
                // restart (re-replaying is at-least-once safe) instead of
                // silently bootstrapping "from now". The establishment write
                // performs the deferred cleanup, guarded by the dead
                // processed token carried in the context.
                builder.resumeAt(Instant.ofEpochSecond(oldestTs.getTime()));
                log.info("Stream '{}' will resume from oldest oplog entry at {}", streamName, oldestTs);
                return new ResumeContext(null, null, oldestTs, deadProcessedToken);
            }
        }
        throw new IllegalStateException("Unknown OnHistoryLost strategy: " + strategy);
    }

    private BsonTimestamp getOldestOplogTimestamp(MongoTemplate template) {
        Document oldestEntry = template.getMongoDatabaseFactory()
                .getMongoDatabase("local")
                .getCollection("oplog.rs")
                .find()
                .sort(new Document("$natural", 1))
                .limit(1)
                .first();
        if (oldestEntry == null) {
            throw new IllegalStateException("oplog.rs is empty or not accessible");
        }
        return oldestEntry.get("ts", org.bson.BsonTimestamp.class);
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
                    cp.idleHeartbeatIntervalSeconds(),
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
