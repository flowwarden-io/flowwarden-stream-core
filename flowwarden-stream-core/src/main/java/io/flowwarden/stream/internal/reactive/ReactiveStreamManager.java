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
import io.flowwarden.stream.internal.StreamRestarter;
import io.flowwarden.stream.internal.lock.LeaderElectionCoordinator;
import io.flowwarden.stream.spi.ChangeEventMetadata;
import io.flowwarden.stream.internal.checkpoint.CheckpointHeartbeat;
import io.flowwarden.stream.internal.checkpoint.ResumeCascade;
import io.flowwarden.stream.internal.checkpoint.ResumeContext;
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
    private final Map<String, ScheduledFuture<?>> idleProbeTasks = new ConcurrentHashMap<>();
    private final Map<String, CheckpointHeartbeat> heartbeats = new ConcurrentHashMap<>();
    private final ScheduledExecutorService intervalScheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "fw-checkpoint-interval-reactive");
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
                Thread t = new Thread(r, "fw-heartbeat-probe-reactive");
                t.setDaemon(true);
                return t;
            });

    private record ReactiveStreamState(
            AtomicReference<Disposable> disposableHolder,
            ChangeStreamDefinition definition,
            AtomicBoolean gracefulStop,
            AtomicReference<Throwable> lastError) {
    }

    /**
     * Managed resubscription after a runtime termination (cursor error or
     * unexpected completion — e.g. an invalidate): full startup path (resume
     * cascade included) with capped exponential backoff. Terminal failures
     * stop the loop and, under SINGLE_LEADER, release the lock.
     */
    private final StreamRestarter restarter =
            new StreamRestarter("fw-stream-restart-reactive", new StreamRestarter.Callbacks() {
                @Override
                public void startStream(String streamName) {
                    ReactiveStreamManager.this.startStream(streamName);
                }

                @Override
                public boolean isInstalled(String streamName) {
                    return streams.containsKey(streamName);
                }

                @Override
                public void onTerminalGiveUp(String streamName) {
                    ChangeStreamDefinition def = registry.findByName(streamName).orElse(null);
                    if (def != null && def.config().deploymentMode() == DeploymentMode.SINGLE_LEADER
                            && leaderElection != null) {
                        // The lease must not be renewed for a stream the loop
                        // gave up on — release it so a standby's operator at
                        // least sees the same terminal failure honestly.
                        leaderElection.stop(streamName);
                    }
                }
            });

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

    /**
     * Per-stream lifecycle serialization: manual starts, operator stops and
     * managed restart attempts are mutually exclusive for a given stream —
     * see the imperative manager's twin field for the full rationale.
     */
    private final Map<String, Object> lifecycleLocks = new ConcurrentHashMap<>();

    private Object lifecycleLock(String streamName) {
        return lifecycleLocks.computeIfAbsent(streamName, k -> new Object());
    }

    @Override
    public void startStream(String streamName) {
        synchronized (lifecycleLock(streamName)) {
            doStartStream(streamName);
        }
    }

    private void doStartStream(String streamName) {
        if (streams.containsKey(streamName)) {
            log.warn("Stream '{}' is already running", streamName);
            return;
        }

        ChangeStreamDefinition def = findDefinition(streamName);
        ReactiveMongoTemplate streamTemplate = templateFor(def);

        ChangeStreamOptions.ChangeStreamOptionsBuilder optionsBuilder = ChangeStreamOptions.builder();

        // Resolve the @Pipeline stages once: the main stream and the heartbeat
        // probe MUST observe the exact same pipeline documents.
        List<Document> resolvedPipeline = def.pipelineMethod() != null
                ? def.pipelineMethod().resolve(def.bean())
                : List.of();
        ReactiveHeartbeatProbe probe =
                new ReactiveHeartbeatProbe(streamTemplate, def, resolvedPipeline);

        ResumeContext resumeContext = ResumeContext.NONE;
        if (def.checkpointAnnotation() != null
                && def.checkpointAnnotation().startPosition() == StartPosition.RESUME) {
            resumeContext = ResumeCascade.resolve(streamName, def.checkpointAnnotation(),
                    checkpointStore, probe,
                    token -> isTokenValid(def.collection(), token, streamTemplate),
                    () -> getOldestOplogTimestamp(streamTemplate));
            if (resumeContext.seedToken() != null) {
                optionsBuilder.resumeAfter(resumeContext.seedToken());
            } else if (resumeContext.initialOperationTime() != null) {
                optionsBuilder.resumeAt(resumeContext.initialOperationTime());
            }
        }
        BsonDocument seedToken = resumeContext.seedToken();

        if (def.config().fullDocument() != FullDocumentMode.DEFAULT) {
            optionsBuilder.fullDocumentLookup(FullDocument.valueOf(def.config().fullDocument().name()));
        }
        if (def.config().fullDocumentBeforeChange() != FullDocumentBeforeChangeMode.OFF) {
            optionsBuilder.fullDocumentBeforeChangeLookup(
                    FullDocumentBeforeChange.valueOf(def.config().fullDocumentBeforeChange().name()));
        }

        if (!resolvedPipeline.isEmpty()) {
            List<AggregationOperation> ops = resolvedPipeline.stream()
                    .<AggregationOperation>map(doc -> ctx -> doc)
                    .toList();
            optionsBuilder.filter(Aggregation.newAggregation(ops));
            log.debug("Applied @Pipeline with {} stages to stream '{}'",
                    resolvedPipeline.size(), streamName);
        }

        ChangeStreamOptions options = optionsBuilder.build();

        AtomicBoolean gracefulStop = new AtomicBoolean(false);
        AtomicReference<Throwable> lastError = new AtomicReference<>();
        // The pipeline can terminate synchronously during subscribe() (e.g. a
        // synchronous aggregate error). State is therefore installed BEFORE
        // subscribing, the doFinally cleans it through the maps, and the
        // termination flag tells the code below that cleanup already ran —
        // otherwise a dead stream would keep a scheduled heartbeat forever.
        AtomicReference<Disposable> disposableHolder = new AtomicReference<>();
        AtomicBoolean terminated = new AtomicBoolean(false);

        if (seedToken != null) {
            // Seed the in-memory snapshot with the resume position so the first
            // heartbeat probe chains from it instead of waiting for an event.
            // SEED, not EVENT: a resume position (possibly the older processed
            // token under PROCESSED_FIRST) must never be persisted as a newly
            // delivered seen token.
            latestTokens.computeIfAbsent(streamName, k -> new AtomicReference<>())
                    .set(new TokenSnapshot(seedToken, Instant.now(), TokenSnapshot.Source.SEED));
        }

        ReactiveStreamState state =
                new ReactiveStreamState(disposableHolder, def, gracefulStop, lastError);
        streams.put(streamName, state);
        scheduleIntervalCheckpoint(def, probe, resumeContext);

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
                    try {
                        // Observational only — a throwing provider must not
                        // replace the original cause nor break termination.
                        FlowWardenMetrics.get().onEventError(streamName, e, true, 0, null);
                    } catch (Exception metricsError) {
                        log.warn("Metrics provider failed on event error for stream '{}': {}",
                                streamName, metricsError.getMessage());
                    }
                    return Mono.empty();
                })
                .doFinally(signal -> {
                    // Lock-free termination signal FIRST: a startStream
                    // holding the lifecycle lock through subscribe() must be
                    // able to observe an asynchronous termination of its own
                    // subscription before reporting it as started.
                    terminated.set(true);
                    // The lifecycle COMMIT — ownership claim, eviction,
                    // hand-off — runs inside the lock: a concurrent
                    // stopStream is either fully before it (this termination
                    // loses ownership, or sees gracefulStop) or fully after
                    // it (its cancel() finds and kills the restart the
                    // hand-off just armed). Reentrant with the synchronous
                    // doFinally of a dispose() inside doStopStream (same
                    // thread). Lifecycle first, observables after: eviction
                    // and hand-off are guaranteed regardless of any metrics
                    // provider failure.
                    synchronized (lifecycleLock(streamName)) {
                        // Generation-safe eviction: this termination only
                        // owns the shared per-name resources if ITS state is
                        // still the registered one. A late doFinally of an
                        // old subscription must never dismantle the newer
                        // generation's tokens, heartbeat or tasks — nor arm
                        // a restart over a living stream. The heartbeat is
                        // invalidated BEFORE its task is cancelled.
                        boolean owner = streams.remove(streamName, state);
                        if (owner) {
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
                        boolean graceful = gracefulStop.get();
                        try {
                            FlowWardenMetrics.get().onStreamStopped(streamName,
                                    graceful ? StopReason.GRACEFUL : StopReason.CRASHED,
                                    graceful ? null : lastError.get());
                        } catch (Exception metricsError) {
                            log.warn("Metrics provider failed on stop signal for stream '{}': {}",
                                    streamName, metricsError.getMessage());
                        }
                        if (!graceful && owner) {
                            if (deathHandoffTestHook != null) {
                                deathHandoffTestHook.run();
                            }
                            // Runtime death (cursor error or unexpected
                            // completion): hand the stream to the managed
                            // resubscription loop — it re-enters the full
                            // startup path, resume cascade included, with
                            // capped exponential backoff.
                            restarter.onRuntimeDeath(streamName, lastError.get());
                        }
                    }
                })
                .subscribe(v -> { }, lateError ->
                        // The in-band path terminates through onErrorResume /
                        // doFinally; only a LATE driver signal (an async
                        // getMore callback completing after the pipeline
                        // already terminated) can reach this consumer — absorb
                        // it instead of throwing ErrorCallbackNotImplemented
                        // into Reactor's global onErrorDropped hook.
                        log.debug("Late change stream signal for '{}' after termination: {}",
                                streamName, lateError.toString()));
        disposableHolder.set(disposable);

        if (terminated.get()) {
            // Terminated during subscribe(): doFinally already evicted the
            // state installed above — do not report the stream as started.
            disposable.dispose();
            log.warn("Reactive Change Stream '{}' terminated during subscription", streamName);
            return;
        }

        log.info("Started reactive Change Stream '{}' on collection '{}'",
                streamName, def.collection());

        FlowWardenMetrics.get().onStreamStarted(streamName, buildStreamConfiguration(def, "REACTIVE"));
    }

    /**
     * Test hook: runs inside the lifecycle-locked death section, between the
     * ownership claim and the restarter hand-off.
     */
    volatile Runnable deathHandoffTestHook;

    @Override
    public void stopStream(String streamName) {
        // The cancel runs INSIDE the lifecycle lock: the death hand-off is
        // itself lifecycle-locked, so either a death published its restart
        // before this section (the cancel finds and kills it) or the death
        // section runs after the stop and loses its ownership claim. An
        // in-flight restart attempt holding the lock via startStream is
        // waited out, then its installed generation is torn down here — the
        // operator stop is always the last lifecycle owner.
        synchronized (lifecycleLock(streamName)) {
            restarter.cancel(streamName);
            doStopStream(streamName);
        }
    }

    private void doStopStream(String streamName) {
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

        ReactiveStreamState state = streams.remove(streamName);
        if (state == null) {
            log.warn("Stream '{}' is not running", streamName);
            return;
        }

        lastActivityTimes.remove(streamName);
        eventCounters.remove(streamName);

        state.gracefulStop().set(true);
        try {
            Disposable disposable = state.disposableHolder().get();
            if (disposable != null) {
                disposable.dispose();
            }
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
        if (state == null) {
            return false;
        }
        Disposable disposable = state.disposableHolder().get();
        return disposable != null && !disposable.isDisposed();
    }

    @Override
    public Instant getLastEventTime(String streamName) {
        return lastActivityTimes.get(streamName);
    }

    // --- package-private diagnostic hooks (down-window observability in tests) ---

    boolean hasLatestToken(String streamName) {
        return latestTokens.containsKey(streamName);
    }

    boolean hasIntervalTask(String streamName) {
        return intervalTasks.containsKey(streamName);
    }

    boolean hasHeartbeat(String streamName) {
        return heartbeats.containsKey(streamName);
    }

    boolean isRestartPending(String streamName) {
        return restarter.isRestartPending(streamName);
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down FlowWarden ReactiveStreamManager ({} streams)", streams.size());
        restarter.shutdown();
        if (leaderElection != null) {
            leaderElection.shutdown();
        }
        for (String streamName : streams.keySet()) {
            stopStream(streamName);
        }
        intervalScheduler.shutdownNow();
        probeScheduler.shutdownNow();
    }

    private Mono<Void> handleEventReactive(ChangeStreamDocument<Document> raw,
                                                                     ChangeStreamDefinition def) {
        // The event's token is NOT published to the heartbeat snapshot here:
        // a token observed at receipt is not a safe certification chain
        // source while its handler outcome is undecided (a probe chained
        // from it could certify past an event that later fails and must be
        // redelivered). Publication happens only at terminal-safe outcomes
        // — see publishSettledToken.

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
            return Mono.empty();
        }

        if (def.filterMethod() != null) {
            if (!def.filterMethod().evaluate(def.bean(), ctx)) {
                log.debug("Event filtered by @Filter in stream '{}'", def.streamName());
                publishSettledToken(def, raw);
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
            // Handler success is terminal — publish AFTER the processed
            // anchor had its chance to be written.
            publishSettledToken(def, raw);
        }).onErrorResume(e -> {
            long durationNanos = System.nanoTime() - startNanos;
            // Every error reaching this operator is terminal (SKIP, DLQ,
            // retries exhausted — retryWhen handles retries upstream; a
            // cancellation never reaches it): the event is settled, its
            // token may serve as a chain source. DLQ reserve: the DECISION
            // settles — the DLQ write below is best-effort and detached,
            // its failure is signaled but does not suspend terminality.
            publishSettledToken(def, raw);

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

    private void scheduleIntervalCheckpoint(ChangeStreamDefinition def,
                                            ReactiveHeartbeatProbe probe,
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
        // the state is installed.
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

    private BsonTimestamp getOldestOplogTimestamp(ReactiveMongoTemplate template) {
        Document oldestEntry = template.getMongoDatabaseFactory()
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
