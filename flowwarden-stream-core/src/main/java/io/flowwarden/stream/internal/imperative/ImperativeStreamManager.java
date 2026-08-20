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
import io.flowwarden.stream.OnHistoryLost;
import io.flowwarden.stream.OperationType;
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
import io.flowwarden.stream.internal.StreamRestarter;
import io.flowwarden.stream.internal.discovery.PipelineMethod;
import io.flowwarden.stream.internal.lock.LeaderElectionCoordinator;
import io.flowwarden.stream.spi.ChangeEventMetadata;
import io.flowwarden.stream.spi.StopReason;
import io.flowwarden.stream.spi.StreamConfiguration;
import io.flowwarden.stream.internal.checkpoint.CheckpointHeartbeat;
import io.flowwarden.stream.internal.checkpoint.ResumeCascade;
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
import java.util.concurrent.atomic.AtomicBoolean;
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
    // save() (the hot path) is invoked from event processing; count() only
    // ever runs on the stats thread (startup, periodic tick, coalesced
    // post-write refresh). The remaining read methods are cold-path API for
    // downstream consumers.
    private final DlqStore dlqWriter;
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
    /**
     * Dedicated single thread for observability collection (periodic oplog
     * stats, DLQ backlog counts): unbounded backend I/O that must never ride
     * the flush scheduler (checkpoint coalescing) nor the probe thread. A
     * blocked count blocks only this thread — flushes, probes and event
     * processing keep advancing. Single-threaded on purpose: periodic
     * refreshes never overlap themselves, and fresh post-DLQ-write emits
     * serialize with them.
     */
    private final ScheduledExecutorService statsScheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "fw-stats");
                t.setDaemon(true);
                return t;
            });

    /**
     * Per-stream state, installed BEFORE the container registration so an
     * immediately-dying reading task finds it (handshake): the subscription
     * arrives through the holder once {@code register} returns, and the
     * termination flag tells {@code startStream} that the error handler
     * already tore this generation down — no seed, no schedules, no started
     * signal for a ghost stream.
     */
    /**
     * Per-stream state. The invalidate bookkeeping lives HERE, per
     * generation — a late DROP/INVALIDATE delivery from an old subscription
     * must never repair, terminate or classify the generation that replaced
     * it ({@code pendingInvalidateCause} is the DROP / DROP_DATABASE /
     * RENAME preceding an INVALIDATE; {@code invalidatedTerminal} marks a
     * terminal classification — rename, or any invalidation under
     * {@code OnHistoryLost.FAIL}).
     */
    private record StreamState(
            MessageListenerContainer container,
            AtomicReference<Subscription> subscriptionHolder,
            ChangeStreamDefinition definition,
            AtomicBoolean terminated,
            ImperativeHeartbeatProbe probe,
            AtomicReference<OperationType> pendingInvalidateCause,
            AtomicBoolean invalidatedTerminal) {
    }

    /**
     * Managed resubscription after a runtime cursor death: full startup path
     * (resume cascade included) with capped exponential backoff. Terminal
     * failures stop the loop and, under SINGLE_LEADER, release the lock.
     */
    private final StreamRestarter restarter =
            new StreamRestarter("fw-stream-restart", new StreamRestarter.Callbacks() {
                @Override
                public void startStream(String streamName) {
                    ImperativeStreamManager.this.startStream(streamName);
                }

                @Override
                public boolean isInstalled(String streamName) {
                    return streams.containsKey(streamName);
                }

                @Override
                public void onTerminalGiveUp(String streamName) {
                    // The lease must not be renewed for a stream the loop
                    // gave up on — release it so a standby's operator at
                    // least sees the same terminal failure honestly.
                    releaseSingleLeaderLease(streamName);
                }
            });

    public ImperativeStreamManager(MongoTemplateRegistry templateRegistry,
                                   StreamRegistry registry,
                                   CheckpointStore checkpointStore,
                                   DlqStore dlqWriter,
                                   LeaderElectionCoordinator leaderElection) {
        this.templateRegistry = templateRegistry;
        this.defaultTemplate = templateRegistry.getDefaultTemplate();
        this.registry = registry;
        this.checkpointStore = checkpointStore;
        this.dlqWriter = dlqWriter;
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
        scheduleStatsRefresh();
    }

    /**
     * Per-stream lifecycle serialization: manual starts, operator stops and
     * managed restart attempts are mutually exclusive for a given stream.
     * This is what makes ownership sound end to end — a stop issued while a
     * restart attempt is inside {@code startStream} is guaranteed to run
     * after it and tear down exactly the generation the attempt installed,
     * and a manual start can never race a restart into two concurrent
     * subscriptions ({@code containsKey} and {@code put} are atomic under
     * the lock).
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

        MongoTemplate streamTemplate = templateFor(def);
        MessageListenerContainer container = createContainer(streamTemplate, streamName);

        // Resolve the @Pipeline stages once: the main stream and the heartbeat
        // probe MUST observe the exact same pipeline documents.
        List<Document> resolvedPipeline = def.pipelineMethod() != null
                ? def.pipelineMethod().resolve(def.bean())
                : List.of();
        ImperativeHeartbeatProbe probe =
                new ImperativeHeartbeatProbe(streamTemplate, def, resolvedPipeline);
        StreamState state = new StreamState(container, new AtomicReference<>(), def,
                new AtomicBoolean(false), probe,
                new AtomicReference<>(), new AtomicBoolean(false));

        MessageListener<ChangeStreamDocument<Document>, Document> listener =
                new FlowWardenMessageListenerWrapper(
                        message -> handleMessage(message, def, state),
                        def.streamName(),
                        // Lock-free termination signal FIRST (a startStream
                        // holding the lifecycle lock must see the death of
                        // its own registration), then the COMPLETE fail-stop
                        // under the lock: the subscription cancel must not
                        // wait for the wrapper's metrics emission — a
                        // blocked provider would otherwise keep the reading
                        // task alive on an already-evicted generation (the
                        // marker path's cancel stays as an idempotent net).
                        () -> {
                            state.terminated().set(true);
                            synchronized (lifecycleLock(def.streamName())) {
                                cancelSubscriptionBestEffort(def.streamName(),
                                        state.subscriptionHolder().get());
                                clearStreamStateIf(def.streamName(), state);
                            }
                        });
        ChangeStreamRequest.ChangeStreamRequestBuilder<Document> builder = ChangeStreamRequest.builder()
                .collection(def.collection())
                .publishTo(listener);

        ResumeContext resumeContext = ResumeContext.NONE;
        if (def.checkpointAnnotation() != null
                && def.checkpointAnnotation().startPosition() == StartPosition.RESUME) {
            resumeContext = ResumeCascade.resolve(streamName, def.checkpointAnnotation(),
                    checkpointStore, probe,
                    token -> isTokenValid(def.collection(), token, streamTemplate, streamName),
                    () -> getOldestOplogTimestamp(streamTemplate));
            if (resumeContext.seedToken() != null) {
                builder.resumeAfter(resumeContext.seedToken());
            } else if (resumeContext.initialOperationTime() != null) {
                builder.resumeAt(Instant.ofEpochSecond(
                        resumeContext.initialOperationTime().getTime()));
            }
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

        // Handshake: the state is installed BEFORE register — the container
        // is already running and submits the reading task immediately, so an
        // error can fire before register() even returns. The handler must
        // find this generation's state, not reconstruct a ghost stream.
        streams.put(streamName, state);

        container.start();
        // The custom ErrorHandler is the only observation point for cursor
        // death: without it, a non-resumable cursor error (the driver
        // auto-resumes transient ones internally) kills the reading task
        // through Spring's default logging handler — no SPI signal, no state
        // eviction, and the heartbeat keeps confirming the checkpoint of a
        // dead stream.
        Subscription subscription = container.register(request, Document.class,
                error -> handleStreamError(streamName, state, error));
        state.subscriptionHolder().set(subscription);

        if (state.terminated().get()) {
            // Died during registration (the death signal is lock-free, so it
            // is visible even while this thread holds the lifecycle lock):
            // do not seed, do not schedule, do not report a ghost stream as
            // started. Cancel the just-received subscription — idempotent on
            // Spring's asynchronous path (the death commit cancels through
            // the holder), required for a custom container invoking the
            // listener synchronously before register() returned the
            // subscription. The eviction is deliberately NOT done here — the
            // ownership stays with the asynchronous death commit (or with an
            // operator stop if it wins the lock first), so exactly one of
            // them evicts and only the death commit may arm the restart.
            cancelSubscriptionBestEffort(streamName, subscription);
            log.warn("Change Stream '{}' terminated during registration", streamName);
            return;
        }

        // Seed the in-memory snapshot with the resume position so the first
        // heartbeat probe chains from it instead of waiting for an event.
        // SEED, not EVENT: a resume position (possibly the older processed
        // token under PROCESSED_FIRST) must never be persisted as a newly
        // delivered seen token.
        if (seedToken != null) {
            latestTokens.computeIfAbsent(streamName, k -> new AtomicReference<>())
                    .set(new TokenSnapshot(seedToken, Instant.now(), TokenSnapshot.Source.SEED));
        }

        scheduleIntervalCheckpoint(def, probe, resumeContext);
        log.info("Started Change Stream '{}' on collection '{}'", streamName, def.collection());

        FlowWardenMetrics.get().onStreamStarted(streamName, buildStreamConfiguration(def, "IMPERATIVE"));
    }

    /**
     * Invoked by the container's reading task through the registered
     * {@code ErrorHandler}. Provenance decides the semantics:
     *
     * <ul>
     *   <li><strong>Listener crashes</strong> — the crash wrapper rethrows a
     *       {@link FlowWardenMessageListenerWrapper.ListenerCrashedException}
     *       after signalling and evicting. Historic fail-stop, restored: the
     *       subscription is cancelled (Spring's default handler used to do
     *       this — an untracked consumer must never keep reading), no
     *       restart.</li>
     *   <li><strong>Cursor death</strong> — any other throwable comes from
     *       the reading task's own loop (which cancelled itself before
     *       invoking the handler; the wrapper lets nothing else escape the
     *       listener): signal, evict, hand to the managed restart loop.</li>
     * </ul>
     *
     * <p>The handler operates on <em>its own generation's</em> state: a
     * graceful {@code stopStream} marks it terminated before closing the
     * cursor (whose death throe would otherwise look like a crash), and the
     * conditional eviction can never touch a newer generation registered
     * under the same name.</p>
     */
    /**
     * Test hook: runs inside the lifecycle-locked death section, between the
     * termination claim and the restarter hand-off.
     */
    volatile Runnable deathHandoffTestHook;

    private void handleStreamError(String streamName, StreamState state, Throwable error) {
        try {
            // Split transition (review rounds 4-5): the local termination
            // SIGNAL is lock-free — a startStream currently holding the
            // lifecycle lock must be able to observe the death of its own
            // registration before publishing schedules and the started
            // signal. The lifecycle COMMIT (identity-conditional eviction +
            // restarter hand-off) runs under the lock: a concurrent stop is
            // either fully before it (this generation loses ownership, no
            // hand-off) or fully after it (its cancel() kills the restart
            // the hand-off just armed).
            if (error instanceof FlowWardenMessageListenerWrapper.ListenerCrashedException) {
                // Fail-stop: onCrash already signalled, cancelled and
                // evicted — this branch is the idempotent safety net (a
                // failed physical close is retried, but the logical cleanup
                // no longer depends on it).
                state.terminated().set(true);
                cancelSubscriptionBestEffort(streamName, state.subscriptionHolder().get());
                synchronized (lifecycleLock(streamName)) {
                    clearStreamStateIf(streamName, state);
                }
                return;
            }
            if (!state.terminated().compareAndSet(false, true)) {
                return; // graceful stop's death throe, or already handled
            }
            log.error("Change stream cursor died for stream '{}': {}",
                    streamName, error.getMessage(), error);
            try {
                FlowWardenMetrics.get().onStreamStopped(streamName, StopReason.CRASHED, error);
            } catch (Exception metricsError) {
                log.warn("Metrics provider failed on crash signal for stream '{}': {}",
                        streamName, metricsError.getMessage());
            }
            synchronized (lifecycleLock(streamName)) {
                boolean owned = clearStreamStateIf(streamName, state);
                if (deathHandoffTestHook != null) {
                    deathHandoffTestHook.run();
                }
                if (owned) {
                    if (state.invalidatedTerminal().get()) {
                        // Rename, or invalidation under FAIL: no restart —
                        // operator action required (the invalidate marker in
                        // the checkpoint keeps later boots honest too).
                        log.error("Stream '{}' stopped terminally after invalidation — "
                                + "no automatic restart", streamName);
                        releaseSingleLeaderLease(streamName);
                    } else {
                        restarter.onRuntimeDeath(streamName, error);
                    }
                }
            }
        } catch (RuntimeException e) {
            // Never propagate: this runs inside the reading task's own error
            // path — a throw here would kill the executor thread silently.
            log.error("Stream error handling failed for stream '{}'", streamName, e);
        }
    }

    /** Test seam: the reading-task container for one stream. */
    MessageListenerContainer createContainer(MongoTemplate streamTemplate, String streamName) {
        // The container gets a template whose database factory stamps
        // comment "flowwarden:<stream>" on the change stream cursor for
        // $currentOp attribution — Spring Data exposes no comment option, so
        // the stamp rides the only object the cursor is created from. Same
        // converter, so document mapping is untouched.
        MongoTemplate stamped = new MongoTemplate(
                io.flowwarden.stream.internal.CursorCommentStamping.stamp(
                        streamTemplate.getMongoDatabaseFactory(),
                        io.flowwarden.stream.internal.CursorCommentStamping.commentFor(streamName)),
                streamTemplate.getConverter());
        // Named reading threads (Spring's default executor spawns anonymous
        // "SimpleAsyncTaskExecutor-N" threads). Deliberately NON-daemon: in a
        // non-web application whose only permanent work is the change stream,
        // this reading thread may be the last non-daemon thread alive after
        // main() returns — making it daemon would let the JVM exit while the
        // Spring context and the stream are healthy.
        org.springframework.core.task.SimpleAsyncTaskExecutor executor =
                new org.springframework.core.task.SimpleAsyncTaskExecutor("fw-stream-listener-");
        return new DefaultMessageListenerContainer(stamped, executor);
    }

    /**
     * Best-effort physical close: {@link Subscription#cancel()} declares
     * {@code DataAccessResourceFailureException} — the logical cleanup
     * (eviction, restartability) must never depend on the cursor's physical
     * closure succeeding.
     */
    private void cancelSubscriptionBestEffort(String streamName, Subscription subscription) {
        if (subscription == null) {
            return;
        }
        try {
            subscription.cancel();
        } catch (RuntimeException e) {
            log.warn("Failed to cancel subscription for stream '{}': {}",
                    streamName, e.getMessage());
        }
    }

    @Override
    public void stopStream(String streamName) {
        // The cancel runs INSIDE the lifecycle lock: the death hand-off is
        // itself lifecycle-locked, so either a death published its restart
        // before this section (the cancel finds and kills it) or the death
        // section runs after the stop and is absorbed by the terminated
        // flag. An in-flight restart attempt holding the lock via
        // startStream is waited out, then its installed generation is torn
        // down here — the operator stop is always the last lifecycle owner.
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

        StreamState state = streams.remove(streamName);
        if (state == null) {
            log.warn("Stream '{}' is not running", streamName);
            return;
        }

        lastActivityTimes.remove(streamName);
        eventCounters.remove(streamName);

        // Mark THIS generation terminated before closing the cursor: its
        // death throe on the task thread would otherwise reach the error
        // handler and masquerade as a runtime crash.
        state.terminated().set(true);
        try {
            Subscription subscription = state.subscriptionHolder().get();
            if (subscription != null) {
                subscription.cancel();
            }
            state.container().stop();
        } catch (Exception e) {
            log.warn("Error stopping stream '{}'", streamName, e);
        }

        FlowWardenMetrics.get().onStreamStopped(streamName, StopReason.GRACEFUL, null);
        log.info("Stopped Change Stream '{}'", streamName);
    }

    /**
     * Generation-safe eviction, invoked by the crash wrapper and the error
     * handler. The state entry is removed only if it still belongs to the
     * given generation ({@code remove(key, value)}): a late termination of
     * an old subscription can never evict a stream that was restarted under
     * the same name. When the entry was already removed (wrapper eviction
     * followed by the error handler's), the map cleanup already happened —
     * skipping it protects the newer generation's tasks.
     */
    private boolean clearStreamStateIf(String streamName, StreamState expected) {
        if (!streams.remove(streamName, expected)) {
            return false;
        }
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
        return true;
    }

    @Override
    public boolean isRunning(String streamName) {
        StreamState state = streams.get(streamName);
        if (state == null || state.terminated().get()) {
            return false;
        }
        Subscription subscription = state.subscriptionHolder().get();
        return subscription != null && subscription.isActive();
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

    /**
     * Reaction to a delivered INVALIDATE event (the underlying cursor is
     * about to die — MongoDB closes it right after). The pre-invalidate
     * cause and the stream's {@code onHistoryLost} strategy decide:
     *
     * <ul>
     *   <li><strong>Terminal</strong> (rename — the declared collection
     *       identity is gone — or any invalidation under {@code FAIL}): mark
     *       the coming cursor death as give-up, and persist the invalidate
     *       token as the seen position. That token is deliberately NOT a
     *       usable resume position ({@code resumeAfter} rejects it), so
     *       every future cascade escalates to level 3 where {@code FAIL}
     *       keeps failing loudly instead of silently replaying.</li>
     *   <li><strong>Self-heal</strong> (drop under a self-repairing
     *       strategy): repair the checkpoint NOW, synchronously — the
     *       restart that follows the death must cascade onto a sane
     *       position; resuming any pre-invalidate token would replay the
     *       invalidate in a loop. {@code RESUME_FROM_OPLOG_START} heals like
     *       {@code RESUME_FROM_NOW} here: a change stream cannot replay
     *       ACROSS an invalidate.</li>
     * </ul>
     */
    /**
     * Test hook: runs after the lock-free part of the invalidate handling
     * (signal, probe, termination claim) and before the lifecycle-locked
     * repair/commit section — the TOCTOU window a stop/start can race into.
     */
    volatile Runnable invalidateRepairTestHook;

    private void handleInvalidate(ChangeStreamDefinition def, ChangeStreamDocument<Document> raw,
                                  StreamState state) {
        String streamName = def.streamName();
        // Generation fencing, cheap early exit — re-checked under the
        // lifecycle lock before any shared mutation (the slow work below can
        // race a stop/start).
        if (streams.get(streamName) != state) {
            log.debug("Ignoring INVALIDATE from a previous generation of stream '{}'", streamName);
            return;
        }
        // The RUNNING exit comes FIRST — before any external callback and any
        // network I/O. An invalidate closes the cursor cleanly (tryNext
        // returns null forever, no throwable ever reaches the ErrorHandler):
        // this generation must be terminated HERE, and the health transition
        // must not wait for a slow metrics provider or the repair probe —
        // that would recreate the very RUNNING-while-dead symptom of the
        // original issue.
        boolean claimed = state.terminated().compareAndSet(false, true);

        OperationType cause = state.pendingInvalidateCause().get() != null
                ? state.pendingInvalidateCause().get() : OperationType.DROP;
        log.error("Watched collection invalidated for stream '{}' (cause: {})", streamName, cause);
        try {
            FlowWardenMetrics.get().onStreamInvalidated(streamName, cause);
        } catch (Exception metricsError) {
            log.warn("Metrics provider failed on invalidate signal for stream '{}': {}",
                    streamName, metricsError.getMessage());
        }
        if (claimed) {
            try {
                FlowWardenMetrics.get().onStreamStopped(streamName, StopReason.CRASHED, null);
            } catch (Exception metricsError) {
                log.warn("Metrics provider failed on crash signal for stream '{}': {}",
                        streamName, metricsError.getMessage());
            }
        }

        OnHistoryLost strategy = def.checkpointAnnotation() != null
                ? def.checkpointAnnotation().onHistoryLost() : null;
        boolean terminal = cause == OperationType.RENAME || strategy == OnHistoryLost.FAIL;
        BsonDocument invalidateToken = raw.getResumeToken();
        Instant now = Instant.now();

        if (terminal) {
            state.invalidatedTerminal().set(true); // this generation's field
            if (cause == OperationType.RENAME) {
                log.error("Stream '{}' stops terminally: the watched collection was renamed — "
                        + "redeploy with @ChangeStream pointing at the new collection", streamName);
            }
        }

        // Slow I/O OUTSIDE the lock, AFTER the health transition: the
        // fresh-position probe must neither hold the lifecycle lock across a
        // network round-trip nor delay the RUNNING exit above. Its aggregate
        // carries a server-side maxTimeMS, but that does not bound server
        // selection or a client socket read — a wall-clock bound would have
        // to come from the Mongo client's own timeouts, which is exactly why
        // the health transition must not wait for this call.
        BsonDocument fresh = null;
        RuntimeException probeFailure = null;
        if (!terminal && def.checkpointAnnotation() != null) {
            try {
                fresh = state.probe().initialPosition();
            } catch (RuntimeException e) {
                probeFailure = e;
            }
        }

        if (invalidateRepairTestHook != null) {
            invalidateRepairTestHook.run();
        }

        synchronized (lifecycleLock(streamName)) {
            // TOCTOU fence: a stop/start may have installed a newer
            // generation while this one was signalling or probing. A stale
            // generation must not touch the checkpoint (it would even read
            // the NEW generation's processed token as the dead-guard and
            // legitimately clear it), the shared snapshot, or the restart
            // lifecycle.
            if (streams.get(streamName) != state) {
                log.debug("Discarding invalidate repair of a previous generation of stream '{}'",
                        streamName);
                return;
            }
            if (def.checkpointAnnotation() != null) {
                if (terminal) {
                    persistInvalidateMarker(streamName, def, invalidateToken, now);
                } else if (fresh != null) {
                    try {
                        BsonDocument deadProcessed = checkpointStore.findByStreamName(streamName)
                                .map(io.flowwarden.stream.spi.Checkpoint::lastProcessedToken)
                                .orElse(null);
                        checkpointStore.resetAfterHistoryLost(streamName, fresh, deadProcessed, now);
                        latestTokens.computeIfAbsent(streamName, k -> new AtomicReference<>())
                                .set(new TokenSnapshot(fresh, now, TokenSnapshot.Source.SEED));
                        log.info("Stream '{}' checkpoint self-repaired after invalidate — the "
                                + "restart will resume from a fresh certified position", streamName);
                    } catch (RuntimeException e) {
                        // Best-effort fallback: the marker forces every
                        // future cascade to level 3, where the self-repairing
                        // strategy heals at restart time instead of replaying
                        // the invalidate.
                        log.warn("Post-invalidate self-repair failed for stream '{}' ({}) — "
                                + "falling back to the level-3 marker", streamName, e.getMessage());
                        persistInvalidateMarker(streamName, def, invalidateToken, now);
                    }
                } else {
                    log.warn("Post-invalidate self-repair probe failed for stream '{}' ({}) — "
                            + "falling back to the level-3 marker", streamName,
                            probeFailure != null ? probeFailure.getMessage() : "no position");
                    persistInvalidateMarker(streamName, def, invalidateToken, now);
                }
            }
            if (claimed) {
                cancelSubscriptionBestEffort(streamName, state.subscriptionHolder().get());
                boolean owned = clearStreamStateIf(streamName, state);
                if (owned) {
                    if (state.invalidatedTerminal().get()) {
                        log.error("Stream '{}' stopped terminally after invalidation — "
                                + "no automatic restart", streamName);
                        releaseSingleLeaderLease(streamName);
                    } else {
                        restarter.onRuntimeDeath(streamName, null);
                    }
                }
            }
        }
    }

    /**
     * Durable invalidate marker: the invalidate token becomes the seen
     * position (deliberately unusable — {@code resumeAfter} rejects it) and
     * the processed pair is cleared atomically under the store's guard.
     * Every future cascade is forced to level 3 regardless of
     * {@code ResumeStrategy} — a still-valid pre-invalidate processed token
     * would otherwise win level 1 and replay the invalidation in a loop.
     */
    private void persistInvalidateMarker(String streamName, ChangeStreamDefinition def,
                                         BsonDocument invalidateToken, Instant now) {
        if (def.checkpointAnnotation() == null || invalidateToken == null) {
            return;
        }
        try {
            BsonDocument deadProcessed = checkpointStore.findByStreamName(streamName)
                    .map(io.flowwarden.stream.spi.Checkpoint::lastProcessedToken)
                    .orElse(null);
            checkpointStore.resetAfterHistoryLost(streamName, invalidateToken, deadProcessed, now);
        } catch (RuntimeException e) {
            log.warn("Failed to persist the invalidate marker for stream '{}': {}",
                    streamName, e.getMessage());
        }
    }

    /**
     * Under {@code SINGLE_LEADER}, stop the election so the lock is released
     * instead of being renewed for a stream that terminally gave up.
     */
    private void releaseSingleLeaderLease(String streamName) {
        ChangeStreamDefinition def = registry.findByName(streamName).orElse(null);
        if (def != null && def.config().deploymentMode() == DeploymentMode.SINGLE_LEADER
                && leaderElection != null) {
            leaderElection.stop(streamName);
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down FlowWarden ImperativeStreamManager ({} streams)", streams.size());
        restarter.shutdown();
        if (leaderElection != null) {
            leaderElection.shutdown();
        }
        // Snapshot before iterating: a crash eviction on a listener thread
        // mutates the map concurrently, and the weakly-consistent iterator
        // could then skip a live stream (leaking its container).
        for (String streamName : new java.util.ArrayList<>(streams.keySet())) {
            stopStream(streamName);
        }
        intervalScheduler.shutdownNow();
        probeScheduler.shutdownNow();
        statsScheduler.shutdownNow();
    }

    @SuppressWarnings("unchecked")
    private void handleMessage(Message<ChangeStreamDocument<Document>, Document> message,
                               ChangeStreamDefinition def,
                               StreamState state) {
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
                            dlqWriter.save(new FailedEvent(
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
                        emitDlqBacklogAsync(def.streamName());
                    }

                    @Override
                    public void saveCheckpointNow() {
                        if (isLifecycleEvent(ctx.getOperationType())) {
                            // A lifecycle event's token is never a usable
                            // resume position — anchoring it manually would
                            // recreate exactly what the invalidate handling
                            // guards against (a checkpointed RENAME comes
                            // back as an unclassifiable INVALIDATE).
                            log.warn("Ignoring manual checkpoint of a {} event for stream '{}': "
                                    + "lifecycle tokens are not resume positions",
                                    ctx.getOperationType(), def.streamName());
                            return;
                        }
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

        OperationType opType = ctx.getOperationType();
        if (opType == OperationType.DROP || opType == OperationType.DROP_DATABASE
                || opType == OperationType.RENAME) {
            // Remember the pre-invalidate cause on THIS generation: the
            // INVALIDATE that follows carries no indication of what killed
            // the collection.
            state.pendingInvalidateCause().set(opType);
        } else if (opType == OperationType.INVALIDATE) {
            // INVALIDATE is a lifecycle-internal event: SPI signal + repair
            // or terminal stop, never dispatched to application handlers —
            // a manual ctx.saveCheckpointNow() on its (unusable) token would
            // overwrite the repair.
            handleInvalidate(def, raw, state);
            return;
        }

        HandlerMethod handler = def.resolveHandler(opType);
        if (handler == null) {
            log.debug("No handler for {} in stream '{}'", opType, def.streamName());
            publishSettledToken(def, raw, opType);
            return;
        }

        if (def.filterMethod() != null) {
            if (!def.filterMethod().evaluate(def.bean(), ctx)) {
                log.debug("Event filtered by @Filter in stream '{}'", def.streamName());
                publishSettledToken(def, raw, opType);
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
                if (!ctx.isCheckpointSavedManually() && !isLifecycleEvent(opType)) {
                    // Lifecycle events never anchor a resume position: an
                    // invalidate token is rejected by resumeAfter, and a
                    // drop/rename token would resume PAST the very event the
                    // next boot needs to replay to reclassify the
                    // invalidation (a checkpointed RENAME would come back as
                    // an unclassifiable INVALIDATE and be self-healed).
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
            publishSettledToken(def, raw, opType);
        }
    }

    /**
     * Publishes a resume token to the heartbeat's shared snapshot. Only
     * called on terminal-safe outcomes — a token observed at receipt is
     * never certifiable while its handler outcome is undecided. An INVALIDATE
     * token is excluded entirely: it is not a usable resume position
     * ({@code resumeAfter} rejects it) and must never overwrite the fresh
     * position the post-invalidate self-repair just installed.
     */
    private void publishSettledToken(ChangeStreamDefinition def,
                                     ChangeStreamDocument<Document> raw,
                                     OperationType opType) {
        if (isLifecycleEvent(opType)) {
            return;
        }
        if (def.checkpointAnnotation() != null && raw.getResumeToken() != null) {
            latestTokens.computeIfAbsent(def.streamName(), k -> new AtomicReference<>())
                    .set(new TokenSnapshot(raw.getResumeToken(), Instant.now()));
        }
    }

    /**
     * Collection-lifecycle events (as opposed to data events). Their tokens
     * never become resume positions — see the saveCheckpointIfNeeded and
     * publishSettledToken call sites.
     */
    private static boolean isLifecycleEvent(OperationType opType) {
        return opType == OperationType.INVALIDATE
                || opType == OperationType.DROP
                || opType == OperationType.DROP_DATABASE
                || opType == OperationType.RENAME;
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

            dlqWriter.save(failedEvent, policy);
            FlowWardenMetrics.get().onEventSentToDlq(def.streamName());
            log.info("Event sent to DLQ for stream '{}'", def.streamName());
            emitDlqBacklogAsync(def.streamName());
        } catch (Exception e) {
            FlowWardenMetrics.get().onEventDlqFailed(def.streamName(), e);
            log.error("Failed to send event to DLQ for stream '{}': {}", def.streamName(), e.getMessage(), e);
        }
    }

    private final Map<String, AtomicBoolean> backlogEmitScheduled = new ConcurrentHashMap<>();
    private final Map<String, AtomicBoolean> backlogEmitDirty = new ConcurrentHashMap<>();

    /**
     * Fresh backlog gauge right after a successful DLQ write, submitted to
     * the stats thread: the count is unbounded backend I/O and must never
     * hold up the event-processing path — the DLQ entry is already durable.
     *
     * <p>Coalesced per stream: at most one count queued or running, plus a
     * dirty bit granting exactly one more pass when a write lands during an
     * active count. A blocked backend therefore costs a bounded queue (one
     * task per stream, not one per write) and its return triggers one
     * catch-up count, not one per missed write — while the LAST state is
     * always eventually published. Each pass runs as its own task so a
     * write flood never monopolizes the stats thread.</p>
     */
    private void emitDlqBacklogAsync(String streamName) {
        AtomicBoolean scheduled =
                backlogEmitScheduled.computeIfAbsent(streamName, k -> new AtomicBoolean());
        AtomicBoolean dirty =
                backlogEmitDirty.computeIfAbsent(streamName, k -> new AtomicBoolean());
        dirty.set(true);
        if (scheduled.compareAndSet(false, true)) {
            submitToStats(() -> runCoalescedBacklogEmit(streamName, scheduled, dirty));
        }
    }

    private void runCoalescedBacklogEmit(String streamName,
                                         AtomicBoolean scheduled,
                                         AtomicBoolean dirty) {
        dirty.set(false);
        try {
            emitDlqBacklog(streamName);
        } finally {
            scheduled.set(false);
            // A write that landed during the count saw scheduled=true and
            // did not submit — pick it up here (exactly one of the two
            // racing sides wins the CAS).
            if (dirty.get() && scheduled.compareAndSet(false, true)) {
                submitToStats(() -> runCoalescedBacklogEmit(streamName, scheduled, dirty));
            }
        }
    }

    private void submitToStats(Runnable task) {
        try {
            statsScheduler.execute(task);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // Shutting down — the gauge is best-effort.
        }
    }

    private void emitDlqBacklog(String streamName) {
        try {
            long backlog = dlqWriter.count(streamName);
            if (backlog >= 0) {
                FlowWardenMetrics.get().onDlqBacklog(streamName, backlog);
            }
        } catch (Exception e) {
            log.debug("DLQ backlog count failed for stream '{}': {}", streamName, e.getMessage());
        }
    }

    private void registerDlqCollections() {
        if (!(dlqWriter instanceof MongoDlqStore mongoStore)) {
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

    boolean isTokenValid(String collection, BsonDocument token, MongoTemplate template,
                         String streamName) {
        try (MongoCursor<ChangeStreamDocument<Document>> cursor =
                     template.getCollection(collection).watch()
                             .resumeAfter(token)
                             .comment(io.flowwarden.stream.internal.CursorCommentStamping
                                     .validationCommentFor(streamName))
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
     * Schedules the periodic stats refresh (oplog window + DLQ backlogs)
     * on the dedicated stats thread. Fixed DELAY, not fixed rate: an
     * observability poll doing I/O means "60s after the previous pass
     * finished" — a pass blocked for minutes must not be followed by a
     * catch-up burst of the missed deadlines.
     */
    private void scheduleStatsRefresh() {
        // Initial delay 0: the startup collection IS the first pass of the
        // same fixed-delay chain — a blocked initial pass therefore delays
        // the first tick instead of being overlapped by it.
        statsScheduler.scheduleWithFixedDelay(this::collectStats,
                0, OPLOG_REFRESH_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    /** One stats pass — the first one doubles as the startup collection. */
    private void collectStats() {
        try {
            collectOplogStats();
        } catch (Exception e) {
            log.debug("Oplog stats refresh failed: {}", e.getMessage());
        }
        try {
            collectDlqBacklogs();
        } catch (Exception e) {
            log.debug("DLQ backlog refresh failed: {}", e.getMessage());
        }
    }

    /**
     * Pushes the standing DLQ backlog gauge for every DLQ-enabled stream —
     * whether or not the stream currently runs, its backlog stays
     * console-relevant. Best-effort per stream; a store that cannot count
     * (negative) emits nothing rather than a lying zero.
     */
    private void collectDlqBacklogs() {
        for (ChangeStreamDefinition def : registry.getDefinitions()) {
            DeadLetterQueue dlqAnn = def.deadLetterQueueAnnotation();
            if (dlqAnn == null || !dlqAnn.enabled()) {
                continue;
            }
            emitDlqBacklog(def.streamName());
        }
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
