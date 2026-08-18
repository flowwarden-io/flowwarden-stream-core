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
package io.flowwarden.stream.annotation;

import io.flowwarden.stream.OnHistoryLost;
import io.flowwarden.stream.ResumeStrategy;
import io.flowwarden.stream.StartPosition;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Enables automatic persistence of Change Stream resume tokens.
 *
 * <p>Place this annotation on a class that is also annotated with
 * {@link ChangeStream}. FlowWarden tracks two independent resume tokens
 * for each stream and uses them in a layered resume strategy on restart.</p>
 *
 * <h2>Dual-token model</h2>
 *
 * <ul>
 *   <li><strong>{@code lastSeenToken}</strong> — advances on <em>every</em> event
 *       received from MongoDB (including events rejected by {@code @Filter}, events
 *       with no matching handler, and events being retried), flushed on the
 *       {@link #saveIntervalSeconds()} cadence — and, when the stream goes idle,
 *       through the {@link #idleHeartbeatIntervalSeconds() idle heartbeat}: a
 *       bounded, ephemeral change stream probe chained from the last delivered
 *       position fetches a server-certified post-batch resume token, so the
 *       persisted position keeps tracking the oplog head with zero traffic. As
 *       long as probes succeed within the oplog window, this keeps idle or
 *       massively-filtered streams recoverable.</li>
 *   <li><strong>{@code lastProcessedToken}</strong> — advances only when a handler
 *       method returns successfully (or the event is acknowledged by the DLQ
 *       pipeline). The {@link #saveEveryN() counter} persists this token at the
 *       configured cadence to preserve at-least-once delivery on crash.</li>
 * </ul>
 *
 * <h2>3-level resume cascade</h2>
 *
 * <p>On restart with {@link StartPosition#RESUME}, the stream resumes in order
 * determined by {@link #resumeStrategy()}:</p>
 * <ol>
 *   <li>From the <em>primary</em> token (default {@code lastProcessedToken}, which
 *       preserves strict at-least-once).</li>
 *   <li>If the primary has aged out of the oplog, fall back to the <em>secondary</em>
 *       token — the stream avoids a {@code ChangeStreamHistoryLost} at the cost of
 *       either re-delivering events ({@code lastProcessedToken} secondary) or
 *       skipping in-flight events ({@code lastSeenToken} secondary).</li>
 *   <li>If both tokens are aged out, apply the {@link #onHistoryLost()} strategy.</li>
 * </ol>
 *
 * <h2>Attribute roles</h2>
 *
 * <ul>
 *   <li>{@link #saveEveryN()} = how often to persist {@code lastProcessedToken}.
 *       {@code 1} (default) writes after every handler success — recommended for
 *       at-least-once critical streams.</li>
 *   <li>{@link #saveIntervalSeconds()} = how often the heartbeat timer persists
 *       {@code lastSeenToken}. {@code 5} (default) is a safe trade-off; {@code 0}
 *       disables the timer and removes the cascade level-2 safety net.</li>
 *   <li>{@link #startPosition()} = whether to apply the cascade ({@code RESUME})
 *       or ignore both tokens ({@code LATEST}, bootstrap).</li>
 *   <li>{@link #onHistoryLost()} = strategy at cascade level 3 only.</li>
 *   <li>{@link #resumeStrategy()} = which token is tried first in the cascade
 *       (at-least-once vs fast restart).</li>
 * </ul>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Checkpoint {

    /**
     * Persist {@code lastProcessedToken} after every N successful handler invocations.
     * Must be {@code >= 1}; {@code 1} (default) gives strict at-least-once on crash.
     * Larger values reduce write pressure at the cost of replaying up to {@code N-1}
     * events on crash recovery.
     */
    int saveEveryN() default 1;

    /**
     * Write-coalescing flush interval in seconds for {@code lastSeenToken}.
     *
     * <p>Each tick persists the most recent event token received (including
     * filtered or no-handler events) — <em>only if it changed</em> since the
     * last flush. This is purely a write-pressure control on active streams:
     * a clean tick writes nothing and never opens any cursor. Keeping a
     * stream's resume point alive while it is <em>idle</em> is the separate
     * responsibility of {@link #idleHeartbeatIntervalSeconds()}.</p>
     *
     * <p>{@code 5} (default) is a balanced value; set to {@code 0} to disable
     * the periodic flush ({@code lastSeenToken} then only advances through
     * the idle heartbeat).</p>
     */
    int saveIntervalSeconds() default 5;

    /**
     * Idle heartbeat interval in seconds — the oplog-rollover protection
     * policy for streams that stop receiving events.
     *
     * <p>When the main cursor has not delivered any token for this long, the
     * heartbeat opens an ephemeral, bounded change stream probe chained from
     * the last delivered position (same collection, same resolved
     * {@code @Pipeline} stages and {@code fullDocument} options as the
     * stream, stamped with a {@code flowwarden:heartbeat:&lt;stream&gt;}
     * comment). When the server certifies the interval empty, the returned
     * post-batch resume token is persisted as {@code lastSeenToken}: the
     * resume point keeps tracking the oplog head with zero traffic — the
     * stream stays recoverable as long as probes keep succeeding within the
     * oplog window. Any activity of the main cursor re-arms the idle delay;
     * active streams never probe. This is the level-2 safety net of the
     * resume cascade for idle workloads.</p>
     *
     * <p>Timing contract (nominal, not a hard bound): idleness is checked
     * every few seconds and the probe runs as soon as the dedicated
     * single-threaded probe executor is available. All streams of an
     * instance share that executor — a queue of idle streams or a slow probe
     * can delay another stream's probe beyond the check cadence. During
     * sustained idleness, a stream's probes stay spaced one full interval
     * apart.</p>
     *
     * <p>The checkpoint's {@code lastHeartbeatTimestamp} records the last time
     * a recoverable position was confirmed (fresh event flush or successful
     * empty probe); its age is the operational signal for resume-point
     * health.</p>
     *
     * <p>The interval must stay well below the expected oplog window, with
     * enough margin to absorb transient probe failures and outages — there is
     * no universally safe value: {@code 300} (default) is a reasonable
     * trade-off for typical windows, not a guarantee against an oplog that
     * rolls faster or probes that keep failing. Set to {@code 0} to disable
     * idle probing entirely: the stream is then exposed to oplog rollover
     * whenever it stays idle longer than the window.</p>
     */
    int idleHeartbeatIntervalSeconds() default 300;

    /**
     * Where to start consuming on (re)start.
     * {@link StartPosition#RESUME} applies the 3-level cascade.
     * {@link StartPosition#LATEST} ignores both persisted tokens and starts from now.
     */
    StartPosition startPosition() default StartPosition.RESUME;

    /**
     * Strategy when <strong>both</strong> persisted tokens have aged out of the
     * MongoDB oplog (cascade level 3). With the dual-token cascade in place,
     * reaching this level signals a genuine anomaly (downtime longer than the
     * oplog window combined with timer not running).
     */
    OnHistoryLost onHistoryLost() default OnHistoryLost.FAIL;

    /**
     * Order in which the two persisted tokens are tried by the resume cascade.
     *
     * <p>{@link ResumeStrategy#PROCESSED_FIRST} (default) preserves strict
     * at-least-once delivery: in-flight events at crash time are re-delivered,
     * at the cost of a potentially long oplog scan on low-volume or
     * filter-heavy streams. {@link ResumeStrategy#SEEN_FIRST} restarts fast
     * from the heartbeat-fresh {@code lastSeenToken} and falls back to
     * {@code lastProcessedToken} as the safety net before escalating to
     * {@link #onHistoryLost()}.</p>
     */
    ResumeStrategy resumeStrategy() default ResumeStrategy.PROCESSED_FIRST;
}
