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
 *       with no matching handler, and events being retried), and — when no event
 *       arrives — through the {@link #saveIntervalSeconds() heartbeat}: a bounded,
 *       ephemeral change stream probe chained from the last delivered position
 *       fetches a server-certified post-batch resume token, so the persisted
 *       position keeps tracking the oplog head with zero traffic. This keeps the
 *       stream recoverable on idle or massively-filtered workloads.</li>
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
     * Heartbeat interval in seconds for persisting {@code lastSeenToken}.
     *
     * <p>Each tick persists the most recent event received (including filtered
     * or no-handler events). When no event arrived since the previous tick,
     * the heartbeat instead opens an ephemeral, bounded change stream cursor
     * chained from the last delivered position (same pipeline and
     * {@code fullDocument} options as the stream, stamped with a
     * {@code flowwarden:heartbeat:&lt;stream&gt;} comment) and — when the
     * server certifies the interval empty — advances {@code lastSeenToken} to
     * the returned post-batch resume token. Idle streams therefore stay
     * recoverable indefinitely: the persisted position tracks the oplog head
     * instead of freezing until it ages out. This is the level-2 safety net of
     * the resume cascade.</p>
     *
     * <p>The checkpoint's {@code lastHeartbeatTimestamp} records the last time
     * a recoverable position was confirmed; its age is the operational signal
     * for resume-point health.</p>
     *
     * <p>{@code 5} (default) is a balanced value; set to {@code 0} to disable
     * the heartbeat (disables cascade level 2 — the stream is then nude against
     * oplog rollover on idle / massively-filtered workloads).</p>
     */
    int saveIntervalSeconds() default 5;

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
