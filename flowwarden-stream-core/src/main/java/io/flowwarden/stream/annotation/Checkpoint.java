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
 * {@link ChangeStream}. FlowWarden tracks two anchors of different natures
 * for each stream — each with a single writer — and applies a fixed resume
 * cascade on restart.</p>
 *
 * <h2>Dual-anchor model</h2>
 *
 * <ul>
 *   <li><strong>{@code lastProcessedToken}</strong> — the durability anchor of
 *       the <em>work</em>: the token of the last <em>terminally settled</em>
 *       event — handler success, rejection by {@code @Filter}, no matching
 *       handler, or a terminal skip/DLQ decision. An event still being
 *       processed or retried (or whose retry backoff was interrupted by a
 *       shutdown) never advances it, so resuming from this anchor can never
 *       skip a delivery that a crash would need to replay. Note the DLQ
 *       reserve: the terminal <em>decision</em> settles the event even when
 *       the best-effort DLQ write itself fails (signaled via
 *       {@code onEventDlqFailed}) — an event abandoned after exhaustion is
 *       settled, durably dead-lettered or not. Persisted by a count-or-time
 *       policy, whichever threshold is reached first: the
 *       {@link #saveEveryN() counter} bounds the replay in number of events,
 *       the {@link #saveIntervalSeconds() timer} bounds the age of a dirty
 *       anchor in time (a clean anchor is never rewritten).</li>
 *   <li><strong>{@code lastSeenToken}</strong> — the durability anchor of the
 *       <em>position</em>: exclusively a server-certified post-batch resume
 *       token (PBRT) written by the {@link #idleHeartbeatIntervalSeconds()
 *       idle heartbeat}. When the stream stops settling events, a bounded,
 *       ephemeral probe chained from the last settled position obtains a
 *       PBRT certified by the server, so the resume point keeps tracking the
 *       oplog head with zero traffic. A token carried by a delivered event
 *       is never written here — replayed events therefore cannot regress
 *       this anchor by construction.</li>
 * </ul>
 *
 * <h2>3-level resume cascade</h2>
 *
 * <p>On restart with {@link StartPosition#RESUME}, the stream resumes in a
 * fixed order:</p>
 * <ol>
 *   <li>From {@code lastProcessedToken} — preserves strict at-least-once:
 *       everything after the last settled event is re-delivered.</li>
 *   <li>If it is absent or has aged out of the oplog, from
 *       {@code lastSeenToken} — the certified position keeps the stream
 *       recoverable (typical for idle streams whose processed anchor aged
 *       out while the heartbeat kept certifying).</li>
 *   <li>If neither anchor is usable, apply the {@link #onHistoryLost()}
 *       strategy.</li>
 * </ol>
 *
 * <p>Checkpoints written by earlier versions may hold an event token in
 * {@code lastSeenToken}; it remains a valid resume position for the cascade,
 * and the field adopts the PBRT-only semantics at the first successful
 * certification.</p>
 *
 * <h2>Attribute roles</h2>
 *
 * <ul>
 *   <li>{@link #saveEveryN()} = count threshold of the processed-anchor
 *       policy. {@code 1} (default) writes after every settlement.</li>
 *   <li>{@link #saveIntervalSeconds()} = time threshold of the same policy —
 *       the maximum age of a dirty processed anchor.</li>
 *   <li>{@link #idleHeartbeatIntervalSeconds()} = idle certification cadence,
 *       the sole writer of {@code lastSeenToken}.</li>
 *   <li>{@link #startPosition()} = whether to apply the cascade
 *       ({@code RESUME}) or ignore both anchors ({@code LATEST},
 *       bootstrap).</li>
 *   <li>{@link #onHistoryLost()} = strategy at cascade level 3 only.</li>
 * </ul>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Checkpoint {

    /**
     * Count threshold of the processed-anchor policy: persist
     * {@code lastProcessedToken} at every N-th terminally settled event
     * (handler success, {@code @Filter} rejection, no matching handler, or
     * terminal skip/DLQ decision). Must be {@code >= 1}; {@code 1} (default)
     * gives strict at-least-once on crash. Larger values reduce write
     * pressure at the cost of replaying up to {@code N-1} settled events on
     * crash recovery.
     */
    int saveEveryN() default 1;

    /**
     * Time threshold of the processed-anchor policy: the maximum time a
     * <em>dirty</em> {@code lastProcessedToken} waits before being
     * persisted. Complements {@link #saveEveryN()} — the anchor is written
     * at whichever threshold is reached first. A clean anchor is never
     * rewritten: on an active-but-slow stream this bounds the anchor's age
     * in wall-clock time (protecting it against oplog aging that a pure
     * count threshold cannot see), while on a stream that settles nothing it
     * writes nothing at all — keeping an <em>idle</em> stream's resume point
     * alive is the separate responsibility of
     * {@link #idleHeartbeatIntervalSeconds()}.
     *
     * <p>{@code 5} (default) is a balanced value; set to {@code 0} to disable
     * the time threshold ({@code lastProcessedToken} then only advances
     * through the {@link #saveEveryN()} counter).</p>
     */
    int saveIntervalSeconds() default 5;

    /**
     * Idle heartbeat interval in seconds — the oplog-rollover protection
     * policy for streams that stop settling events, and the <strong>sole
     * writer</strong> of {@code lastSeenToken}.
     *
     * <p>When the main cursor has not settled any event for this long, the
     * heartbeat opens an ephemeral, bounded change stream probe chained from
     * the last settled position (same collection, same resolved
     * {@code @Pipeline} stages and {@code fullDocument} options as the
     * stream, stamped with a {@code flowwarden:heartbeat:&lt;stream&gt;}
     * comment). When the server certifies the interval empty, the returned
     * post-batch resume token is persisted as {@code lastSeenToken}: the
     * resume point keeps tracking the oplog head with zero traffic — the
     * stream stays recoverable as long as probes keep succeeding within the
     * oplog window. If events sit undelivered, the probe abstains and writes
     * nothing: a certified position never crosses unsettled work. Any
     * activity of the main cursor re-arms the idle delay; active streams
     * never probe (their recoverability rides on the processed anchor).</p>
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
     * a recoverable position was confirmed (processed-anchor write or
     * successful empty probe); its age is the operational signal for
     * resume-point health.</p>
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
     * {@link StartPosition#LATEST} ignores both persisted anchors and starts from now.
     */
    StartPosition startPosition() default StartPosition.RESUME;

    /**
     * Strategy when <strong>both</strong> persisted anchors have aged out of the
     * MongoDB oplog (cascade level 3). With the dual-anchor cascade in place,
     * reaching this level signals a genuine anomaly (downtime longer than the
     * oplog window combined with the heartbeat not running).
     */
    OnHistoryLost onHistoryLost() default OnHistoryLost.FAIL;
}
