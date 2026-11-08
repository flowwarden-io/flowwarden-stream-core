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
package io.flowwarden.stream;

import java.time.Instant;
import java.util.Optional;

/**
 * What a stream knows about the history it lost, handed to the
 * {@link io.flowwarden.stream.annotation.Reconcile @Reconcile} hook before
 * the stream resumes after a history loss.
 *
 * <p>MongoDB has discarded the events of the lost window: no strategy can
 * replay them. The library knows the <em>bound</em> of that window — from
 * the last activity it recorded in the checkpoint to the position the stream
 * is about to resume from — and nothing about its content: whether the
 * watched collection changed inside it is unknowable from the oplog. The
 * bound is therefore <strong>as wide as possible</strong>, never too narrow.</p>
 *
 * <p>Everything after {@link #lostWindowEnd()} is delivered by the stream
 * itself once the hook has returned, at-least-once as usual. A reconciliation
 * that covers up to and including {@code lostWindowEnd()} is complete; the
 * overlap with the first redelivered events is the ordinary at-least-once
 * duplicate the handlers must already tolerate.</p>
 *
 * <p>Only timestamps are exposed, no oplog positions: an application
 * reconciles its own collections by time, not by resume token.</p>
 *
 * @see io.flowwarden.stream.annotation.Reconcile
 * @see io.flowwarden.stream.core.ReconcileHandler
 * @see OnHistoryLost
 */
public interface HistoryLostContext {

    /**
     * The stream being resumed.
     */
    String streamName();

    /**
     * The strategy the stream resumes under — {@link OnHistoryLost#RESUME_FROM_NOW}
     * or {@link OnHistoryLost#RESUME_FROM_OPLOG_START}, never
     * {@link OnHistoryLost#FAIL} (which refuses to start and calls no hook).
     */
    OnHistoryLost strategy();

    /**
     * When the last event was settled by a handler, as recorded in the
     * checkpoint; empty if never recorded.
     */
    Optional<Instant> lastProcessedTimestamp();

    /**
     * When the idle heartbeat last certified a server position, as recorded
     * in the checkpoint; empty if never recorded.
     */
    Optional<Instant> lastSeenTimestamp();

    /**
     * When the idle heartbeat last confirmed the stream alive, as recorded in
     * the checkpoint; empty if never recorded.
     */
    Optional<Instant> lastHeartbeatTimestamp();

    /**
     * The start of the lost window: the most recent of the three checkpoint
     * timestamps — the last moment the library can vouch for. Empty only for
     * a checkpoint that predates timestamp recording; reconcile from the
     * beginning of time in that case.
     */
    Optional<Instant> lostWindowStart();

    /**
     * The end of the lost window: where the stream resumes. Under
     * {@link OnHistoryLost#RESUME_FROM_OPLOG_START}, the oldest surviving
     * oplog entry — everything after it is replayed by the stream. Under
     * {@link OnHistoryLost#RESUME_FROM_NOW}, the moment the fresh position
     * was captured — nothing before it is replayed.
     */
    Instant lostWindowEnd();

    /**
     * {@code 1} on the first invocation for a start attempt. {@code 2} when
     * the position resolved before the first invocation rolled out of the
     * oplog while the hook was running (the reconciliation outlived the oplog
     * window): the second invocation is bounded to the small residual window
     * from the previous {@link #lostWindowEnd()} to the new one. There is
     * never a third: if that window rolls out as well, the start fails with
     * {@link ReconciliationFailedException}.
     */
    int attempt();
}
