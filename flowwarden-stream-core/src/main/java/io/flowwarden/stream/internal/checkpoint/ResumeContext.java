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
package io.flowwarden.stream.internal.checkpoint;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

/**
 * Immutable outcome of the resume cascade, built from the single checkpoint
 * read the cascade already performed — the heartbeat setup derives everything
 * from it without re-reading the store, so stream startup cannot fail after
 * the cursor has been registered/subscribed (no TOCTOU, no half-started
 * stream).
 *
 * @param seedToken            the position the stream starts from, or
 *                             {@code null} when no token-based position was
 *                             established ({@code LATEST},
 *                             {@code RESUME_FROM_OPLOG_START} recovery)
 * @param persistedSeenToken   the {@code lastSeenToken} persisted at cascade
 *                             time (after a bootstrap, the freshly persisted
 *                             position — equal to the seed by construction)
 * @param initialOperationTime {@code RESUME_FROM_OPLOG_START} recovery only:
 *                             the operation time the stream resumes at. The
 *                             heartbeat uses it as a last-resort chain source
 *                             (never "now" — a probe from this time certifies
 *                             nothing left to replay) until a real position
 *                             exists.
 * @param deadProcessedToken   {@code RESUME_FROM_OPLOG_START} recovery only:
 *                             the expired processed token read at
 *                             history-loss detection — the at-least-once
 *                             guard of the deferred cleanup
 *                             ({@code resetAfterHistoryLost} only removes the
 *                             processed pair while it still equals this
 *                             value)
 */
public record ResumeContext(BsonDocument seedToken,
                            BsonDocument persistedSeenToken,
                            BsonTimestamp initialOperationTime,
                            BsonDocument deadProcessedToken) {

    public static final ResumeContext NONE = new ResumeContext(null, null, null, null);

    public ResumeContext(BsonDocument seedToken, BsonDocument persistedSeenToken) {
        this(seedToken, persistedSeenToken, null, null);
    }

    /**
     * Whether the heartbeat probe may fall back to the persisted
     * {@code lastSeenToken} when no in-memory position exists: only when the
     * cascade actually established a position.
     */
    public boolean allowPersistedFallback() {
        return seedToken != null;
    }

    /**
     * Whether the stream resumes <em>behind</em> a persisted seen position it
     * must not regress: a real divergence requires both a seed and an
     * existing persisted position differing from it. Without a persisted seen
     * there is nothing to protect — resuming from the processed token is not
     * a regression and the flush operates normally.
     */
    public boolean startInCatchUp() {
        return seedToken != null
                && persistedSeenToken != null
                && !seedToken.equals(persistedSeenToken);
    }
}
