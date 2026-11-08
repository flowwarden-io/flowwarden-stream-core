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

/**
 * Strategy to apply when a saved resume token has expired from the MongoDB oplog.
 *
 * <p>Under {@link #RESUME_FROM_NOW} and {@link #RESUME_FROM_OPLOG_START} the
 * events of the lost window are gone with the oplog. A stream that maintains
 * a derived view declares a {@link io.flowwarden.stream.annotation.Reconcile
 * @Reconcile} method: it receives the bound of the lost window and runs
 * before the stream resumes, before any checkpoint write; a throw stops the
 * start like {@link #FAIL} would, with the expired tokens kept so the next
 * start reconciles again.</p>
 *
 * @see io.flowwarden.stream.annotation.Checkpoint#onHistoryLost()
 * @see io.flowwarden.stream.annotation.Reconcile
 */
public enum OnHistoryLost {

    /**
     * Refuse to start — a terminal stop by design, not a retryable state.
     * The stream will keep failing on every start until an operator
     * intervenes: delete the stream's document from the checkpoint
     * collection ({@code _fw_checkpoints} with the shipped Mongo stores) to
     * restart from a fresh position, or switch the strategy. Choose this
     * (the default) when losing events must never go unnoticed.
     */
    FAIL,

    /**
     * Start from the oldest available oplog entry, replaying whatever
     * history is still readable.
     *
     * <p>Inherently racy: the oldest entry is by definition about to fall
     * off the oplog, so on a tight or fast-rolling oplog the resume may
     * still fail with {@code ChangeStreamHistoryLost} shortly after start.
     * If reading the oplog boundary fails, the recovery falls back to the
     * {@link #RESUME_FROM_NOW} behavior (fresh certified position, persisted
     * before start).</p>
     */
    RESUME_FROM_OPLOG_START,

    /**
     * Abandon the lost history explicitly: the recovery captures a fresh
     * server-certified position, persists it as a startup precondition
     * (clearing the expired tokens), and resumes from it. Recommended for
     * rebuildable projections (search models, caches) where a reindex is
     * cheaper than a hard stop.
     */
    RESUME_FROM_NOW
}
