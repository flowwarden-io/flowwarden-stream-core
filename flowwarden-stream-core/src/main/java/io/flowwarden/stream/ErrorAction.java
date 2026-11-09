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
 * Actions that an {@code @OnError} handler can return to control error processing.
 *
 * <ul>
 *   <li>{@link #SKIP} — Ignore the event (log warning). No retry, no DLQ.</li>
 *   <li>{@link #RETRY} — Retry the event, bounded by {@code @RetryPolicy.maxAttempts}. Without a
 *       {@code @RetryPolicy} on the stream no retry happens and the event follows the DLQ path.</li>
 *   <li>{@link #DLQ} — Send directly to DLQ, bypassing remaining retries.</li>
 *   <li>{@link #RETHROW} — Let FlowWarden handle with the standard policy (retry → DLQ).</li>
 * </ul>
 */
public enum ErrorAction {
    /** Ignore the event (log warning). No retry, no DLQ. */
    SKIP,
    /**
     * Retry the event, bounded by {@code @RetryPolicy.maxAttempts}. Without a
     * {@code @RetryPolicy} on the stream no retry happens and the event follows
     * the DLQ path, exactly like {@link #DLQ}.
     */
    RETRY,
    /** Send directly to DLQ, bypassing remaining retries. */
    DLQ,
    /** Let FlowWarden handle with the standard policy (retry → DLQ). */
    RETHROW
}
