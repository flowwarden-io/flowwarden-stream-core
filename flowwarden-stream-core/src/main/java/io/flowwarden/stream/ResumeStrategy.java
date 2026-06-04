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
 * Order in which the two persisted resume tokens are tried at restart.
 *
 * <p>FlowWarden tracks two tokens for every checkpointed stream: {@code lastProcessedToken}
 * (advanced after handler success) and {@code lastSeenToken} (advanced by the heartbeat
 * timer on every event received from MongoDB). The resume cascade always falls back from
 * the chosen primary to the secondary, then to the {@link OnHistoryLost} strategy if both
 * are aged out of the oplog. This enum chooses which token is the <em>primary</em>.</p>
 *
 * <h2>Trade-off</h2>
 *
 * <ul>
 *   <li>{@link #PROCESSED_FIRST} (default) — strict at-least-once: a handler that was
 *       in flight at crash time is re-delivered, because resume starts from the last
 *       acknowledged event. Cost: on low-volume or filter-heavy streams,
 *       {@code lastProcessedToken} can be far behind the oplog head and MongoDB must
 *       scan a long span to catch up.</li>
 *   <li>{@link #SEEN_FIRST} — fast restart: resume starts from the heartbeat-fresh
 *       {@code lastSeenToken}, so MongoDB scans only the events since the last timer
 *       tick. Cost: events in flight at crash time (handler not yet acknowledged) may
 *       be skipped. The cascade still falls back to {@code lastProcessedToken} before
 *       escalating to {@code onHistoryLost}, so the safety net of the dual-token model
 *       is preserved.</li>
 * </ul>
 *
 * @see io.flowwarden.stream.annotation.Checkpoint#resumeStrategy()
 */
public enum ResumeStrategy {

    /**
     * Try {@code lastProcessedToken} first, then fall back to {@code lastSeenToken},
     * then to {@link OnHistoryLost}. Default — preserves strict at-least-once delivery.
     */
    PROCESSED_FIRST,

    /**
     * Try {@code lastSeenToken} first, then fall back to {@code lastProcessedToken},
     * then to {@link OnHistoryLost}. Optimized for restart speed on low-volume or
     * filter-heavy streams; may skip events that were in flight at crash time.
     */
    SEEN_FIRST
}
