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
 * @see io.flowwarden.stream.annotation.Checkpoint#onHistoryLost()
 */
public enum OnHistoryLost {

    /** Refuse to start — operator must intervene. This is the default. */
    FAIL,

    /** Start from the oldest available oplog entry. */
    RESUME_FROM_OPLOG_START,

    /** Start from the current moment, skipping all past events. */
    RESUME_FROM_NOW
}
