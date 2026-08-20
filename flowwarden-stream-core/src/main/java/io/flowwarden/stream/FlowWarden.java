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
 * FlowWarden Stream Core marker class.
 */
public final class FlowWarden {

    /**
     * Prefix of the {@code comment} stamped on every MongoDB change stream
     * cursor opened by FlowWarden, for attribution in {@code $currentOp},
     * server logs and the profiler. (Ordinary query cursors — checkpoint,
     * DLQ, lock reads — are not stamped.) A stream's main change stream cursor carries
     * {@code flowwarden:<streamName>}; heartbeat probe cursors carry
     * {@code flowwarden:heartbeat:<streamName>}; the ephemeral resume-token
     * validation cursors opened during the startup/restart resume cascade
     * carry {@code flowwarden:resume-validation:<streamName>}. Downstream
     * tooling can
     * match on this prefix to tell FlowWarden cursors apart from any other
     * change stream consumer on the deployment.
     */
    public static final String CURSOR_COMMENT_PREFIX = "flowwarden:";

    private FlowWarden() {
    }
}
