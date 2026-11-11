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
package io.flowwarden.stream.spi;

import java.util.Objects;

/**
 * What the runtime knows about a DLQ-enabled stream at startup, handed to
 * {@link DlqStore#onStreamRegistered(DlqStreamRegistration)} once per stream
 * before any event can reach the store.
 *
 * <p>Carries the cross-cutting {@link DlqPolicy} plus the hints the core's
 * own MongoDB backend needs to provision its per-stream resources — the
 * collection declared through {@code @MongoDlqOptions} (or
 * {@code MongoDlqOptionsSpec} for a contributed stream). A backend that does
 * not understand a hint ignores it; a backend with its own companion
 * annotation keeps resolving it from the stream bean as before. The record
 * may gain accessors in later versions; it never loses one.</p>
 *
 * @param streamName      unique stream identifier
 * @param policy          cross-cutting DLQ policy of the stream (never
 *                        {@code null})
 * @param mongoCollection MongoDB collection requested for this stream's
 *                        entries, or the empty string when the stream
 *                        declared none and the backend default applies
 *                        (never {@code null})
 */
public record DlqStreamRegistration(
        String streamName,
        DlqPolicy policy,
        String mongoCollection) {

    public DlqStreamRegistration {
        Objects.requireNonNull(streamName, "streamName");
        Objects.requireNonNull(policy, "policy");
        mongoCollection = mongoCollection == null ? "" : mongoCollection;
    }

    /**
     * Whether the stream asked for a specific MongoDB collection.
     *
     * @return {@code true} when {@link #mongoCollection()} is non-empty
     */
    public boolean hasMongoCollection() {
        return !mongoCollection.isEmpty();
    }
}
