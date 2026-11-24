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
package io.flowwarden.stream.core;

import java.time.Instant;
import java.util.Set;

/**
 * Manages the lifecycle of FlowWarden Change Streams.
 */
public interface FlowWardenStreamManager {

    /**
     * Returns the names of the registered streams, whether they are running or
     * not: the names accepted by {@link #startStream(String)},
     * {@link #stopStream(String)}, {@link #isRunning(String)} and
     * {@link #getLastEventTime(String)}.
     *
     * <p>The result is an unmodifiable snapshot in registration order: it is
     * not updated afterwards, and a stream that is stopped, crashed, or not
     * auto-started (including under {@code @EnableFlowWarden(enabled = false)})
     * is still listed.</p>
     *
     * <p><strong>When the list is complete.</strong> Streams are registered
     * while the application context starts, not when they start consuming:
     * a {@code @ChangeStream} class when its bean is initialized, a stream
     * contributed by a {@code StreamDefinitionContributor} once all singletons
     * are instantiated. The list is therefore complete from the end of the
     * context refresh on — in a {@code ContextRefreshedEvent} or
     * {@code ApplicationStartedEvent} listener, a {@code SmartLifecycle},
     * an {@code ApplicationRunner} or {@code CommandLineRunner} — all of which
     * run <em>before</em> {@code ApplicationReadyEvent}, when the streams
     * open. Earlier than that (a constructor, {@code @PostConstruct}, or
     * another {@code SmartInitializingSingleton}) it may be partial: only the
     * streams whose beans are already initialized are returned. Stream
     * registration is bootstrap-only, so the list does not change once
     * complete.</p>
     *
     * <p>The default implementation returns an empty set; the managers
     * provided by FlowWarden override it.</p>
     *
     * @return the registered stream names, never {@code null}
     */
    default Set<String> getStreamNames() {
        return Set.of();
    }

    /**
     * Starts listening on the given stream.
     *
     * @param streamName unique stream identifier
     */
    void startStream(String streamName);

    /**
     * Stops listening on the given stream.
     *
     * @param streamName unique stream identifier
     */
    void stopStream(String streamName);

    /**
     * Returns whether the given stream is currently running.
     *
     * @param streamName unique stream identifier
     */
    boolean isRunning(String streamName);

    /**
     * Returns the time of the last event that reached a handler of the given
     * stream, or {@code null} if none has yet. In imperative mode the time is
     * recorded whatever the handler outcome; in reactive mode only after a
     * successful handler. Events rejected by {@code @Filter} or without a
     * matching handler do not update it.
     *
     * @param streamName unique stream identifier
     */
    default Instant getLastEventTime(String streamName) {
        return null;
    }
}
