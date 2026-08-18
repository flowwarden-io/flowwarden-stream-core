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

import io.flowwarden.stream.FlowWardenMetrics;
import io.flowwarden.stream.spi.CheckpointStore;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Tick logic of the {@code saveIntervalSeconds} checkpoint heartbeat, shared
 * by both stream managers.
 *
 * <p>Each tick either persists a fresh event token, or — when no event
 * arrived since the last persisted position — runs a {@link HeartbeatProbe}
 * chained from the last delivered position. The probe may only advance
 * {@code lastSeenToken} across an interval the server certified empty; a
 * concurrent event token always wins via compare-and-set on the shared
 * in-memory snapshot.</p>
 *
 * <p>Write contract (three probe outcomes, three write behaviors):</p>
 * <ul>
 *   <li>PBRT different from the chained token → atomic write of position +
 *       heartbeat ({@code saveSeen(name, pbrt, now, now)});</li>
 *   <li>PBRT identical → heartbeat-only write, position untouched;</li>
 *   <li>event pending, timeout, error or null PBRT → no checkpoint write.</li>
 * </ul>
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public final class CheckpointHeartbeat {

    private static final Logger log = LoggerFactory.getLogger(CheckpointHeartbeat.class);

    private final String streamName;
    private final CheckpointStore checkpointStore;
    private final HeartbeatProbe probe;
    private final Supplier<AtomicReference<TokenSnapshot>> latestTokenRef;

    /**
     * Last {@code lastSeenToken} value this heartbeat persisted (event token
     * or probe PBRT). Only read/written from the single-threaded interval
     * scheduler.
     */
    private BsonDocument lastPersistedSeen;

    public CheckpointHeartbeat(String streamName,
                               CheckpointStore checkpointStore,
                               HeartbeatProbe probe,
                               Supplier<AtomicReference<TokenSnapshot>> latestTokenRef) {
        this.streamName = streamName;
        this.checkpointStore = checkpointStore;
        this.probe = probe;
        this.latestTokenRef = latestTokenRef;
    }

    /**
     * Runs one heartbeat tick. Never throws.
     */
    public void tick() {
        try {
            doTick();
        } catch (Exception e) {
            FlowWardenMetrics.get().onCheckpointFailed(streamName, e);
            log.warn("Failed to save periodic checkpoint for stream '{}': {}",
                    streamName, e.getMessage(), e);
        }
    }

    private void doTick() {
        Instant now = Instant.now();
        AtomicReference<TokenSnapshot> ref = latestTokenRef.get();
        TokenSnapshot snapshot = ref.get();

        // Case 1: a fresh event token arrived since the last persisted position.
        if (snapshot != null && !snapshot.token().equals(lastPersistedSeen)) {
            checkpointStore.saveSeen(streamName, snapshot.token(), snapshot.timestamp(), now);
            lastPersistedSeen = snapshot.token();
            FlowWardenMetrics.get().onCheckpoint(streamName, snapshot.token().toJson());
            log.debug("Periodic lastSeenToken update for stream '{}'", streamName);
            return;
        }

        // Case 2: no fresh event — probe, chained from the last delivered
        // position (in-memory snapshot first, persisted lastSeenToken next).
        BsonDocument chainToken = snapshot != null ? snapshot.token() : persistedSeenToken();
        if (chainToken == null) {
            // No position exists to chain from (e.g. StartPosition.LATEST with
            // no event yet). Probing from "now" is unsafe — skip.
            log.debug("Heartbeat skipped for stream '{}': no position to chain from", streamName);
            return;
        }

        ProbeOutcome outcome = probe.probe(chainToken);
        switch (outcome.type()) {
            case EVENT_PENDING ->
                // Undelivered events sit between the chained token and the
                // head — the main stream must deliver them first.
                log.debug("Heartbeat abstained for stream '{}': events pending delivery", streamName);
            case FAILED -> {
                FlowWardenMetrics.get().onHeartbeatProbeFailed(streamName, outcome.cause());
                log.warn("Heartbeat probe failed for stream '{}': {}", streamName,
                        outcome.cause() != null ? outcome.cause().getMessage() : "unknown",
                        outcome.cause());
            }
            case EMPTY -> persistCertifiedPosition(ref, snapshot, chainToken, outcome.pbrt(), now);
        }
    }

    private void persistCertifiedPosition(AtomicReference<TokenSnapshot> ref,
                                          TokenSnapshot snapshot,
                                          BsonDocument chainToken,
                                          BsonDocument pbrt,
                                          Instant now) {
        if (pbrt.equals(chainToken)) {
            // Position unchanged but re-certified valid: heartbeat-only write.
            checkpointStore.saveHeartbeat(streamName, now);
            log.debug("Heartbeat re-certified position for stream '{}'", streamName);
            return;
        }
        // An event delivered while the probe was in flight always wins.
        if (!ref.compareAndSet(snapshot, new TokenSnapshot(pbrt, now))) {
            log.debug("Heartbeat probe result discarded for stream '{}': event token won the race",
                    streamName);
            return;
        }
        checkpointStore.saveSeen(streamName, pbrt, now, now);
        lastPersistedSeen = pbrt;
        FlowWardenMetrics.get().onCheckpoint(streamName, pbrt.toJson());
        log.debug("Periodic lastSeenToken update for stream '{}'", streamName);
    }

    private BsonDocument persistedSeenToken() {
        return checkpointStore.findByStreamName(streamName)
                .map(io.flowwarden.stream.spi.Checkpoint::lastSeenToken)
                .orElse(null);
    }
}
