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

import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import io.flowwarden.stream.OnHistoryLost;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.spi.CheckpointStore;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Level 3 of the cascade under {@code RESUME_FROM_OPLOG_START}: the oldest
 * oplog position is validated before the stream is committed to it, and the
 * recovery degrades to a fresh certified position when that position is
 * already gone or when the previous attempt lost the same race.
 */
class ResumeCascadeTest {

    private static final String STREAM = "cascade-test";
    private static final BsonDocument DEAD_PROCESSED = BsonDocument.parse("{\"_data\": \"dead-processed\"}");
    private static final BsonDocument DEAD_SEEN = BsonDocument.parse("{\"_data\": \"dead-seen\"}");
    private static final BsonDocument FRESH = BsonDocument.parse("{\"_data\": \"fresh\"}");
    private static final BsonDocument PBRT = BsonDocument.parse("{\"_data\": \"pbrt\"}");
    private static final BsonTimestamp OLDEST = new BsonTimestamp(1_700_000_000, 395);

    @Checkpoint(onHistoryLost = OnHistoryLost.RESUME_FROM_OPLOG_START)
    private static final class OplogStartStream {
    }

    private final Checkpoint oplogStart = OplogStartStream.class.getAnnotation(Checkpoint.class);
    private final CheckpointStore store = mock(CheckpointStore.class);
    private final HeartbeatProbe probe = mock(HeartbeatProbe.class);
    private final AtomicInteger oplogReads = new AtomicInteger();

    @BeforeEach
    void setUp() {
        Instant past = Instant.now().minusSeconds(86_400);
        when(store.findByStreamName(STREAM)).thenReturn(Optional.of(
                new io.flowwarden.stream.spi.Checkpoint(STREAM, null, DEAD_SEEN, past,
                        DEAD_PROCESSED, past, Map.of())));
        when(probe.initialPosition()).thenReturn(FRESH);
    }

    private ResumeContext resolve(boolean oplogStartAllowed) {
        return ResumeCascade.resolve(STREAM, oplogStart, store, probe,
                token -> false, // both anchors aged out
                () -> {
                    oplogReads.incrementAndGet();
                    return OLDEST;
                },
                oplogStartAllowed);
    }

    private static MongoCommandException historyLost() {
        return new MongoCommandException(
                BsonDocument.parse("{\"ok\": 0.0, \"code\": 286, \"codeName\": \"ChangeStreamHistoryLost\", "
                        + "\"errmsg\": \"Resume of change stream was not possible\"}"),
                new ServerAddress("localhost", 27017));
    }

    @Test
    void oldestEntryStillThere_resumesAtIt_withTheDeadTokensKeptAsMarker() {
        when(probe.probeFromOperationTime(OLDEST)).thenReturn(ProbeOutcome.empty(PBRT));

        ResumeContext ctx = resolve(true);

        assertThat(ctx.seedToken()).isNull();
        assertThat(ctx.initialOperationTime()).as("the full timestamp, increment included").isEqualTo(OLDEST);
        assertThat(ctx.deadProcessedToken()).isEqualTo(DEAD_PROCESSED);
        verify(store, never()).resetAfterHistoryLost(any(), any(), any(), any());
        verify(store, never()).saveSeen(any(), any(), any(), any());
        verify(probe, never()).initialPosition();
    }

    @Test
    void oldestEntryWithEventsPending_isAValidPosition() {
        when(probe.probeFromOperationTime(OLDEST)).thenReturn(ProbeOutcome.eventPending());

        ResumeContext ctx = resolve(true);

        assertThat(ctx.initialOperationTime()).isEqualTo(OLDEST);
    }

    @Test
    void oldestEntryRolledOver_degradesToAFreshCertifiedPosition() {
        when(probe.probeFromOperationTime(OLDEST)).thenReturn(ProbeOutcome.failed(historyLost()));

        ResumeContext ctx = resolve(true);

        assertThat(ctx.seedToken()).isEqualTo(FRESH);
        assertThat(ctx.initialOperationTime()).isNull();
        verify(store).resetAfterHistoryLost(eq(STREAM), eq(FRESH), eq(DEAD_PROCESSED), any());
    }

    @Test
    void transientValidationFailure_leavesTheDecisionToTheOpen() {
        when(probe.probeFromOperationTime(OLDEST))
                .thenReturn(ProbeOutcome.failed(new RuntimeException("socket timeout")));

        ResumeContext ctx = resolve(true);

        assertThat(ctx.initialOperationTime()).isEqualTo(OLDEST);
        verify(probe, never()).initialPosition();
    }

    @Test
    void previousAttemptLostTheRace_degradesWithoutReadingTheOplog() {
        ResumeContext ctx = resolve(false);

        assertThat(ctx.seedToken()).isEqualTo(FRESH);
        assertThat(oplogReads).hasValue(0);
        verify(probe, never()).probeFromOperationTime(any());
        verify(store).resetAfterHistoryLost(eq(STREAM), eq(FRESH), eq(DEAD_PROCESSED), any());
    }

    @Test
    void unreadableOplog_degradesToAFreshCertifiedPosition() {
        ResumeContext ctx = ResumeCascade.resolve(STREAM, oplogStart, store, probe,
                token -> false,
                () -> {
                    throw new IllegalStateException("oplog.rs is empty or not accessible");
                },
                true);

        assertThat(ctx.seedToken()).isEqualTo(FRESH);
        verify(store).resetAfterHistoryLost(eq(STREAM), eq(FRESH), eq(DEAD_PROCESSED), any());
        verify(probe, never()).probeFromOperationTime(any());
    }

    @Test
    void noProcessedAnchor_degradeHasNoDeadPairToPurge() {
        // Only a dead seen position: nothing to guard, the fresh position is
        // simply written over it.
        Instant past = Instant.now().minusSeconds(86_400);
        when(store.findByStreamName(STREAM)).thenReturn(Optional.of(
                new io.flowwarden.stream.spi.Checkpoint(STREAM, null, DEAD_SEEN, past, null, null, Map.of())));
        when(probe.probeFromOperationTime(OLDEST)).thenReturn(ProbeOutcome.failed(historyLost()));

        ResumeContext ctx = resolve(true);

        assertThat(ctx.seedToken()).isEqualTo(FRESH);
        verify(store).saveSeen(eq(STREAM), eq(FRESH), any(), any());
        verify(store, never()).resetAfterHistoryLost(any(), any(), any(), any());
    }
}
