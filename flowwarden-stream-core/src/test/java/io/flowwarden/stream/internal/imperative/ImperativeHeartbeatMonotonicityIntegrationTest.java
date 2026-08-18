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
package io.flowwarden.stream.internal.imperative;

import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.flowwarden.stream.ChangeStreamContext;
import io.flowwarden.stream.StartPosition;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Regression tests from the PR #61 review.
 *
 * <p>First: a {@code PROCESSED_FIRST} resume seeds the heartbeat with the
 * older processed token; neither that seed nor the replayed events may ever
 * regress the persisted {@code lastSeenToken} below its high-water mark.</p>
 *
 * <p>Second: {@code StartPosition.LATEST} ignores persisted tokens — the
 * heartbeat must not chain from an old checkpoint (probing unconsumed history
 * would strand it in permanent abstention) and must leave the stale document
 * untouched until a live event re-seeds the chain.</p>
 *
 * <p>Third: the bootstrap hand-off property — a PBRT captured from a change
 * stream's initial reply is a safe {@code resumeAfter} position: an event
 * committed after the capture (during the hand-off window) is delivered.</p>
 */
@SpringBootTest(classes = ImperativeHeartbeatMonotonicityIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeHeartbeatMonotonicityIntegrationTest {

    private static final String RESUME_STREAM = "hb-monotonic";
    private static final String RESUME_COLLECTION = "hb_monotonic";
    private static final String LATEST_STREAM = "hb-latest";
    private static final String LATEST_COLLECTION = "hb_latest";
    private static final String CATCHUP_OPTOUT_STREAM = "hb-catchup-optout";
    private static final String CATCHUP_OPTOUT_COLLECTION = "hb_catchup_optout";

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired MongoTemplate mongoTemplate;
    @Autowired ImperativeStreamManager streamManager;
    @Autowired CheckpointStore checkpointStore;
    @Autowired MonotonicHandler monotonicHandler;
    @Autowired LatestHandler latestHandler;
    @Autowired CatchUpOptOutHandler catchUpOptOutHandler;

    @BeforeEach
    void setUp() {
        monotonicHandler.clear();
        latestHandler.clear();
        catchUpOptOutHandler.clear();
        mongoTemplate.dropCollection(RESUME_COLLECTION);
        mongoTemplate.dropCollection(LATEST_COLLECTION);
        mongoTemplate.dropCollection(CATCHUP_OPTOUT_COLLECTION);
        checkpointStore.delete(RESUME_STREAM);
        checkpointStore.delete(LATEST_STREAM);
        checkpointStore.delete(CATCHUP_OPTOUT_STREAM);
    }

    @AfterEach
    void tearDown() {
        try { streamManager.stopStream(RESUME_STREAM); } catch (Exception ignored) {}
        try { streamManager.stopStream(LATEST_STREAM); } catch (Exception ignored) {}
        try { streamManager.stopStream(CATCHUP_OPTOUT_STREAM); } catch (Exception ignored) {}
        checkpointStore.delete(RESUME_STREAM);
        checkpointStore.delete(LATEST_STREAM);
        checkpointStore.delete(CATCHUP_OPTOUT_STREAM);
    }

    @Test
    void processedFirstResume_neverRegressesSeenBelowHighWaterMark() throws Exception {
        // Build a real divergent checkpoint: processed = position BEFORE an
        // event, seen = position AFTER it. Resuming from processed replays it.
        BsonDocument processedPos = currentPosition(RESUME_COLLECTION);
        mongoTemplate.insert(new Document("tag", "replayed"), RESUME_COLLECTION);
        BsonDocument seenPos = currentPosition(RESUME_COLLECTION);
        assertThat(dataOf(seenPos)).isGreaterThan(dataOf(processedPos));

        Instant now = Instant.now();
        checkpointStore.save(new io.flowwarden.stream.spi.Checkpoint(
                RESUME_STREAM, null, seenPos, now, processedPos, now, now,
                Collections.emptyMap()));

        streamManager.startStream(RESUME_STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(RESUME_STREAM));

        // The replay happens (at-least-once from processed)...
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(monotonicHandler.tags()).contains("replayed"));

        // ...and at NO point (seed tick, replayed-event tick, probe tick) may
        // the persisted seen drop below its high-water mark.
        for (int i = 0; i < 8; i++) {
            var cp = checkpointStore.findByStreamName(RESUME_STREAM).orElseThrow();
            assertThat(dataOf(cp.lastSeenToken()))
                    .as("lastSeenToken must never regress below the pre-restart mark")
                    .isGreaterThanOrEqualTo(dataOf(seenPos));
            Thread.sleep(500);
        }
    }

    @Test
    void latestStream_ignoresStaleCheckpoint_untilALiveEventReseedsTheChain() {
        // Stale checkpoint + unconsumed history on the collection.
        BsonDocument oldPos = currentPosition(LATEST_COLLECTION);
        Instant past = Instant.now().minusSeconds(3600);
        checkpointStore.save(new io.flowwarden.stream.spi.Checkpoint(
                LATEST_STREAM, null, oldPos, past, null, null, null,
                Collections.emptyMap()));
        mongoTemplate.insert(new Document("tag", "history-1"), LATEST_COLLECTION);
        mongoTemplate.insert(new Document("tag", "history-2"), LATEST_COLLECTION);

        streamManager.startStream(LATEST_STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(LATEST_STREAM));

        // Several ticks: the heartbeat must not touch the stale document nor
        // deliver the unconsumed history.
        await().atMost(Duration.ofSeconds(6)).pollDelay(Duration.ofSeconds(3))
                .untilAsserted(() -> {
                    var cp = checkpointStore.findByStreamName(LATEST_STREAM).orElseThrow();
                    assertThat(cp.lastSeenToken()).isEqualTo(oldPos);
                    assertThat(cp.lastHeartbeatTimestamp())
                            .as("LATEST must not heartbeat from a checkpoint it ignores")
                            .isNull();
                    assertThat(latestHandler.tags()).isEmpty();
                });

        // A live event re-seeds the chain: delivery + fresh checkpoint.
        Instant beforeLive = Instant.now();
        mongoTemplate.insert(new Document("tag", "live"), LATEST_COLLECTION);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(latestHandler.tags()).containsExactly("live");
            var cp = checkpointStore.findByStreamName(LATEST_STREAM).orElseThrow();
            assertThat(dataOf(cp.lastSeenToken())).isGreaterThan(dataOf(oldPos));
            assertThat(cp.lastHeartbeatTimestamp()).isAfterOrEqualTo(beforeLive);
        });
    }

    @Test
    void initialReplyPbrt_isASafeResumePosition_eventDuringHandOffIsDelivered() {
        // The bootstrap property, pinned directly: capture the PBRT of a
        // cursor's INITIAL reply, close the cursor, commit an event, then
        // resume a second cursor from the captured position — the event is in
        // its range.
        BsonDocument pbrt0 = currentPosition(RESUME_COLLECTION);

        mongoTemplate.insert(new Document("tag", "hand-off"), RESUME_COLLECTION);

        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor =
                     mongoTemplate.getCollection(RESUME_COLLECTION).watch()
                             .resumeAfter(pbrt0)
                             .maxAwaitTime(2, TimeUnit.SECONDS)
                             .cursor()) {
            ChangeStreamDocument<Document> event = cursor.tryNext();
            assertThat(event).as("the event committed after the capture must be delivered").isNotNull();
            assertThat(event.getFullDocument().getString("tag")).isEqualTo("hand-off");
        }
    }

    @Test
    void divergenceWithIdleProbingOptedOut_catchUpStillCompletes_flushKeepsWorking() {
        // The catch-up correction must run even with idleHeartbeatIntervalSeconds=0:
        // opting out of idle probing must never permanently disable the flush
        // (PR-review blocking point on the catch-up/idle coupling).
        BsonDocument processedPos = currentPosition(CATCHUP_OPTOUT_COLLECTION);
        mongoTemplate.insert(new Document("tag", "replayed"), CATCHUP_OPTOUT_COLLECTION);
        BsonDocument seenPos = currentPosition(CATCHUP_OPTOUT_COLLECTION);

        Instant savedAt = Instant.now();
        checkpointStore.save(new io.flowwarden.stream.spi.Checkpoint(
                CATCHUP_OPTOUT_STREAM, null, seenPos, savedAt, processedPos, savedAt, savedAt,
                Collections.emptyMap()));

        streamManager.startStream(CATCHUP_OPTOUT_STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(CATCHUP_OPTOUT_STREAM));

        // The replay is delivered, and the transient catch-up chain (immediate
        // attempt + 5s retries) certifies completion despite idle probing
        // being opted out — observable through the heartbeat timestamp (the
        // certified PBRT may be byte-identical to seenPos on a quiet cluster,
        // so the token itself is not a reliable certification signal).
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(catchUpOptOutHandler.tags()).contains("replayed");
            assertThat(checkpointStore.findByStreamName(CATCHUP_OPTOUT_STREAM)
                    .orElseThrow().lastHeartbeatTimestamp()).isAfter(savedAt);
        });
        BsonDocument certifiedToken = checkpointStore
                .findByStreamName(CATCHUP_OPTOUT_STREAM).orElseThrow().lastSeenToken();

        // Only THEN insert the live event: with idle=0 and the catch-up chain
        // finished, the flush is the only mechanism able to move the position
        // again — this pins that the flush actually resumed (the previous
        // assertion was satisfied by the certification PBRT alone).
        mongoTemplate.insert(new Document("tag", "live"), CATCHUP_OPTOUT_COLLECTION);
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(catchUpOptOutHandler.tags()).contains("live");
            assertThat(checkpointStore.findByStreamName(CATCHUP_OPTOUT_STREAM)
                    .orElseThrow().lastSeenToken())
                    .as("the flush must persist events again once catch-up completed")
                    .isNotEqualTo(certifiedToken);
        });
    }

    /**
     * Captures the server's current position for a collection from a change
     * stream's <em>initial</em> aggregate reply — the exact mechanism the
     * bootstrap uses (the sync cursor API only caches the PBRT after an
     * iteration, so the reply is read via a raw command).
     */
    private BsonDocument currentPosition(String collection) {
        Document reply = mongoTemplate.executeCommand(new Document("aggregate", collection)
                .append("pipeline", List.of(new Document("$changeStream", new Document())))
                .append("cursor", new Document("batchSize", 1)));
        Document cursor = reply.get("cursor", Document.class);
        long cursorId = ((Number) cursor.get("id")).longValue();
        Document pbrt = cursor.get("postBatchResumeToken", Document.class);
        if (cursorId != 0) {
            mongoTemplate.executeCommand(new Document("killCursors", collection)
                    .append("cursors", List.of(cursorId)));
        }
        assertThat(pbrt).isNotNull();
        return BsonDocument.parse(pbrt.toJson());
    }

    private static String dataOf(BsonDocument token) {
        return token.getString("_data").getValue();
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import({MonotonicHandler.class, LatestHandler.class, CatchUpOptOutHandler.class})
    static class TestApp {}

    @ChangeStream(name = CATCHUP_OPTOUT_STREAM, collection = CATCHUP_OPTOUT_COLLECTION,
            documentType = Document.class, autoStart = false)
    @Checkpoint(saveEveryN = 100, saveIntervalSeconds = 1, idleHeartbeatIntervalSeconds = 0)
    static class CatchUpOptOutHandler {

        private final List<Document> events = new CopyOnWriteArrayList<>();

        @OnInsert
        void handle(ChangeStreamContext<Document> ctx) {
            ctx.getFullDocument(Document.class).ifPresent(events::add);
        }

        List<String> tags() {
            return events.stream().map(d -> d.getString("tag")).toList();
        }

        void clear() { events.clear(); }
    }

    @ChangeStream(name = RESUME_STREAM, collection = RESUME_COLLECTION,
            documentType = Document.class, autoStart = false)
    @Checkpoint(saveEveryN = 100, saveIntervalSeconds = 1, idleHeartbeatIntervalSeconds = 1)
    static class MonotonicHandler {

        private final List<Document> events = new CopyOnWriteArrayList<>();

        @OnInsert
        void handle(ChangeStreamContext<Document> ctx) {
            ctx.getFullDocument(Document.class).ifPresent(events::add);
        }

        List<String> tags() {
            return events.stream().map(d -> d.getString("tag")).toList();
        }

        void clear() { events.clear(); }
    }

    @ChangeStream(name = LATEST_STREAM, collection = LATEST_COLLECTION,
            documentType = Document.class, autoStart = false)
    @Checkpoint(saveEveryN = 100, saveIntervalSeconds = 1, idleHeartbeatIntervalSeconds = 1,
            startPosition = StartPosition.LATEST)
    static class LatestHandler {

        private final List<Document> events = new CopyOnWriteArrayList<>();

        @OnInsert
        void handle(ChangeStreamContext<Document> ctx) {
            ctx.getFullDocument(Document.class).ifPresent(events::add);
        }

        List<String> tags() {
            return events.stream().map(d -> d.getString("tag")).toList();
        }

        void clear() { events.clear(); }
    }
}
