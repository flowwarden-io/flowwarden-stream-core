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

import io.flowwarden.stream.ChangeStreamContext;
import io.flowwarden.stream.FlowWardenMetrics;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnChange;
import io.flowwarden.stream.annotation.Pipeline;
import io.flowwarden.stream.internal.checkpoint.ProbeOutcome;
import io.flowwarden.stream.internal.discovery.ChangeStreamDefinition;
import io.flowwarden.stream.internal.discovery.StreamRegistry;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.StreamMetricsProvider;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Characterization test: PBRT advancement across a LONG fully-filtered
 * backlog. On the supported MongoDB versions, a change stream cursor whose
 * pipeline rejects every event still advances its post-batch resume token
 * as the server scans through the backlog — empty batches carry progress.
 * The heartbeat probe relies on that behavior; this test pins it against
 * the container image so a future image bump with different PBRT semantics
 * fails visibly here first.
 *
 * <p>Per the deferred spec: probe timeouts are failures and NO partial
 * progress is asserted. The primary oracle is ONE direct
 * {@link ImperativeHeartbeatProbe} call with the stream's exact pipeline,
 * resumed from the pre-backlog position, with the managed stream stopped:
 * {@code EMPTY} is the server certifying the interval it actually
 * traversed — from the pre-backlog position to the returned PBRT — free
 * of matching events, and a {@code FAILED} (timeout or error) fails that
 * single call, so no retry or later probe can mask it. The contract does
 * NOT promise the returned PBRT sits past the whole backlog (a bounded
 * {@code getMore} may return an empty batch with partial progression);
 * what is asserted, and what the pinned image exhibits, is exactly: one
 * probe, {@code EMPTY} without timing out, PBRT advanced. No resume-token order is compared (not a
 * client contract) and no cursor is resumed with a different pipeline
 * (MongoDB requires resuming with the pipeline and options that produced
 * the token). A recording metrics provider additionally asserts zero
 * {@code onHeartbeatProbeFailed} over the managed portions of the run,
 * startup diagnostic probe included.</p>
 */
@SpringBootTest(classes = ImperativeHeartbeatFilteredBacklogIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeHeartbeatFilteredBacklogIntegrationTest {

    private static final String STREAM_NAME = "hb-filtered-backlog";
    private static final String COLLECTION = "hb_filtered_backlog";
    private static final int BACKLOG_SIZE = 2_000;

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired MongoTemplate mongoTemplate;
    @Autowired ImperativeStreamManager streamManager;
    @Autowired CheckpointStore checkpointStore;
    @Autowired StreamRegistry streamRegistry;
    @Autowired BacklogHandler handler;

    private final RecordingMetrics metrics = new RecordingMetrics();

    @BeforeEach
    void setUp() {
        handler.clear();
        FlowWardenMetrics.setProvider(metrics);
        mongoTemplate.dropCollection(COLLECTION);
        checkpointStore.delete(STREAM_NAME);
        streamManager.startStream(STREAM_NAME);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(STREAM_NAME));
    }

    @AfterEach
    void tearDown() {
        try { streamManager.stopStream(STREAM_NAME); } catch (Exception ignored) {}
        checkpointStore.delete(STREAM_NAME);
        FlowWardenMetrics.setProvider(StreamMetricsProvider.noOp());
    }

    @Test
    void longFullyFilteredBacklog_singleProbeReturnsEmptyAndPbrtAdvances_nextMatchingWriteDelivered() {
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(checkpointStore.findByStreamName(STREAM_NAME)
                        .orElseThrow().lastSeenToken()).isNotNull());
        BsonDocument initialToken = checkpointStore.findByStreamName(STREAM_NAME)
                .orElseThrow().lastSeenToken();

        // A long backlog the server-side $match rejects entirely. Bulk
        // batches keep the write phase short; every one of these events must
        // be scanned by the probe's cursor without any being delivered.
        List<Document> batch = new ArrayList<>();
        for (int i = 0; i < BACKLOG_SIZE; i++) {
            batch.add(new Document("status", "inactive").append("seq", i));
            if (batch.size() == 500) {
                mongoTemplate.insert(batch, COLLECTION);
                batch = new ArrayList<>();
            }
        }

        // Secondary: the MANAGED probes also progress the checkpoint over
        // the backlog (persistence mechanics — the server oracle is below).
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var cp = checkpointStore.findByStreamName(STREAM_NAME).orElseThrow();
            assertThat(cp.lastSeenToken())
                    .as("the managed probes must make progress over a %s-event filtered backlog"
                            .formatted(BACKLOG_SIZE))
                    .isNotEqualTo(initialToken);
        });
        assertThat(handler.count()).isZero();

        // No race with the managed probes from here on.
        streamManager.stopStream(STREAM_NAME);

        // PRIMARY ORACLE — one probe, exact pipeline. EMPTY certifies the
        // interval the server actually traversed (pre-backlog position →
        // returned PBRT) free of matching events; EVENT_PENDING would mean
        // a missed matching write; FAILED (timeout or error) fails this
        // single call right here.
        ChangeStreamDefinition def = streamRegistry.findByName(STREAM_NAME).orElseThrow();
        ImperativeHeartbeatProbe directProbe =
                new ImperativeHeartbeatProbe(mongoTemplate, def, handler.pipeline());
        ProbeOutcome outcome = directProbe.probe(initialToken);
        assertThat(outcome.type())
                .as(("a single probe over a %s-event filtered backlog must return EMPTY "
                        + "without timing out, got: %s").formatted(BACKLOG_SIZE, outcome))
                .isEqualTo(ProbeOutcome.Type.EMPTY);
        assertThat(outcome.pbrt())
                .as("the certifying PBRT must have advanced from the pre-backlog position")
                .isNotNull()
                .isNotEqualTo(initialToken);

        // Delivery from the MANAGED checkpoint (secondary): the direct
        // probe's PBRT is never persisted — the restart resumes from the
        // checkpoint the managed probes wrote. The next matching write is
        // the FIRST and ONLY delivery.
        mongoTemplate.insert(new Document("status", "active")
                .append("tag", "after-backlog"), COLLECTION);
        streamManager.startStream(STREAM_NAME);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(STREAM_NAME));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(handler.tags()).contains("after-backlog"));
        assertThat(handler.count()).isEqualTo(1);

        // Characterization, not retry-convergence: no probe may have failed
        // or timed out anywhere in the run for the certification to count.
        assertThat(metrics.probeFailures)
                .as("probe timeouts/failures are test failures per the spec")
                .isEmpty();
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import(BacklogHandler.class)
    static class TestApp {}

    @ChangeStream(name = STREAM_NAME, collection = COLLECTION,
            documentType = Document.class, autoStart = false)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1, idleHeartbeatIntervalSeconds = 1)
    static class BacklogHandler {

        private final List<Document> events = new CopyOnWriteArrayList<>();

        @Pipeline
        List<Document> pipeline() {
            return List.of(new Document("$match",
                    new Document("fullDocument.status", "active")));
        }

        @OnChange
        void handle(ChangeStreamContext<Document> ctx) {
            ctx.getFullDocument(Document.class).ifPresent(events::add);
        }

        int count() { return events.size(); }

        List<String> tags() {
            return events.stream().map(d -> d.getString("tag")).toList();
        }

        void clear() { events.clear(); }
    }

    /** Records probe failures; every other callback is irrelevant here. */
    static final class RecordingMetrics implements StreamMetricsProvider {
        final List<Throwable> probeFailures = new CopyOnWriteArrayList<>();

        @Override
        public void onHeartbeatProbeFailed(String streamName, Throwable cause) {
            probeFailures.add(cause);
        }

        @Override
        public void onStreamStarted(String streamName,
                io.flowwarden.stream.spi.StreamConfiguration config) {
        }

        @Override
        public void onEventReceived(String streamName,
                io.flowwarden.stream.spi.ChangeEventMetadata metadata) {
        }

        @Override
        public void onEventProcessed(String streamName, long durationNanos, boolean success) {
        }

        @Override
        public void onEventError(String streamName, Throwable error, boolean willRetry,
                int attemptNumber, io.flowwarden.stream.spi.ChangeEventMetadata metadata) {
        }

        @Override
        public void onCheckpoint(String streamName, String resumeToken) {
        }

        @Override
        public void onBufferStatus(String streamName, int currentSize, int maxSize) {
        }

        @Override
        public void onBackpressure(String streamName,
                io.flowwarden.stream.spi.BackpressureAction action) {
        }

        @Override
        public void onEventSentToDlq(String streamName) {
        }

        @Override
        public void onOplogStats(double logLengthHours, String status) {
        }
    }
}
