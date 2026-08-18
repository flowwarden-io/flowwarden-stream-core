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
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.internal.checkpoint.MongoCheckpointStore;
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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Bootstrap persistence is a startup precondition (PR #61 review, point 2): a
 * stream with no prior checkpoint whose initial position cannot be persisted
 * must fail its start instead of running on a non-durable position — a crash
 * before the next checkpoint would silently restart from a newer position.
 */
@SpringBootTest(classes = ImperativeBootstrapPersistenceIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeBootstrapPersistenceIntegrationTest {

    private static final String STREAM_NAME = "bootstrap-persistence";
    private static final String COLLECTION = "bootstrap_persistence";

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired MongoTemplate mongoTemplate;
    @Autowired ImperativeStreamManager streamManager;
    @Autowired FailingSeenStore checkpointStore;
    @Autowired BootstrapHandler handler;

    @BeforeEach
    void setUp() {
        handler.clear();
        mongoTemplate.dropCollection(COLLECTION);
        checkpointStore.failSeenWrites.set(false);
        checkpointStore.delete(STREAM_NAME);
    }

    @AfterEach
    void tearDown() {
        checkpointStore.failSeenWrites.set(false);
        try { streamManager.stopStream(STREAM_NAME); } catch (Exception ignored) {}
        checkpointStore.delete(STREAM_NAME);
    }

    @Test
    void bootstrapPersistenceFailure_failsTheStart_insteadOfRunningNonDurable() {
        checkpointStore.failSeenWrites.set(true);

        assertThatThrownBy(() -> streamManager.startStream(STREAM_NAME))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("checkpoint store down");
        assertThat(streamManager.isRunning(STREAM_NAME)).isFalse();
        assertThat(checkpointStore.findByStreamName(STREAM_NAME)).isEmpty();

        // Once the store recovers, the same stream starts durably.
        checkpointStore.failSeenWrites.set(false);
        streamManager.startStream(STREAM_NAME);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(STREAM_NAME));
        assertThat(checkpointStore.findByStreamName(STREAM_NAME)
                .orElseThrow().lastSeenToken()).isNotNull();
    }

    @Test
    void oplogStartRecovery_establishmentWriteFails_deadTokensSurvive_recoveryReenters() {
        // PR-review blocker: the dead tokens are the durable "recovery due"
        // marker. When the establishment write fails (crash-equivalent), they
        // must survive so the restart re-enters the history-lost recovery —
        // never the "no checkpoint" fresh bootstrap (which would silently
        // skip the still-readable history).
        var expired = BsonDocument.parse("{\"_data\": \"0000DEAD\"}");
        Instant past = Instant.now().minusSeconds(86_400);
        checkpointStore.save(new io.flowwarden.stream.spi.Checkpoint(
                CRASH_STREAM, null, expired, past, expired, past,
                java.util.Collections.emptyMap()));

        checkpointStore.failSeenWrites.set(true);
        streamManager.startStream(CRASH_STREAM);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(CRASH_STREAM));

        // Establishment attempts keep failing: the marker must stay intact.
        await().atMost(Duration.ofSeconds(8)).pollDelay(Duration.ofSeconds(3))
                .untilAsserted(() -> {
                    var cp = checkpointStore.findByStreamName(CRASH_STREAM).orElseThrow();
                    assertThat(cp.lastSeenToken())
                            .as("the recovery marker must survive failed establishment writes")
                            .isEqualTo(expired);
                    assertThat(cp.lastProcessedToken()).isEqualTo(expired);
                });

        // "Crash" and restart with a healthy store: the recovery re-enters
        // (the dead tokens force the history-lost path — a fresh bootstrap is
        // structurally impossible while they exist) and completes durably.
        streamManager.stopStream(CRASH_STREAM);
        checkpointStore.failSeenWrites.set(false);
        streamManager.startStream(CRASH_STREAM);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var cp = checkpointStore.findByStreamName(CRASH_STREAM).orElseThrow();
            assertThat(cp.lastSeenToken()).isNotNull().isNotEqualTo(expired);
            assertThat(cp.lastProcessedToken())
                    .as("the deferred cleanup removes the dead processed token")
                    .isNull();
        });

        streamManager.stopStream(CRASH_STREAM);
        checkpointStore.delete(CRASH_STREAM);
    }

    private static final String CRASH_STREAM = "bootstrap-crash-window";
    private static final String CRASH_COLLECTION = "bootstrap_crash_window";

    @SpringBootApplication
    @EnableFlowWarden
    @Import({BootstrapHandler.class, CrashWindowHandler.class})
    static class TestApp {

        @Bean
        FailingSeenStore checkpointStore(MongoTemplate mongoTemplate) {
            return new FailingSeenStore(new MongoCheckpointStore(mongoTemplate));
        }
    }

    /**
     * Delegating store whose combined seen+heartbeat write can be toggled to
     * fail — simulating a checkpoint backend outage during bootstrap.
     */
    static class FailingSeenStore implements CheckpointStore {

        final AtomicBoolean failSeenWrites = new AtomicBoolean(false);
        private final CheckpointStore delegate;

        FailingSeenStore(CheckpointStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public void saveSeen(String streamName, BsonDocument token, Instant timestamp,
                             Instant heartbeatTimestamp) {
            if (failSeenWrites.get()) {
                throw new RuntimeException("checkpoint store down");
            }
            delegate.saveSeen(streamName, token, timestamp, heartbeatTimestamp);
        }

        @Override
        public void resetAfterHistoryLost(String streamName, BsonDocument freshSeenToken,
                                          BsonDocument expectedDeadProcessed, Instant timestamp) {
            if (failSeenWrites.get()) {
                throw new RuntimeException("checkpoint store down");
            }
            delegate.resetAfterHistoryLost(streamName, freshSeenToken, expectedDeadProcessed, timestamp);
        }

        @Override
        public void save(io.flowwarden.stream.spi.Checkpoint checkpoint) {
            delegate.save(checkpoint);
        }

        @Override
        public Optional<io.flowwarden.stream.spi.Checkpoint> findByStreamName(String streamName) {
            return delegate.findByStreamName(streamName);
        }

        @Override
        public void saveSeen(String streamName, BsonDocument token, Instant timestamp) {
            delegate.saveSeen(streamName, token, timestamp);
        }

        @Override
        public void saveHeartbeat(String streamName, Instant heartbeatTimestamp) {
            delegate.saveHeartbeat(streamName, heartbeatTimestamp);
        }

        @Override
        public void saveProcessed(String streamName, BsonDocument token, Instant timestamp) {
            delegate.saveProcessed(streamName, token, timestamp);
        }

        @Override
        public void delete(String streamName) {
            delegate.delete(streamName);
        }
    }

    @ChangeStream(name = STREAM_NAME, collection = COLLECTION,
            documentType = Document.class, autoStart = false)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1)
    static class BootstrapHandler {

        @OnInsert
        void handle(ChangeStreamContext<Document> ctx) {
        }

        void clear() {
        }
    }

    @ChangeStream(name = CRASH_STREAM, collection = CRASH_COLLECTION,
            documentType = Document.class, autoStart = false)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 0, idleHeartbeatIntervalSeconds = 0,
            onHistoryLost = io.flowwarden.stream.OnHistoryLost.RESUME_FROM_OPLOG_START)
    static class CrashWindowHandler {

        @OnInsert
        void handle(ChangeStreamContext<Document> ctx) {
        }
    }
}
