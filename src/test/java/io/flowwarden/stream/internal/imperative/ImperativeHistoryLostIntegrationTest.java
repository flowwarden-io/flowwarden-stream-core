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
import io.flowwarden.stream.HistoryLostException;
import io.flowwarden.stream.OnHistoryLost;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.spi.CheckpointStore;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import io.flowwarden.stream.test.SharedMongoContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = ImperativeHistoryLostIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeHistoryLostIntegrationTest {

    private static final BsonDocument FAKE_TOKEN =
            BsonDocument.parse("{\"_data\": \"DEADBEEF\"}");

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired MongoTemplate mongoTemplate;
    @Autowired ImperativeStreamManager streamManager;
    @Autowired CheckpointStore checkpointStore;
    @Autowired HistoryLostResumeNowHandler resumeNowHandler;
    @Autowired HistoryLostResumeOplogHandler resumeOplogHandler;

    @AfterEach
    void tearDown() {
        try { streamManager.stopStream("hl-fail-test"); } catch (Exception ignored) {}
        try { streamManager.stopStream("hl-resume-now-test"); } catch (Exception ignored) {}
        try { streamManager.stopStream("hl-resume-oplog-test"); } catch (Exception ignored) {}
        checkpointStore.delete("hl-fail-test");
        checkpointStore.delete("hl-resume-now-test");
        checkpointStore.delete("hl-resume-oplog-test");
        resumeNowHandler.clear();
        resumeOplogHandler.clear();
    }

    @Test
    void failStrategy_throwsHistoryLostException() {
        saveExpiredCheckpoint("hl-fail-test");

        assertThatThrownBy(() -> streamManager.startStream("hl-fail-test"))
                .isInstanceOf(HistoryLostException.class)
                .hasMessageContaining("hl-fail-test")
                .hasMessageContaining("resume token has expired");
    }

    @Test
    void failStrategy_preservesCheckpoint() {
        saveExpiredCheckpoint("hl-fail-test");

        try { streamManager.startStream("hl-fail-test"); } catch (HistoryLostException ignored) {}

        assertThat(checkpointStore.findByStreamName("hl-fail-test")).isPresent();
    }

    @Test
    void resumeFromNow_startsSuccessfully() {
        saveExpiredCheckpoint("hl-resume-now-test");

        streamManager.startStream("hl-resume-now-test");

        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("hl-resume-now-test"));
    }

    @Test
    void resumeFromNow_receivesNewEvents() {
        saveExpiredCheckpoint("hl-resume-now-test");

        streamManager.startStream("hl-resume-now-test");

        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("hl-resume-now-test"));

        mongoTemplate.insert(new Document("item", "after-recovery"), "hl_resume_now");

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(resumeNowHandler.getEvents()).hasSizeGreaterThanOrEqualTo(1));
    }

    @Test
    void resumeFromOplogStart_startsSuccessfully() {
        saveExpiredCheckpoint("hl-resume-oplog-test");

        streamManager.startStream("hl-resume-oplog-test");

        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("hl-resume-oplog-test"));
    }

    private void saveExpiredCheckpoint(String streamName) {
        Instant past = Instant.now().minusSeconds(86400);
        checkpointStore.save(new io.flowwarden.stream.spi.Checkpoint(
                streamName, null, FAKE_TOKEN, past,
                FAKE_TOKEN, past, Collections.emptyMap()));
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import({
            ImperativeHistoryLostIntegrationTest.HistoryLostFailHandler.class,
            ImperativeHistoryLostIntegrationTest.HistoryLostResumeNowHandler.class,
            ImperativeHistoryLostIntegrationTest.HistoryLostResumeOplogHandler.class
    })
    static class TestApp {}

    @ChangeStream(name = "hl-fail-test", collection = "hl_fail", autoStart = false)
    @Checkpoint(saveEveryN = 1, onHistoryLost = OnHistoryLost.FAIL)
    static class HistoryLostFailHandler {
        @OnInsert
        void handle(ChangeStreamContext<?> ctx) {}
    }

    @ChangeStream(name = "hl-resume-now-test", collection = "hl_resume_now", autoStart = false)
    @Checkpoint(saveEveryN = 1, onHistoryLost = OnHistoryLost.RESUME_FROM_NOW)
    static class HistoryLostResumeNowHandler {
        private final List<ChangeStreamContext<?>> events = new CopyOnWriteArrayList<>();

        @OnInsert
        void handle(ChangeStreamContext<?> ctx) {
            events.add(ctx);
        }

        List<ChangeStreamContext<?>> getEvents() { return events; }
        void clear() { events.clear(); }
    }

    @ChangeStream(name = "hl-resume-oplog-test", collection = "hl_resume_oplog", autoStart = false)
    @Checkpoint(saveEveryN = 1, onHistoryLost = OnHistoryLost.RESUME_FROM_OPLOG_START)
    static class HistoryLostResumeOplogHandler {
        private final List<ChangeStreamContext<?>> events = new CopyOnWriteArrayList<>();

        @OnInsert
        void handle(ChangeStreamContext<?> ctx) {
            events.add(ctx);
        }

        List<ChangeStreamContext<?>> getEvents() { return events; }
        void clear() { events.clear(); }
    }
}
