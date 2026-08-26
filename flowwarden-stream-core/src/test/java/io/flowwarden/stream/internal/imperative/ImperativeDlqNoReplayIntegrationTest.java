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
import io.flowwarden.stream.annotation.DeadLetterQueue;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.annotation.RetryPolicy;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.DlqStore;
import io.flowwarden.stream.test.SharedMongoContainer;
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

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The settled-anchor semantics close the duplicate-DLQ window: a DLQ'd event
 * is terminally settled and advances {@code lastProcessedToken}, so a
 * restart no longer re-delivers it (it used to sit after the last handler
 * SUCCESS, get replayed, re-fail and be dead-lettered twice — the DLQ
 * reserve already preserves the event, the anchor may pass it).
 */
@SpringBootTest(classes = ImperativeDlqNoReplayIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeDlqNoReplayIntegrationTest {

    private static final String STREAM = "imp-dlq-no-replay";
    private static final String COLLECTION = "impDlqNoReplayOrders";

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired MongoTemplate mongoTemplate;
    @Autowired ImperativeStreamManager streamManager;
    @Autowired CheckpointStore checkpointStore;
    @Autowired DlqStore dlqStore;
    @Autowired FailOnceHandler handler;

    @AfterEach
    void tearDown() {
        try { streamManager.stopStream(STREAM); } catch (Exception ignored) {}
        checkpointStore.delete(STREAM);
    }

    @Test
    void dlqSettledEvent_isAnchored_andNeverDeadLetteredTwiceAcrossARestart() {
        streamManager.startStream(STREAM);
        await().atMost(Duration.ofSeconds(5)).until(() -> streamManager.isRunning(STREAM));

        // A poison document: exhausts its retries and lands in the DLQ.
        mongoTemplate.insert(new Document("poison", true).append("id", "p1"), COLLECTION);
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(dlqStore.findByStreamName(STREAM)).hasSize(1));

        // The DLQ decision settled the event: the anchor advances past it.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(checkpointStore.findByStreamName(STREAM)
                        .orElseThrow().lastProcessedToken()).isNotNull());

        // Restart: the resume from the anchor must NOT re-deliver the poison
        // event — no second DLQ entry, no re-invocation.
        streamManager.stopStream(STREAM);
        int invocationsBeforeRestart = handler.invocations.size();
        streamManager.startStream(STREAM);
        await().atMost(Duration.ofSeconds(5)).until(() -> streamManager.isRunning(STREAM));

        // Drive a healthy event through to prove the stream is live past the
        // poison position.
        mongoTemplate.insert(new Document("poison", false).append("id", "ok1"), COLLECTION);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(handler.successes).contains("ok1"));

        assertThat(dlqStore.findByStreamName(STREAM))
                .as("the settled poison event must not be dead-lettered twice")
                .hasSize(1);
        assertThat(handler.invocations.stream().filter("p1"::equals).count())
                .as("the poison event was not re-delivered after the restart")
                .isEqualTo(handler.invocations.subList(0, invocationsBeforeRestart)
                        .stream().filter("p1"::equals).count());
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import(FailOnceHandler.class)
    static class TestApp {}

    @ChangeStream(name = STREAM, collection = COLLECTION, autoStart = false)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1)
    @RetryPolicy(maxAttempts = 2, initialDelay = "50ms", maxDelay = "100ms", jitter = false)
    @DeadLetterQueue
    static class FailOnceHandler {
        final List<String> invocations = new CopyOnWriteArrayList<>();
        final List<String> successes = new CopyOnWriteArrayList<>();

        @OnInsert
        void handle(ChangeStreamContext<Document> ctx) {
            Document doc = ctx.getFullDocument(Document.class).orElseThrow();
            invocations.add(doc.getString("id"));
            if (Boolean.TRUE.equals(doc.getBoolean("poison"))) {
                throw new RuntimeException("poison document");
            }
            successes.add(doc.getString("id"));
        }
    }
}
