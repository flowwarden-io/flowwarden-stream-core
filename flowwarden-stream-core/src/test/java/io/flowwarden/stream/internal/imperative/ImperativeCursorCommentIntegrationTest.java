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
import io.flowwarden.stream.FlowWarden;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnChange;
import io.flowwarden.stream.test.CursorCommentAssertions;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;

import static org.awaitility.Awaitility.await;

/**
 * Acceptance for the cursor-attribution comment, imperative mode: the
 * running stream's cursor must appear in {@code $currentOp} stamped with
 * {@code flowwarden:<streamName>}.
 */
@SpringBootTest(classes = ImperativeCursorCommentIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeCursorCommentIntegrationTest {

    private static final String STREAM_NAME = "imp-cursor-comment";
    private static final String COLLECTION = "imp_cursor_comment";

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired ImperativeStreamManager streamManager;

    @AfterEach
    void tearDown() {
        try { streamManager.stopStream(STREAM_NAME); } catch (Exception ignored) {}
    }

    @Test
    void runningStreamCursor_isStampedInCurrentOp() {
        streamManager.startStream(STREAM_NAME);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(STREAM_NAME));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                CursorCommentAssertions.assertCursorStamped(
                        FlowWarden.CURSOR_COMMENT_PREFIX + STREAM_NAME, COLLECTION));
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import(CommentedHandler.class)
    static class TestApp {}

    @ChangeStream(name = STREAM_NAME, collection = COLLECTION,
            documentType = Document.class, autoStart = false)
    static class CommentedHandler {
        @OnChange
        void handle(ChangeStreamContext<Document> ctx) {
        }
    }
}
