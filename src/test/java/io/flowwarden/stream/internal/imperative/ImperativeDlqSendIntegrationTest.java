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
import io.flowwarden.stream.internal.dlq.MongoDlqStore;
import io.flowwarden.stream.spi.DlqStore;
import io.flowwarden.stream.spi.FailedEvent;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.bson.Document;
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

@SpringBootTest(classes = ImperativeDlqSendIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeDlqSendIntegrationTest {

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired
    MongoTemplate mongoTemplate;

    @Autowired
    DlqSendHandler dlqSendHandler;

    @Autowired
    ImperativeStreamManager streamManager;

    @Autowired
    DlqStore dlqStore;

    @Test
    void sendToDlqPersistsEvent() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("dlq-test"));

        mongoTemplate.insert(new Document("reason", "test-dlq"), "dlq_orders");

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(dlqSendHandler.getProcessedEvents()).isNotEmpty());

        // Verify the event was persisted in DLQ
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    List<FailedEvent> events = dlqStore.findByStreamName("dlq-test");
                    assertThat(events).isNotEmpty();

                    FailedEvent event = events.get(0);
                    assertThat(event.streamName()).isEqualTo("dlq-test");
                    assertThat(event.operationType()).isEqualTo("INSERT");
                    assertThat(event.status()).isEqualTo(FailedEvent.STATUS_PENDING);
                    assertThat(event.error()).isNotNull();
                    assertThat(event.error().type()).isEqualTo("ManualDlq");
                    assertThat(event.error().message()).isEqualTo("manual reason");
                    assertThat(event.id()).isNotEmpty();
                    assertThat(event.attempts()).isEqualTo(1);
                });
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import(ImperativeDlqSendIntegrationTest.DlqSendHandler.class)
    static class TestApp {
    }

    @ChangeStream(name = "dlq-test", collection = "dlq_orders")
    @Checkpoint(saveEveryN = 1)
    static class DlqSendHandler {
        private final List<ChangeStreamContext<?>> events = new CopyOnWriteArrayList<>();

        @OnInsert
        void handle(ChangeStreamContext<?> ctx) {
            events.add(ctx);
            ctx.sendToDlq("manual reason");
        }

        List<ChangeStreamContext<?>> getProcessedEvents() {
            return events;
        }
    }
}
