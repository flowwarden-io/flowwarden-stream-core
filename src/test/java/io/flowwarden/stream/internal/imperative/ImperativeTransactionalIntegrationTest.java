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
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = ImperativeTransactionalIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeTransactionalIntegrationTest {

    private static final String TXN_SUCCESS_COLLECTION = "txnOrders";
    private static final String TXN_FAIL_COLLECTION = "txnFailOrders";
    private static final String MANUAL_CP_COLLECTION = "manualCpOrders";
    private static final String PROCESSED_COLLECTION = "processed_orders";

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired
    MongoTemplate mongoTemplate;

    @Autowired
    TransactionalSuccessHandler successHandler;

    @Autowired
    TransactionalFailHandler failHandler;

    @Autowired
    ManualCheckpointHandler manualCpHandler;

    @Autowired
    ImperativeStreamManager streamManager;

    @Autowired
    CheckpointStore checkpointStore;

    @BeforeEach
    void setUp() {
        successHandler.clear();
        failHandler.clear();
        manualCpHandler.clear();
        mongoTemplate.remove(new Query(), PROCESSED_COLLECTION);
    }

    @Test
    void transactionalHandler_successCommitsAtomically() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("txn-success"));

        mongoTemplate.insert(new Document("item", "A").append("amount", 100), TXN_SUCCESS_COLLECTION);

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(successHandler.getProcessedIds()).hasSize(1));

        // Business write committed
        List<Document> processed = mongoTemplate.findAll(Document.class, PROCESSED_COLLECTION);
        assertThat(processed).anyMatch(d -> "PROCESSED".equals(d.getString("status")));

        // Checkpoint saved (via saveCheckpointNow inside the transaction)
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    var checkpoint = checkpointStore.findByStreamName("txn-success");
                    assertThat(checkpoint).isPresent();
                    assertThat(checkpoint.get().lastProcessedToken()).isNotNull();
                });
    }

    @Test
    void transactionalHandler_failureRollsBackAll() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("txn-fail"));

        mongoTemplate.insert(new Document("item", "FAIL"), TXN_FAIL_COLLECTION);

        // Wait for the handler to be invoked (it will throw after writing)
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(failHandler.getAttempts()).isGreaterThanOrEqualTo(1));

        // Business write should NOT exist (rolled back by transaction)
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(3))
                .untilAsserted(() -> {
                    List<Document> processed = mongoTemplate.findAll(Document.class, PROCESSED_COLLECTION);
                    assertThat(processed).noneMatch(d -> "SHOULD_NOT_EXIST".equals(d.getString("status")));
                });
    }

    @Test
    void manualCheckpointOnly_noDoubleSave() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("manual-cp"));

        mongoTemplate.insert(new Document("item", "M1"), MANUAL_CP_COLLECTION);

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(manualCpHandler.getEvents()).hasSize(1));

        // Checkpoint should be saved (via explicit saveCheckpointNow)
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    var checkpoint = checkpointStore.findByStreamName("manual-cp");
                    assertThat(checkpoint).isPresent();
                });

        // Verify the save count — should be exactly 1 (no double save)
        assertThat(manualCpHandler.getSaveCheckpointCount()).isEqualTo(1);
    }

    // --- Test application ---

    @SpringBootApplication
    @EnableFlowWarden
    @Import({
            ImperativeTransactionalIntegrationTest.TransactionalSuccessHandler.class,
            ImperativeTransactionalIntegrationTest.TransactionalFailHandler.class,
            ImperativeTransactionalIntegrationTest.ManualCheckpointHandler.class
    })
    static class TestApp {
        @Bean
        MongoTransactionManager transactionManager(MongoDatabaseFactory dbFactory) {
            return new MongoTransactionManager(dbFactory);
        }
    }

    // --- Handlers ---

    @ChangeStream(name = "txn-success", collection = TXN_SUCCESS_COLLECTION)
    @Checkpoint(saveEveryN = 1)
    static class TransactionalSuccessHandler {
        @Autowired
        MongoTemplate mongoTemplate;

        private final List<String> processedIds = new CopyOnWriteArrayList<>();

        @OnInsert
        @Transactional
        void handle(ChangeStreamContext<?> ctx) {
            mongoTemplate.insert(
                    new Document("eventId", ctx.getEventId())
                            .append("status", "PROCESSED"),
                    PROCESSED_COLLECTION);
            ctx.saveCheckpointNow();
            processedIds.add(ctx.getEventId());
        }

        List<String> getProcessedIds() {
            return processedIds;
        }

        void clear() {
            processedIds.clear();
        }
    }

    @ChangeStream(name = "txn-fail", collection = TXN_FAIL_COLLECTION)
    @Checkpoint(saveEveryN = 1)
    @DeadLetterQueue
    static class TransactionalFailHandler {
        @Autowired
        MongoTemplate mongoTemplate;

        private final AtomicInteger attempts = new AtomicInteger(0);

        @OnInsert
        @Transactional
        void handle(ChangeStreamContext<?> ctx) {
            mongoTemplate.insert(
                    new Document("orderId", "fail-test")
                            .append("status", "SHOULD_NOT_EXIST"),
                    PROCESSED_COLLECTION);
            ctx.saveCheckpointNow();
            attempts.incrementAndGet();
            throw new RuntimeException("Simulated failure after writes");
        }

        int getAttempts() {
            return attempts.get();
        }

        void clear() {
            attempts.set(0);
        }
    }

    @ChangeStream(name = "manual-cp", collection = MANUAL_CP_COLLECTION)
    @Checkpoint(saveEveryN = 1)
    static class ManualCheckpointHandler {
        private final List<ChangeStreamContext<?>> events = new CopyOnWriteArrayList<>();
        private final AtomicInteger saveCheckpointCount = new AtomicInteger(0);

        @OnInsert
        void handle(ChangeStreamContext<?> ctx) {
            events.add(ctx);
            saveCheckpointCount.incrementAndGet();
            ctx.saveCheckpointNow();
            // No @Transactional — just testing that automatic checkpoint is skipped
        }

        List<ChangeStreamContext<?>> getEvents() {
            return events;
        }

        int getSaveCheckpointCount() {
            return saveCheckpointCount.get();
        }

        void clear() {
            events.clear();
            saveCheckpointCount.set(0);
        }
    }
}
