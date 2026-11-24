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

import com.mongodb.client.MongoClients;
import io.flowwarden.stream.ChangeStreamContext;
import io.flowwarden.stream.DeploymentMode;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.annotation.DeadLetterQueue;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.internal.StateBackendReporter;
import io.flowwarden.stream.mongo.checkpoint.MongoCheckpointStore;
import io.flowwarden.stream.mongo.dlq.MongoDlqProperties;
import io.flowwarden.stream.mongo.dlq.MongoDlqStore;
import io.flowwarden.stream.mongo.lock.MongoLockService;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.DlqStore;
import io.flowwarden.stream.spi.LockService;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * An application whose primary {@code MongoTemplate} points to the database it
 * watches keeps its FlowWarden state in a second database by redeclaring the
 * three state beans on the template of that database, with the public
 * constructors of the MongoDB stores — the opposite layout of
 * {@code mongoTemplateRef}, which moves the watched collection and leaves the
 * state where it is.
 */
@SpringBootTest(classes = ImperativeStateInAnotherDatabaseIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeStateInAnotherDatabaseIntegrationTest {

    static final String STATE_DB = "fw_state_db_imperative";
    static final String STREAM = "state-in-another-db-test";
    static final String WATCHED = "state_in_another_db_orders";

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    /** The default template: the database the stream watches. */
    @Autowired
    @Qualifier("mongoTemplate")
    MongoTemplate mongoTemplate;

    @Autowired
    @Qualifier("stateMongoTemplate")
    MongoTemplate stateMongoTemplate;

    @Autowired
    ImperativeStreamManager streamManager;

    @Autowired
    StateBackendReporter stateBackendReporter;

    @Test
    void checkpointsDlqAndLocksAreWrittenToTheStateDatabase() {
        await().atMost(Duration.ofSeconds(10))
                .until(() -> streamManager.isRunning(STREAM));

        // SINGLE_LEADER: the lease is taken before the stream starts.
        assertThat(stateMongoTemplate.findById(STREAM, Document.class, "_fw_locks"))
                .as("lease in the state database").isNotNull();

        mongoTemplate.insert(new Document("reason", "state elsewhere"), WATCHED);

        Query byStream = Query.query(Criteria.where("streamName").is(STREAM));
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(stateMongoTemplate.find(byStream, Document.class, "_fw_dlq"))
                    .as("DLQ entry in the state database").hasSize(1);
            assertThat(stateMongoTemplate.findById(STREAM, Document.class, "_fw_checkpoints"))
                    .as("checkpoint in the state database").isNotNull();
        });

        // Nothing of this stream's state leaked into the watched (default) database.
        assertThat(mongoTemplate.getDb().getName()).isNotEqualTo(STATE_DB);
        assertThat(mongoTemplate.findById(STREAM, Document.class, "_fw_locks")).isNull();
        assertThat(mongoTemplate.findById(STREAM, Document.class, "_fw_checkpoints")).isNull();
        assertThat(mongoTemplate.find(byStream, Document.class, "_fw_dlq")).isEmpty();
    }

    @Test
    void theStartupLineNamesTheStateDatabaseForEachRole() {
        assertThat(stateBackendReporter.describe()).isEqualTo(
                "checkpoints=mongo[" + STATE_DB + "] dlq=mongo[" + STATE_DB + "] locks=mongo[" + STATE_DB + "]");
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import(ImperativeStateInAnotherDatabaseIntegrationTest.DlqHandler.class)
    static class TestApp {

        // An application with two databases declares both templates: Spring Boot's
        // own template backs off as soon as one is declared. The primary one is the
        // database the application reads and watches.
        @Bean
        @Primary
        MongoTemplate mongoTemplate(MongoDatabaseFactory factory) {
            return new MongoTemplate(factory);
        }

        @Bean
        MongoTemplate stateMongoTemplate() {
            return new MongoTemplate(
                    MongoClients.create(SharedMongoContainer.MONGO.getReplicaSetUrl()),
                    STATE_DB);
        }

        // The three beans of the documentation, verbatim.

        @Bean
        CheckpointStore checkpointStore(@Qualifier("stateMongoTemplate") MongoTemplate template) {
            return new MongoCheckpointStore(template);
        }

        @Bean
        DlqStore dlqStore(@Qualifier("stateMongoTemplate") MongoTemplate template,
                          MongoDlqProperties properties) {
            return new MongoDlqStore(template, properties);
        }

        @Bean
        LockService lockService(@Qualifier("stateMongoTemplate") MongoTemplate template) {
            return new MongoLockService(template);
        }
    }

    @ChangeStream(name = STREAM, collection = WATCHED, deploymentMode = DeploymentMode.SINGLE_LEADER)
    @Checkpoint(saveEveryN = 1)
    @DeadLetterQueue
    static class DlqHandler {
        @OnInsert
        void handle(ChangeStreamContext<?> ctx) {
            ctx.sendToDlq("state lives in another database");
        }
    }
}
