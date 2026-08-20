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
package io.flowwarden.stream.internal.reactive;

import com.mongodb.ClientSessionOptions;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoDatabase;
import io.flowwarden.stream.ChangeStreamContext;
import io.flowwarden.stream.OnHistoryLost;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Reactive twin of the oplog-template-resolution proof (#58): a sentinel
 * {@link ReactiveMongoDatabaseFactory} records the {@code local} database
 * reads on the secondary template during a {@code RESUME_FROM_OPLOG_START}
 * recovery — both test templates share one cluster, so <em>which factory was
 * consulted</em> is the only reliable observable.
 */
@SpringBootTest(classes = ReactiveOplogTemplateResolutionIntegrationTest.TestApp.class)
@ActiveProfiles("test-webflux")
class ReactiveOplogTemplateResolutionIntegrationTest {

    private static final String STREAM_NAME = "oplog-template-resolution-reactive";
    private static final String COLLECTION = "oplog_template_resolution_reactive";
    private static final BsonDocument EXPIRED_TOKEN =
            BsonDocument.parse("{\"_data\": \"0000DEAD\"}");

    static final AtomicInteger SENTINEL_LOCAL_READS = new AtomicInteger();

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired ReactiveStreamManager streamManager;
    @Autowired CheckpointStore checkpointStore;

    @BeforeEach
    void setUp() {
        SENTINEL_LOCAL_READS.set(0);
        checkpointStore.delete(STREAM_NAME);
    }

    @AfterEach
    void tearDown() {
        try { streamManager.stopStream(STREAM_NAME); } catch (Exception ignored) {}
        checkpointStore.delete(STREAM_NAME);
    }

    @Test
    void oplogStartRecovery_readsTheOplogOfTheStreamsOwnTemplate() {
        Instant past = Instant.now().minusSeconds(86_400);
        checkpointStore.save(new io.flowwarden.stream.spi.Checkpoint(
                STREAM_NAME, null, EXPIRED_TOKEN, past, EXPIRED_TOKEN, past,
                Collections.emptyMap()));

        streamManager.startStream(STREAM_NAME);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(STREAM_NAME));

        assertThat(SENTINEL_LOCAL_READS.get())
                .as("the oplog boundary must be read through the stream's own template")
                .isPositive();
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import({SentinelMongoConfig.class, SentinelHandler.class})
    static class TestApp {}

    @Configuration
    static class SentinelMongoConfig {
        @Bean
        ReactiveMongoTemplate sentinelReactiveMongoTemplate() {
            ReactiveMongoDatabaseFactory real = new SimpleReactiveMongoDatabaseFactory(
                    MongoClients.create(SharedMongoContainer.MONGO.getReplicaSetUrl()),
                    "oplog_sentinel_db_reactive");
            return new ReactiveMongoTemplate(new RecordingFactory(real));
        }
    }

    /**
     * Delegating factory recording {@code getMongoDatabase("local")} reads.
     */
    static final class RecordingFactory implements ReactiveMongoDatabaseFactory {
        private final ReactiveMongoDatabaseFactory delegate;

        RecordingFactory(ReactiveMongoDatabaseFactory delegate) {
            this.delegate = delegate;
        }

        @Override
        public Mono<MongoDatabase> getMongoDatabase(String dbName) {
            if ("local".equals(dbName)) {
                SENTINEL_LOCAL_READS.incrementAndGet();
            }
            return delegate.getMongoDatabase(dbName);
        }

        @Override
        public Mono<MongoDatabase> getMongoDatabase() {
            return delegate.getMongoDatabase();
        }

        @Override
        public PersistenceExceptionTranslator getExceptionTranslator() {
            return delegate.getExceptionTranslator();
        }

        @Override
        public CodecRegistry getCodecRegistry() {
            return delegate.getCodecRegistry();
        }

        @Override
        public Mono<ClientSession> getSession(ClientSessionOptions options) {
            return delegate.getSession(options);
        }

        @Override
        public ReactiveMongoDatabaseFactory withSession(ClientSession session) {
            return delegate.withSession(session);
        }

        @Override
        public boolean isTransactionActive() {
            return delegate.isTransactionActive();
        }
    }

    @ChangeStream(name = STREAM_NAME, collection = COLLECTION,
            documentType = Document.class, autoStart = false,
            mongoTemplateRef = "sentinelReactiveMongoTemplate")
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1, idleHeartbeatIntervalSeconds = 1,
            onHistoryLost = OnHistoryLost.RESUME_FROM_OPLOG_START)
    static class SentinelHandler {

        @OnInsert
        Mono<Void> handle(ChangeStreamContext<Document> ctx) {
            return Mono.empty();
        }
    }
}
