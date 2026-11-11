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

import io.flowwarden.stream.ChangeStreamContext;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.annotation.DeadLetterQueue;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.MongoDlqOptions;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.internal.dlq.MongoDlqProperties;
import io.flowwarden.stream.internal.dlq.ReactiveMongoDlqStore;
import io.flowwarden.stream.spi.DlqPolicy;
import io.flowwarden.stream.spi.DlqStore;
import io.flowwarden.stream.spi.DlqStreamRegistration;
import io.flowwarden.stream.spi.FailedEvent;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Reactive twin of {@code ImperativeDlqCompositeRegistrationIntegrationTest}:
 * a user-provided {@link DlqStore} wrapping {@link ReactiveMongoDlqStore}
 * gets the per-stream collection and the TTL index through the forwarded
 * SPI hook, with no manual {@code registerStream} call.
 */
@SpringBootTest(classes = ReactiveDlqCompositeRegistrationIntegrationTest.TestApp.class)
@ActiveProfiles("test-webflux")
class ReactiveDlqCompositeRegistrationIntegrationTest {

    static final String STREAM = "reactive-composite-dlq-test";
    static final String WATCHED = "reactive_composite_dlq_orders";
    static final String DLQ_COLLECTION = "reactive_composite_dlq";

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired
    ReactiveMongoTemplate reactiveMongoTemplate;

    @Autowired
    ReactiveStreamManager streamManager;

    @Autowired
    RecordingCompositeDlqStore dlqStore;

    @Test
    void wrappedMongoStoreReceivesTheRegistrationAndRoutesToTheDeclaredCollection() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(STREAM));

        assertThat(dlqStore.registrations)
                .filteredOn(r -> STREAM.equals(r.streamName()))
                .singleElement()
                .satisfies(r -> {
                    assertThat(r.policy().retentionDays()).isEqualTo(7);
                    assertThat(r.policy().includeStackTrace()).isFalse();
                    assertThat(r.mongoCollection()).isEqualTo(DLQ_COLLECTION);
                });

        List<IndexInfo> indexes = reactiveMongoTemplate.indexOps(DLQ_COLLECTION)
                .getIndexInfo().collectList().block();
        assertThat(indexes)
                .as("TTL index on expiresAt in the declared collection")
                .anyMatch(idx -> idx.getExpireAfter().isPresent()
                        && idx.getIndexFields().stream().anyMatch(f -> "expiresAt".equals(f.getKey())));

        reactiveMongoTemplate.insert(new Document("reason", "composite"), WATCHED).block();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            List<Document> routed = reactiveMongoTemplate.findAll(Document.class, DLQ_COLLECTION)
                    .collectList().block();
            assertThat(routed).hasSize(1);
            assertThat(routed.get(0).getString("streamName")).isEqualTo(STREAM);
            assertThat(routed.get(0).get("expiresAt")).as("retentionDays applied").isNotNull();
        });
        assertThat(reactiveMongoTemplate.findAll(Document.class, "_fw_dlq").collectList().block())
                .as("nothing leaks into the default collection")
                .noneMatch(d -> STREAM.equals(d.getString("streamName")));
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import(ReactiveDlqCompositeRegistrationIntegrationTest.DlqHandler.class)
    static class TestApp {

        @Bean
        RecordingCompositeDlqStore dlqStore(ReactiveMongoTemplate reactiveMongoTemplate) {
            return new RecordingCompositeDlqStore(
                    new ReactiveMongoDlqStore(reactiveMongoTemplate, new MongoDlqProperties()));
        }
    }

    @ChangeStream(name = STREAM, collection = WATCHED)
    @Checkpoint(saveEveryN = 1)
    @DeadLetterQueue(retentionDays = 7, includeStackTrace = false)
    @MongoDlqOptions(collection = DLQ_COLLECTION)
    static class DlqHandler {
        @OnInsert
        Mono<Void> handle(ChangeStreamContext<?> ctx) {
            ctx.sendToDlq("routed through the composite");
            return Mono.empty();
        }
    }

    static class RecordingCompositeDlqStore implements DlqStore {
        final ReactiveMongoDlqStore delegate;
        final List<DlqStreamRegistration> registrations = new CopyOnWriteArrayList<>();

        RecordingCompositeDlqStore(ReactiveMongoDlqStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public void onStreamRegistered(DlqStreamRegistration registration) {
            registrations.add(registration);
            delegate.onStreamRegistered(registration);
        }

        @Override
        public void save(FailedEvent event, DlqPolicy policy) {
            delegate.save(event, policy);
        }

        @Override
        public Optional<FailedEvent> findById(String id) {
            return delegate.findById(id);
        }

        @Override
        public List<FailedEvent> findByStreamName(String streamName) {
            return delegate.findByStreamName(streamName);
        }

        @Override
        public long count(String streamName) {
            return delegate.count(streamName);
        }
    }
}
