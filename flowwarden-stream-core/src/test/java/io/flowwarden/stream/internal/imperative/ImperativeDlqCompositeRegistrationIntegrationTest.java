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
import io.flowwarden.stream.annotation.MongoDlqOptions;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.internal.dlq.MongoDlqProperties;
import io.flowwarden.stream.internal.dlq.MongoDlqStore;
import io.flowwarden.stream.registration.CheckpointSpec;
import io.flowwarden.stream.registration.DeadLetterQueueSpec;
import io.flowwarden.stream.registration.MongoDlqOptionsSpec;
import io.flowwarden.stream.registration.StreamDefinitionContributor;
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
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A user-provided {@link DlqStore} that <em>wraps</em> the Mongo store (the
 * hybrid composite shown in the AMQP documentation, a metrics decorator)
 * must get per-stream collections and the TTL index without calling
 * {@code registerStream} by hand: the manager announces every DLQ-enabled
 * stream through the SPI hook, and the wrapper forwards it.
 */
@SpringBootTest(classes = ImperativeDlqCompositeRegistrationIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeDlqCompositeRegistrationIntegrationTest {

    static final String STREAM = "composite-dlq-test";
    static final String WATCHED = "composite_dlq_orders";
    static final String DLQ_COLLECTION = "composite_dlq";

    static final String CONTRIBUTED_STREAM = "composite-contributed-dlq-test";
    static final String CONTRIBUTED_WATCHED = "composite_contributed_dlq_orders";
    static final String CONTRIBUTED_DLQ_COLLECTION = "composite_contributed_dlq";

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired
    MongoTemplate mongoTemplate;

    @Autowired
    ImperativeStreamManager streamManager;

    @Autowired
    RecordingCompositeDlqStore dlqStore;

    @Test
    void wrappedMongoStoreReceivesTheRegistrationAndRoutesToTheDeclaredCollection() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(STREAM));

        // The hook reached the composite with the resolved policy and hint…
        assertThat(dlqStore.registrations)
                .filteredOn(r -> STREAM.equals(r.streamName()))
                .singleElement()
                .satisfies(r -> {
                    assertThat(r.policy().retentionDays()).isEqualTo(7);
                    assertThat(r.policy().includeStackTrace()).isFalse();
                    assertThat(r.mongoCollection()).isEqualTo(DLQ_COLLECTION);
                    assertThat(r.hasMongoCollection()).isTrue();
                });

        // …and the forwarded call provisioned the TTL index on that collection.
        assertThat(mongoTemplate.indexOps(DLQ_COLLECTION).getIndexInfo())
                .as("TTL index on expiresAt in the declared collection")
                .anyMatch(idx -> idx.getExpireAfter().isPresent()
                        && idx.getIndexFields().stream().anyMatch(f -> "expiresAt".equals(f.getKey())));

        mongoTemplate.insert(new Document("reason", "composite"), WATCHED);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            List<Document> routed = mongoTemplate.findAll(Document.class, DLQ_COLLECTION);
            assertThat(routed).hasSize(1);
            assertThat(routed.get(0).getString("streamName")).isEqualTo(STREAM);
            assertThat(routed.get(0).get("expiresAt")).as("retentionDays applied").isNotNull();
        });
        assertThat(mongoTemplate.findAll(Document.class, "_fw_dlq"))
                .as("nothing leaks into the default collection")
                .noneMatch(d -> STREAM.equals(d.getString("streamName")));
    }

    /**
     * Same guarantee for a stream contributed through
     * {@link StreamDefinitionContributor}: the {@code DeadLetterQueueSpec} /
     * {@code MongoDlqOptionsSpec} pair travels through
     * {@code StreamSpecConverter} into the registration the hook receives.
     */
    @Test
    void contributedStreamSpecsReachTheHookAndRouteToTheDeclaredCollection() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning(CONTRIBUTED_STREAM));

        assertThat(dlqStore.registrations)
                .filteredOn(r -> CONTRIBUTED_STREAM.equals(r.streamName()))
                .singleElement()
                .satisfies(r -> {
                    assertThat(r.policy().retentionDays()).isEqualTo(3);
                    assertThat(r.policy().includeOriginalDocument()).isFalse();
                    assertThat(r.mongoCollection()).isEqualTo(CONTRIBUTED_DLQ_COLLECTION);
                });

        assertThat(mongoTemplate.indexOps(CONTRIBUTED_DLQ_COLLECTION).getIndexInfo())
                .as("TTL index on expiresAt in the contributed stream's collection")
                .anyMatch(idx -> idx.getExpireAfter().isPresent()
                        && idx.getIndexFields().stream().anyMatch(f -> "expiresAt".equals(f.getKey())));

        mongoTemplate.insert(new Document("reason", "contributed"), CONTRIBUTED_WATCHED);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            List<Document> routed = mongoTemplate.findAll(Document.class, CONTRIBUTED_DLQ_COLLECTION);
            assertThat(routed).hasSize(1);
            assertThat(routed.get(0).getString("streamName")).isEqualTo(CONTRIBUTED_STREAM);
            assertThat(routed.get(0).get("expiresAt")).as("retentionDays applied").isNotNull();
        });
        assertThat(mongoTemplate.findAll(Document.class, "_fw_dlq"))
                .noneMatch(d -> CONTRIBUTED_STREAM.equals(d.getString("streamName")));
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import(ImperativeDlqCompositeRegistrationIntegrationTest.DlqHandler.class)
    static class TestApp {

        @Bean
        RecordingCompositeDlqStore dlqStore(MongoTemplate mongoTemplate) {
            return new RecordingCompositeDlqStore(
                    new MongoDlqStore(mongoTemplate, new MongoDlqProperties()));
        }

        @Bean
        StreamDefinitionContributor contributedDlqStream() {
            return registration -> registration.stream(CONTRIBUTED_STREAM, Document.class)
                    .collection(CONTRIBUTED_WATCHED)
                    .checkpoint(CheckpointSpec.builder().saveEveryN(1).build())
                    .deadLetterQueue(DeadLetterQueueSpec.builder()
                            .retentionDays(3)
                            .includeOriginalDocument(false)
                            .build())
                    .mongoDlqOptions(MongoDlqOptionsSpec.builder()
                            .collection(CONTRIBUTED_DLQ_COLLECTION)
                            .build())
                    .onInsert((order, ctx) -> ctx.sendToDlq("contributed, routed through the composite"));
        }
    }

    @ChangeStream(name = STREAM, collection = WATCHED)
    @Checkpoint(saveEveryN = 1)
    @DeadLetterQueue(retentionDays = 7, includeStackTrace = false)
    @MongoDlqOptions(collection = DLQ_COLLECTION)
    static class DlqHandler {
        @OnInsert
        void handle(ChangeStreamContext<?> ctx) {
            ctx.sendToDlq("routed through the composite");
        }
    }

    /**
     * The shape a composite / decorator takes: every SPI method forwarded,
     * the startup hook included.
     */
    static class RecordingCompositeDlqStore implements DlqStore {
        final MongoDlqStore delegate;
        final List<DlqStreamRegistration> registrations = new CopyOnWriteArrayList<>();

        RecordingCompositeDlqStore(MongoDlqStore delegate) {
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
