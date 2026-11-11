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
import io.flowwarden.stream.FlowWardenMetrics;
import io.flowwarden.stream.HistoryLostException;
import io.flowwarden.stream.OnHistoryLost;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.StopReason;
import io.flowwarden.stream.spi.StreamMetricsProvider;
import io.flowwarden.stream.test.RecordingMetricsProvider;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Boot contract of {@code ALL_INSTANCES} streams: a stream that cannot
 * start on {@code ApplicationReadyEvent} must not take the application nor
 * the other streams down with it. A terminal failure is reported as
 * {@code CRASHED} and left alone; a transient one enters the restart loop.
 *
 * <p>The failing conditions are installed <em>before</em> the context is
 * ready (a seeding bean), so the three streams below go through the real
 * boot path — the very fact that this test class runs proves the context
 * came up.</p>
 */
@SpringBootTest(classes = ImperativeBootLifecycleIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeBootLifecycleIntegrationTest {

    private static final BsonDocument FAKE_TOKEN = BsonDocument.parse("{\"_data\": \"DEADBEEF\"}");
    static final String FAIL_STREAM = "boot-fail-test";
    static final String OK_STREAM = "boot-ok-test";
    static final String UNREACHABLE_STREAM = "boot-unreachable-test";
    static final RecordingMetricsProvider METRICS = new RecordingMetricsProvider();

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired MongoTemplate mongoTemplate;
    @Autowired ImperativeStreamManager streamManager;
    @Autowired CheckpointStore checkpointStore;
    @Autowired OkHandler okHandler;

    @AfterAll
    static void resetMetrics() {
        FlowWardenMetrics.setProvider(StreamMetricsProvider.noOp());
    }

    @Test
    void terminalBootFailure_doesNotPreventTheOtherStreamsFromStarting() {
        await().atMost(Duration.ofSeconds(10)).until(() -> streamManager.isRunning(OK_STREAM));

        assertThat(streamManager.isRunning(FAIL_STREAM)).isFalse();
        assertThat(streamManager.isRestartPending(FAIL_STREAM)).isFalse();

        mongoTemplate.insert(new Document("item", "after-boot"), "boot_ok");
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(okHandler.items).contains("after-boot"));
    }

    @Test
    void terminalBootFailure_isReportedAsCrashed_andKeepsTheCheckpointForTheOperator() {
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(METRICS.stops()).anyMatch(s -> s.streamName().equals(FAIL_STREAM)));

        RecordingMetricsProvider.Stop stop = METRICS.stops().stream()
                .filter(s -> s.streamName().equals(FAIL_STREAM)).findFirst().orElseThrow();
        assertThat(stop.reason()).isEqualTo(StopReason.CRASHED);
        assertThat(stop.cause()).isInstanceOf(HistoryLostException.class);

        assertThat(checkpointStore.findByStreamName(FAIL_STREAM)).isPresent()
                .get().extracting(io.flowwarden.stream.spi.Checkpoint::lastProcessedToken)
                .isEqualTo(FAKE_TOKEN);
    }

    @Test
    void transientBootFailure_isHandedToTheRestartLoop() {
        try {
            await().atMost(Duration.ofSeconds(10))
                    .until(() -> streamManager.isRestartPending(UNREACHABLE_STREAM));
            assertThat(streamManager.isRunning(UNREACHABLE_STREAM)).isFalse();

            RecordingMetricsProvider.Stop stop = METRICS.stops().stream()
                    .filter(s -> s.streamName().equals(UNREACHABLE_STREAM)).findFirst().orElseThrow();
            assertThat(stop.reason()).isEqualTo(StopReason.CRASHED);
            assertThat(stop.cause()).isNotInstanceOf(HistoryLostException.class);
        } finally {
            // An operator stop wins over the loop: no restart keeps hammering
            // the unreachable address once the test has observed the hand-off.
            streamManager.stopStream(UNREACHABLE_STREAM);
        }
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import({
            ImperativeBootLifecycleIntegrationTest.BootSeed.class,
            ImperativeBootLifecycleIntegrationTest.UnreachableTemplateConfig.class,
            ImperativeBootLifecycleIntegrationTest.FailHandler.class,
            ImperativeBootLifecycleIntegrationTest.OkHandler.class,
            ImperativeBootLifecycleIntegrationTest.UnreachableHandler.class
    })
    static class TestApp {}

    /**
     * Runs during singleton creation, strictly before {@code ApplicationReadyEvent}:
     * installs the recording provider and the dead checkpoint the boot will trip on.
     */
    static class BootSeed {
        BootSeed(CheckpointStore store) {
            FlowWardenMetrics.setProvider(METRICS);
            Instant past = Instant.now().minusSeconds(86_400);
            store.save(new io.flowwarden.stream.spi.Checkpoint(
                    FAIL_STREAM, null, FAKE_TOKEN, past, FAKE_TOKEN, past, Collections.emptyMap()));
        }
    }

    /**
     * A user-defined {@code MongoTemplate} suppresses Spring Boot's default one
     * ({@code @ConditionalOnMissingBean(MongoOperations.class)}), so the reachable
     * default is declared here too, as {@code @Primary}, on Boot's database factory.
     */
    @Configuration
    static class UnreachableTemplateConfig {
        @Bean
        @Primary
        MongoTemplate mongoTemplate(MongoDatabaseFactory factory) {
            return new MongoTemplate(factory);
        }

        @Bean
        MongoTemplate unreachableMongoTemplate() {
            return new MongoTemplate(MongoClients.create(
                    "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=300&connectTimeoutMS=300&socketTimeoutMS=300"),
                    "unreachable");
        }
    }

    @ChangeStream(name = FAIL_STREAM, collection = "boot_fail")
    @Checkpoint(saveEveryN = 1, onHistoryLost = OnHistoryLost.FAIL)
    static class FailHandler {
        @OnInsert
        void handle(ChangeStreamContext<Document> ctx) {}
    }

    @ChangeStream(name = OK_STREAM, collection = "boot_ok")
    static class OkHandler {
        final List<String> items = new CopyOnWriteArrayList<>();

        @OnInsert
        void handle(ChangeStreamContext<Document> ctx) {
            ctx.getFullDocument(Document.class).ifPresent(d -> items.add(d.getString("item")));
        }
    }

    @ChangeStream(name = UNREACHABLE_STREAM, collection = "boot_unreachable",
            mongoTemplateRef = "unreachableMongoTemplate")
    static class UnreachableHandler {
        @OnInsert
        void handle(ChangeStreamContext<Document> ctx) {}
    }
}
