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

import com.mongodb.reactivestreams.client.MongoClients;
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
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Reactive twin of the imperative boot-lifecycle test: a stream that cannot
 * start on {@code ApplicationReadyEvent} in {@code ALL_INSTANCES} mode is
 * reported ({@code CRASHED}) or handed to the restart loop, and never takes
 * the application or the other streams down.
 */
@SpringBootTest(classes = ReactiveBootLifecycleIntegrationTest.TestApp.class)
@ActiveProfiles("test-webflux")
class ReactiveBootLifecycleIntegrationTest {

    private static final BsonDocument FAKE_TOKEN = BsonDocument.parse("{\"_data\": \"DEADBEEF\"}");
    static final String FAIL_STREAM = "boot-fail-reactive-test";
    static final String OK_STREAM = "boot-ok-reactive-test";
    static final String UNREACHABLE_STREAM = "boot-unreachable-reactive-test";
    static final RecordingMetricsProvider METRICS = new RecordingMetricsProvider();

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired ReactiveMongoTemplate reactiveMongoTemplate;
    @Autowired ReactiveStreamManager streamManager;
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

        reactiveMongoTemplate.insert(new Document("item", "after-boot"), "boot_ok_reactive").block();
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
            streamManager.stopStream(UNREACHABLE_STREAM);
        }
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import({
            ReactiveBootLifecycleIntegrationTest.BootSeed.class,
            ReactiveBootLifecycleIntegrationTest.UnreachableTemplateConfig.class,
            ReactiveBootLifecycleIntegrationTest.FailHandler.class,
            ReactiveBootLifecycleIntegrationTest.OkHandler.class,
            ReactiveBootLifecycleIntegrationTest.UnreachableHandler.class
    })
    static class TestApp {}

    static class BootSeed {
        BootSeed(CheckpointStore store) {
            FlowWardenMetrics.setProvider(METRICS);
            Instant past = Instant.now().minusSeconds(86_400);
            store.save(new io.flowwarden.stream.spi.Checkpoint(
                    FAIL_STREAM, null, FAKE_TOKEN, past, FAKE_TOKEN, past, Collections.emptyMap()));
        }
    }

    /**
     * A user-defined {@code ReactiveMongoTemplate} suppresses Spring Boot's default
     * one, so the reachable default is declared here too, as {@code @Primary}.
     */
    @Configuration
    static class UnreachableTemplateConfig {
        @Bean
        @Primary
        ReactiveMongoTemplate reactiveMongoTemplate(ReactiveMongoDatabaseFactory factory) {
            return new ReactiveMongoTemplate(factory);
        }

        @Bean
        ReactiveMongoTemplate unreachableReactiveMongoTemplate() {
            return new ReactiveMongoTemplate(MongoClients.create(
                    "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=300&connectTimeoutMS=300&socketTimeoutMS=300"),
                    "unreachable");
        }
    }

    @ChangeStream(name = FAIL_STREAM, collection = "boot_fail_reactive")
    @Checkpoint(saveEveryN = 1, onHistoryLost = OnHistoryLost.FAIL)
    static class FailHandler {
        @OnInsert
        Mono<Void> handle(ChangeStreamContext<Document> ctx) {
            return Mono.empty();
        }
    }

    @ChangeStream(name = OK_STREAM, collection = "boot_ok_reactive")
    static class OkHandler {
        final List<String> items = new CopyOnWriteArrayList<>();

        @OnInsert
        Mono<Void> handle(ChangeStreamContext<Document> ctx) {
            ctx.getFullDocument(Document.class).ifPresent(d -> items.add(d.getString("item")));
            return Mono.empty();
        }
    }

    @ChangeStream(name = UNREACHABLE_STREAM, collection = "boot_unreachable_reactive",
            mongoTemplateRef = "unreachableReactiveMongoTemplate")
    static class UnreachableHandler {
        @OnInsert
        Mono<Void> handle(ChangeStreamContext<Document> ctx) {
            return Mono.empty();
        }
    }
}
