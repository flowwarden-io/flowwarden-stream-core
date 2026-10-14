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
import io.flowwarden.stream.FlowWardenMetrics;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.spi.StopReason;
import io.flowwarden.stream.spi.StreamMetricsProvider;
import io.flowwarden.stream.test.RecordingMetricsProvider;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = ReactiveStreamCrashIntegrationTest.TestApp.class)
@ActiveProfiles("test-webflux")
class ReactiveStreamCrashIntegrationTest {

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired
    ReactiveMongoTemplate reactiveMongoTemplate;

    @Autowired
    ReactiveStreamManager streamManager;

    // A hand-written provider, not a Mockito mock: the provider is global and the
    // probe/stats/flush schedulers call it concurrently — stubbing a mock while
    // they run is an UnfinishedStubbing race (seen on CI).
    private RecordingMetricsProvider metrics;

    @BeforeEach
    void setUp() {
        metrics = new RecordingMetricsProvider();
        FlowWardenMetrics.setProvider(metrics);
    }

    @AfterEach
    void tearDown() {
        FlowWardenMetrics.setProvider(StreamMetricsProvider.noOp());
    }

    @Test
    void unexpectedPipelineTerminationEmitsCrashedSignal() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("rea-crash-test"));

        // onEventReceived runs synchronously inside handleEventReactive (before the
        // handler). A throw here propagates as onError into the pipeline, gets
        // captured by .doOnError(lastError::set), then .onErrorResume swallows it
        // into Mono.empty() which terminates the source Flux. .doFinally then
        // sees gracefulStop==false and emits CRASHED.
        RuntimeException injectedCause = new RuntimeException("simulated internal crash");
        metrics.failOnEventReceived("rea-crash-test", injectedCause);

        reactiveMongoTemplate.insert(new Document("item", "boom"), "rea_crash_test").block();

        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThat(metrics.stops()).contains(
                        new RecordingMetricsProvider.Stop("rea-crash-test", StopReason.CRASHED, injectedCause)));

        // Per-stream state must be cleared so the public APIs stop lying
        // once the pipeline has terminated unexpectedly.
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(streamManager.isRunning("rea-crash-test")).isFalse();
                    assertThat(streamManager.getLastEventTime("rea-crash-test")).isNull();
                });
    }

    @Test
    void gracefulStopEmitsGracefulSignal() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("rea-graceful-test"));

        streamManager.stopStream("rea-graceful-test");

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(metrics.stops()).contains(
                        new RecordingMetricsProvider.Stop("rea-graceful-test", StopReason.GRACEFUL, null)));

        assertThat(streamManager.isRunning("rea-graceful-test")).isFalse();
        assertThat(streamManager.getLastEventTime("rea-graceful-test")).isNull();
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import({CrashTestHandler.class, GracefulTestHandler.class})
    static class TestApp {
    }

    @ChangeStream(name = "rea-crash-test", collection = "rea_crash_test")
    static class CrashTestHandler {
        @OnInsert
        Mono<Void> handle(ChangeStreamContext<?> ctx) {
            return Mono.empty();
        }
    }

    @ChangeStream(name = "rea-graceful-test", collection = "rea_graceful_test")
    static class GracefulTestHandler {
        @OnInsert
        Mono<Void> handle(ChangeStreamContext<?> ctx) {
            return Mono.empty();
        }
    }
}
