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
import io.flowwarden.stream.FlowWardenMetrics;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.spi.StopReason;
import io.flowwarden.stream.spi.StreamMetricsProvider;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = ImperativeStreamCrashIntegrationTest.TestApp.class)
@ActiveProfiles("test-mvc")
class ImperativeStreamCrashIntegrationTest {

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired
    MongoTemplate mongoTemplate;

    @Autowired
    ImperativeStreamManager streamManager;

    private StreamMetricsProvider metrics;

    @BeforeEach
    void setUp() {
        metrics = mock(StreamMetricsProvider.class);
        FlowWardenMetrics.setProvider(metrics);
    }

    @AfterEach
    void tearDown() {
        FlowWardenMetrics.setProvider(StreamMetricsProvider.noOp());
    }

    @Test
    void uncaughtThrowableFromListenerEmitsCrashedSignal() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("imp-crash-test"));

        // onEventReceived is called from handleMessage at the very top, BEFORE the
        // try/catch that covers handler InvocationTargetException. A throw here is
        // the simplest way to simulate any uncaught exception escaping handleMessage.
        RuntimeException injectedCause = new RuntimeException("simulated internal crash");
        doThrow(injectedCause).when(metrics)
                .onEventReceived(eq("imp-crash-test"), any());

        mongoTemplate.insert(new Document("item", "boom"), "imp_crash_test");

        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> verify(metrics).onStreamStopped(
                        eq("imp-crash-test"),
                        eq(StopReason.CRASHED),
                        same(injectedCause)));
    }

    @Test
    void gracefulStopEmitsGracefulSignal() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> streamManager.isRunning("imp-graceful-test"));

        streamManager.stopStream("imp-graceful-test");

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> verify(metrics).onStreamStopped(
                        eq("imp-graceful-test"),
                        eq(StopReason.GRACEFUL),
                        isNull()));
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import({CrashTestHandler.class, GracefulTestHandler.class})
    static class TestApp {
    }

    @ChangeStream(name = "imp-crash-test", collection = "imp_crash_test")
    static class CrashTestHandler {
        @OnInsert
        void handle(ChangeStreamContext<?> ctx) {
            // no-op; the test forces a crash via the metrics provider
        }
    }

    @ChangeStream(name = "imp-graceful-test", collection = "imp_graceful_test")
    static class GracefulTestHandler {
        @OnInsert
        void handle(ChangeStreamContext<?> ctx) {
            // no-op
        }
    }
}
