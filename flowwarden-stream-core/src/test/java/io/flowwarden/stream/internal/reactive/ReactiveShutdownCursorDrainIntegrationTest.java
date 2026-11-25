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
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The shutdown leaves a recently disposed cursor the time to finish its
 * {@code getMore} before the context closes the MongoDB client — whatever
 * disposed it, and only for what is left of the window.
 *
 * <p>These tests pin the wait itself. The driver error it prevents
 * ({@code state should be: open}) is specific to the 5.x driver and does not
 * show on the driver this build runs with.</p>
 */
class ReactiveShutdownCursorDrainIntegrationTest {

    private static final String STREAM = "rea-shutdown-drain";

    /** Below the 1.2 s drain window, with room for timer granularity. */
    private static final long DRAIN_FLOOR_MILLIS = 1_000;

    @Test
    void contextClose_withARunningStream_waitsForTheCursorToDrain() {
        ConfigurableApplicationContext context = startApp();
        awaitRunning(context);

        assertThat(closeMillis(context)).isGreaterThanOrEqualTo(DRAIN_FLOOR_MILLIS);
    }

    @Test
    void contextClose_rightAfterAnExplicitStop_stillWaits() {
        // The stream is no longer registered when the shutdown runs, but
        // its getMore is as much in flight as if the shutdown had stopped it.
        ConfigurableApplicationContext context = startApp();
        awaitRunning(context);

        context.getBean(ReactiveStreamManager.class).stopStream(STREAM);

        assertThat(closeMillis(context)).isGreaterThanOrEqualTo(DRAIN_FLOOR_MILLIS);
    }

    @Test
    void contextClose_longAfterAnExplicitStop_doesNotWait() throws Exception {
        ConfigurableApplicationContext context = startApp();
        awaitRunning(context);

        context.getBean(ReactiveStreamManager.class).stopStream(STREAM);
        Thread.sleep(1_500); // the drain window is over

        assertThat(closeMillis(context)).isLessThan(DRAIN_FLOOR_MILLIS);
    }

    private static ConfigurableApplicationContext startApp() {
        return new SpringApplicationBuilder(TestApp.class)
                .web(WebApplicationType.NONE)
                .profiles("test-webflux")
                .properties("spring.data.mongodb.uri="
                        + SharedMongoContainer.MONGO.getReplicaSetUrl())
                .run();
    }

    private static void awaitRunning(ConfigurableApplicationContext context) {
        ReactiveStreamManager manager = context.getBean(ReactiveStreamManager.class);
        await().atMost(Duration.ofSeconds(10)).until(() -> manager.isRunning(STREAM));
    }

    private static long closeMillis(ConfigurableApplicationContext context) {
        long start = System.nanoTime();
        context.close();
        return Duration.ofNanos(System.nanoTime() - start).toMillis();
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import(IdleStream.class)
    static class TestApp {
    }

    @ChangeStream(name = STREAM, collection = "rea_shutdown_drain")
    static class IdleStream {
        @OnInsert
        Mono<Void> handle(ChangeStreamContext<Document> ctx) {
            return Mono.empty();
        }
    }
}
