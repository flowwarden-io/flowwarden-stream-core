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
import io.flowwarden.stream.internal.discovery.StreamRegistry;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Reactive twin: {@code @EnableFlowWarden(enabled = false)} registers and
 * validates the streams, starts none, and leaves explicit starts available.
 */
@SpringBootTest(classes = ReactiveEnableFlowWardenDisabledIntegrationTest.TestApp.class)
@ActiveProfiles("test-webflux")
class ReactiveEnableFlowWardenDisabledIntegrationTest {

    static final String STREAM = "disabled-boot-reactive-test";

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired ReactiveMongoTemplate reactiveMongoTemplate;
    @Autowired ReactiveStreamManager streamManager;
    @Autowired StreamRegistry registry;
    @Autowired Handler handler;

    @AfterEach
    void tearDown() {
        streamManager.stopStream(STREAM);
    }

    @Test
    void streamsAreRegisteredButNotStarted_untilStartedExplicitly() {
        assertThat(registry.isAutoStartEnabled()).isFalse();
        assertThat(registry.findByName(STREAM)).isPresent();
        assertThat(streamManager.isRunning(STREAM)).isFalse();
        assertThat(streamManager.isRestartPending(STREAM)).isFalse();

        reactiveMongoTemplate.insert(new Document("item", "while-disabled"), "disabled_boot_reactive").block();

        streamManager.startStream(STREAM);
        await().atMost(Duration.ofSeconds(10)).until(() -> streamManager.isRunning(STREAM));

        reactiveMongoTemplate.insert(new Document("item", "after-manual-start"), "disabled_boot_reactive").block();
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(handler.items).contains("after-manual-start"));
    }

    @SpringBootApplication
    @EnableFlowWarden(enabled = false)
    @Import(ReactiveEnableFlowWardenDisabledIntegrationTest.Handler.class)
    static class TestApp {}

    @ChangeStream(name = STREAM, collection = "disabled_boot_reactive")
    static class Handler {
        final List<String> items = new CopyOnWriteArrayList<>();

        @OnInsert
        Mono<Void> handle(ChangeStreamContext<Document> ctx) {
            ctx.getFullDocument(Document.class).ifPresent(d -> items.add(d.getString("item")));
            return Mono.empty();
        }
    }
}
