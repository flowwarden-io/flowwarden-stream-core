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
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.mongo.checkpoint.ReactiveMongoCheckpointStore;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A graceful stop landing while a processed-anchor save is in flight lets
 * that save finish, and takes no new event while it waits: nothing handled
 * before the stop returned is delivered again on the next start.
 */
@SpringBootTest(classes = ReactiveStopDuringCheckpointSaveIntegrationTest.TestApp.class)
@ActiveProfiles("test-webflux")
class ReactiveStopDuringCheckpointSaveIntegrationTest {

    private static final String DELIVERY_STREAM = "rea-stop-during-save";
    private static final String DELIVERY_COLLECTION = "rea_stop_during_save";
    private static final String FLUSH_STREAM = "rea-stop-during-flush";
    private static final String FLUSH_COLLECTION = "rea_stop_during_flush";

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired
    ReactiveMongoTemplate reactiveMongoTemplate;

    @Autowired
    SlowSaveCheckpointStore store;

    @Autowired
    DeliveryStream deliveryStream;

    @Autowired
    FlushStream flushStream;

    @Autowired
    ReactiveStreamManager streamManager;

    @Test
    void stopDuringAnInFlightSave_letsTheSaveFinish_andTheEventIsNotReplayed() throws Exception {
        await().atMost(Duration.ofSeconds(10)).until(() -> streamManager.isRunning(DELIVERY_STREAM));

        // A first event anchored normally: the stream has a resume position
        // BEFORE the event under test, so a lost save of that event shows up
        // as a replay on the next start.
        reactiveMongoTemplate.insert(new Document("item", "A"), DELIVERY_COLLECTION).block();
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(store.findByStreamName(DELIVERY_STREAM))
                        .hasValueSatisfying(cp -> assertThat(cp.lastProcessedToken()).isNotNull()));
        BsonDocument anchorOfA =
                store.findByStreamName(DELIVERY_STREAM).orElseThrow().lastProcessedToken();

        store.holdNextSave(DELIVERY_STREAM, 800);
        reactiveMongoTemplate.insert(new Document("item", "B"), DELIVERY_COLLECTION).block();
        assertThat(store.awaitHeldSaveEntered(15, TimeUnit.SECONDS))
                .as("the save of B must be in flight before the stop")
                .isTrue();

        streamManager.stopStream(DELIVERY_STREAM);

        assertThat(store.interruptedDuringSave())
                .as("the stop must not interrupt the in-flight save")
                .isFalse();
        assertThat(store.findByStreamName(DELIVERY_STREAM).orElseThrow().lastProcessedToken())
                .as("the anchor of B must be durable once the stop returned")
                .isNotEqualTo(anchorOfA);

        streamManager.startStream(DELIVERY_STREAM);
        await().atMost(Duration.ofSeconds(10)).until(() -> streamManager.isRunning(DELIVERY_STREAM));
        reactiveMongoTemplate.insert(new Document("item", "C"), DELIVERY_COLLECTION).block();

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(deliveryStream.items()).contains("C"));
        assertThat(deliveryStream.items())
                .as("B was settled before the stop and must not be delivered again")
                .containsExactly("A", "B", "C");
    }

    @Test
    void stopWaitingForAFlush_takesNoNewEvent_soNothingHandledIsReplayed() throws Exception {
        await().atMost(Duration.ofSeconds(10)).until(() -> streamManager.isRunning(FLUSH_STREAM));

        // The save of A fails on the delivery path and stays dirty; the
        // periodic flush retries it and is held in the store. Unlike a save
        // on the delivery path, a flush does not hold the dispatch: the
        // cursor is free to deliver while the stop waits for it.
        store.failNextSave(FLUSH_STREAM);
        store.holdNextSave(FLUSH_STREAM, 2_500);
        reactiveMongoTemplate.insert(new Document("item", "A"), FLUSH_COLLECTION).block();
        assertThat(store.awaitHeldSaveEntered(20, TimeUnit.SECONDS))
                .as("the flush retrying A must be in flight before the stop")
                .isTrue();

        Thread stop = new Thread(() -> streamManager.stopStream(FLUSH_STREAM), "stop");
        stop.start();
        Thread.sleep(300); // the stop is waiting for the flush
        reactiveMongoTemplate.insert(new Document("item", "B"), FLUSH_COLLECTION).block();
        stop.join(15_000);
        assertThat(stop.isAlive()).isFalse();

        assertThat(store.interruptedDuringSave())
                .as("the stop must not interrupt the in-flight flush")
                .isFalse();
        assertThat(flushStream.items())
                .as("an event arriving while the stop waits must not be handled")
                .containsExactly("A");

        streamManager.startStream(FLUSH_STREAM);
        await().atMost(Duration.ofSeconds(10)).until(() -> streamManager.isRunning(FLUSH_STREAM));
        reactiveMongoTemplate.insert(new Document("item", "C"), FLUSH_COLLECTION).block();

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(flushStream.items()).contains("C"));
        assertThat(flushStream.items())
                .as("each event is delivered once across the stop")
                .containsExactly("A", "B", "C");
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import({DeliveryStream.class, FlushStream.class, SlowSaveStoreConfig.class})
    static class TestApp {
    }

    @Configuration
    static class SlowSaveStoreConfig {
        @Bean
        @Primary
        SlowSaveCheckpointStore slowSaveCheckpointStore(ReactiveMongoTemplate template) {
            return new SlowSaveCheckpointStore(template);
        }
    }

    // Timer and probes opted out: the delivery path is the only writer of
    // the checkpoint, so the held save is the save of the event under test.
    @ChangeStream(name = DELIVERY_STREAM, collection = DELIVERY_COLLECTION)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 0, idleHeartbeatIntervalSeconds = 0)
    static class DeliveryStream extends RecordingStream {
    }

    // The flush timer is the writer under test; probes opted out.
    @ChangeStream(name = FLUSH_STREAM, collection = FLUSH_COLLECTION)
    @Checkpoint(saveEveryN = 1, saveIntervalSeconds = 1, idleHeartbeatIntervalSeconds = 0)
    static class FlushStream extends RecordingStream {
    }

    abstract static class RecordingStream {
        private final List<String> items = new CopyOnWriteArrayList<>();

        @OnInsert
        Mono<Void> handle(ChangeStreamContext<Document> ctx) {
            items.add(ctx.getFullDocument(Document.class).orElseThrow().getString("item"));
            return Mono.empty();
        }

        List<String> items() {
            return items;
        }
    }

    /**
     * The real reactive store, with one save that can be failed and one
     * that can be held long enough for a stop to land while it is in
     * flight. The hold is interruptible, like the {@code block()} of the
     * real write.
     */
    static final class SlowSaveCheckpointStore extends ReactiveMongoCheckpointStore {
        private final AtomicBoolean failNext = new AtomicBoolean(false);
        private final AtomicBoolean holdNext = new AtomicBoolean(false);
        private final AtomicBoolean interrupted = new AtomicBoolean(false);
        private volatile String failStream;
        private volatile String holdStream;
        private volatile long holdMillis;
        private volatile CountDownLatch entered = new CountDownLatch(1);

        SlowSaveCheckpointStore(ReactiveMongoTemplate template) {
            super(template);
        }

        /** The next save of the stream throws; it is consumed before a hold. */
        void failNextSave(String streamName) {
            failStream = streamName;
            failNext.set(true);
        }

        void holdNextSave(String streamName, long millis) {
            interrupted.set(false);
            entered = new CountDownLatch(1);
            holdStream = streamName;
            holdMillis = millis;
            holdNext.set(true);
        }

        boolean awaitHeldSaveEntered(long timeout, TimeUnit unit) throws InterruptedException {
            return entered.await(timeout, unit);
        }

        boolean interruptedDuringSave() {
            return interrupted.get();
        }

        @Override
        public void saveProcessed(String streamName, BsonDocument token, Instant timestamp,
                                  Instant heartbeatTimestamp) {
            if (streamName.equals(failStream) && failNext.compareAndSet(true, false)) {
                throw new IllegalStateException("injected save failure");
            }
            if (streamName.equals(holdStream) && holdNext.compareAndSet(true, false)) {
                entered.countDown();
                try {
                    Thread.sleep(holdMillis);
                } catch (InterruptedException e) {
                    interrupted.set(true);
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("save interrupted", e);
                }
            }
            super.saveProcessed(streamName, token, timestamp, heartbeatTimestamp);
        }
    }
}
