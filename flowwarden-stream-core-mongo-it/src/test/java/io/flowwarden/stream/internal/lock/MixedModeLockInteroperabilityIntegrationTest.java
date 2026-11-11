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
package io.flowwarden.stream.internal.lock;

import io.flowwarden.stream.spi.LockService;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An IMPERATIVE instance ({@link MongoLockService}, synchronous driver) and a REACTIVE
 * instance ({@link ReactiveMongoLockService}, reactive driver) contending for the same
 * stream lease on the same {@code _fw_locks} collection — the mixed-mode guarantee: both
 * implementations speak the same protocol, so exactly one leader is elected and every
 * transition is visible to the other side.
 */
class MixedModeLockInteroperabilityIntegrationTest {

    private static final String DB = "test-locks-mixed";
    private static final String STREAM = "orders-projection";
    private static final String IMPERATIVE_INSTANCE = "imperative-host:app:8080";
    private static final String REACTIVE_INSTANCE = "reactive-host:app:8080";
    private static final Duration TTL = Duration.ofSeconds(60);

    private com.mongodb.client.MongoClient syncClient;
    private com.mongodb.reactivestreams.client.MongoClient reactiveClient;
    private MongoTemplate mongoTemplate;
    private ReactiveMongoTemplate reactiveMongoTemplate;
    private LockService imperative;
    private LockService reactive;

    @BeforeEach
    void setUp() {
        syncClient = com.mongodb.client.MongoClients.create(SharedMongoContainer.MONGO.getReplicaSetUrl());
        reactiveClient = com.mongodb.reactivestreams.client.MongoClients.create(
                SharedMongoContainer.MONGO.getReplicaSetUrl());
        mongoTemplate = new MongoTemplate(syncClient, DB);
        reactiveMongoTemplate = new ReactiveMongoTemplate(reactiveClient, DB);
        mongoTemplate.remove(new Query(), MongoLockService.COLLECTION);
        imperative = new MongoLockService(mongoTemplate);
        reactive = new ReactiveMongoLockService(reactiveMongoTemplate);
    }

    @AfterEach
    void closeClients() {
        if (syncClient != null) {
            syncClient.close();
        }
        if (reactiveClient != null) {
            reactiveClient.close();
        }
    }

    @Test
    void imperativeHolder_isSeenAndRespectedByReactiveInstance() {
        assertThat(imperative.tryAcquire(STREAM, IMPERATIVE_INSTANCE, TTL)).isTrue();

        assertThat(reactive.tryAcquire(STREAM, REACTIVE_INSTANCE, TTL)).isFalse();
        assertThat(reactive.renew(STREAM, REACTIVE_INSTANCE, TTL)).isFalse();
        assertThat(reactive.getCurrentLeader(STREAM)).hasValue(IMPERATIVE_INSTANCE);

        // The non-owner's release is a no-op on the other side's lease
        reactive.release(STREAM, REACTIVE_INSTANCE);
        assertThat(imperative.renew(STREAM, IMPERATIVE_INSTANCE, TTL)).isTrue();
        assertThat(reactive.getCurrentLeader(STREAM)).hasValue(IMPERATIVE_INSTANCE);
    }

    @Test
    void reactiveHolder_isSeenAndRespectedByImperativeInstance() {
        assertThat(reactive.tryAcquire(STREAM, REACTIVE_INSTANCE, TTL)).isTrue();

        assertThat(imperative.tryAcquire(STREAM, IMPERATIVE_INSTANCE, TTL)).isFalse();
        assertThat(imperative.renew(STREAM, IMPERATIVE_INSTANCE, TTL)).isFalse();
        assertThat(imperative.getCurrentLeader(STREAM)).hasValue(REACTIVE_INSTANCE);

        imperative.release(STREAM, IMPERATIVE_INSTANCE);
        assertThat(reactive.renew(STREAM, REACTIVE_INSTANCE, TTL)).isTrue();
        assertThat(imperative.getCurrentLeader(STREAM)).hasValue(REACTIVE_INSTANCE);
    }

    @Test
    void releaseByHolder_letsTheOtherSideTakeOver() {
        assertThat(imperative.tryAcquire(STREAM, IMPERATIVE_INSTANCE, TTL)).isTrue();
        imperative.release(STREAM, IMPERATIVE_INSTANCE);

        assertThat(reactive.tryAcquire(STREAM, REACTIVE_INSTANCE, TTL)).isTrue();
        assertThat(imperative.getCurrentLeader(STREAM)).hasValue(REACTIVE_INSTANCE);

        reactive.release(STREAM, REACTIVE_INSTANCE);
        assertThat(imperative.tryAcquire(STREAM, IMPERATIVE_INSTANCE, TTL)).isTrue();
        assertThat(reactive.getCurrentLeader(STREAM)).hasValue(IMPERATIVE_INSTANCE);
    }

    @Test
    void expiredLease_isTakenOverByTheOtherSide() {
        assertThat(reactive.tryAcquire(STREAM, REACTIVE_INSTANCE, TTL)).isTrue();
        mongoTemplate.getDb().getCollection(MongoLockService.COLLECTION).updateOne(
                new Document("_id", STREAM),
                new Document("$set", new Document("expiresAt", Date.from(Instant.now().minusSeconds(10)))));

        assertThat(imperative.getLockState(STREAM)).isEmpty();
        assertThat(imperative.tryAcquire(STREAM, IMPERATIVE_INSTANCE, TTL)).isTrue();
        assertThat(reactive.renew(STREAM, REACTIVE_INSTANCE, TTL)).isFalse();
    }

    @Test
    void concurrentElection_electsExactlyOneLeader() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            for (int round = 0; round < 10; round++) {
                mongoTemplate.remove(new Query(), MongoLockService.COLLECTION);

                List<Callable<Boolean>> contenders = List.of(
                        () -> imperative.tryAcquire(STREAM, IMPERATIVE_INSTANCE, TTL),
                        () -> reactive.tryAcquire(STREAM, REACTIVE_INSTANCE, TTL));
                List<Future<Boolean>> results = new ArrayList<>(pool.invokeAll(contenders));

                int winners = 0;
                for (Future<Boolean> result : results) {
                    if (result.get()) {
                        winners++;
                    }
                }
                assertThat(winners).as("round %d", round).isEqualTo(1);

                String leader = imperative.getCurrentLeader(STREAM).orElseThrow();
                assertThat(reactive.getCurrentLeader(STREAM)).hasValue(leader);
                assertThat(leader).isIn(IMPERATIVE_INSTANCE, REACTIVE_INSTANCE);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void bothImplementations_writeTheSameDocumentLayout() {
        imperative.tryAcquire("stream-imperative", IMPERATIVE_INSTANCE, TTL);
        reactive.tryAcquire("stream-reactive", REACTIVE_INSTANCE, TTL);

        Document fromImperative = mongoTemplate.getDb().getCollection(MongoLockService.COLLECTION)
                .find(new Document("_id", "stream-imperative")).first();
        Document fromReactive = mongoTemplate.getDb().getCollection(MongoLockService.COLLECTION)
                .find(new Document("_id", "stream-reactive")).first();

        assertThat(fromImperative).isNotNull();
        assertThat(fromReactive).isNotNull();
        assertThat(fromReactive.keySet()).containsExactlyInAnyOrderElementsOf(fromImperative.keySet());
        assertThat(fromReactive.get("acquiredAt")).isInstanceOf(Date.class);
        assertThat(fromReactive.get("expiresAt")).isInstanceOf(Date.class);
    }
}
