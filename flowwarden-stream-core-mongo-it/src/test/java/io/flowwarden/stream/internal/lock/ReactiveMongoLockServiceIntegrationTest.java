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

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.flowwarden.stream.spi.LockService;
import io.flowwarden.stream.spi.testkit.LockServiceContractTest;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link ReactiveMongoLockService} against the shared {@link LockServiceContractTest},
 * plus the expiry / takeover cases that need the backing document edited in place —
 * the same extra cases as {@code MongoLockServiceIntegrationTest}.
 */
class ReactiveMongoLockServiceIntegrationTest extends LockServiceContractTest {

    private final List<MongoClient> openClients = new ArrayList<>();
    private ReactiveMongoTemplate reactiveMongoTemplate;

    @Override
    protected LockService createLockService() {
        ensureTemplate();
        return new ReactiveMongoLockService(reactiveMongoTemplate);
    }

    @Override
    protected void cleanState() {
        ensureTemplate();
        reactiveMongoTemplate.remove(new Query(), ReactiveMongoLockService.COLLECTION).block();
    }

    @Override
    protected boolean expireLeaseInPlace(String streamName) {
        expireInPlace(streamName);
        return true;
    }

    @AfterEach
    void closeClients() {
        for (MongoClient client : openClients) {
            client.close();
        }
        openClients.clear();
        reactiveMongoTemplate = null;
    }

    private void ensureTemplate() {
        if (reactiveMongoTemplate == null) {
            reactiveMongoTemplate = new ReactiveMongoTemplate(
                    openClient(SharedMongoContainer.MONGO.getReplicaSetUrl()), "test-locks-reactive");
        }
    }

    private MongoClient openClient(String connectionString) {
        MongoClient client = MongoClients.create(connectionString);
        openClients.add(client);
        return client;
    }

    @Test
    void tryAcquire_succeedsIfLockExpired() {
        lockService.tryAcquire("my-stream", INSTANCE_A, TTL);
        expireInPlace("my-stream");

        assertThat(lockService.tryAcquire("my-stream", INSTANCE_B, TTL)).isTrue();
    }

    @Test
    void renew_failsIfLockStolenByOtherInstance() {
        lockService.tryAcquire("my-stream", INSTANCE_A, TTL);

        // Manually change the owner in the document
        updateInPlace("my-stream", new Document("instanceId", INSTANCE_B));

        assertThat(lockService.renew("my-stream", INSTANCE_A, TTL)).isFalse();
    }

    @Test
    void renew_afterExpiry_failsAndLeavesTheExpiredLeaseUntouched() {
        lockService.tryAcquire("my-stream", INSTANCE_A, TTL);
        expireInPlace("my-stream");
        Date expiredAt = storedExpiresAt("my-stream");

        // Still A's document, nobody took it over — but it is no longer A's to extend.
        assertThat(lockService.renew("my-stream", INSTANCE_A, TTL)).isFalse();
        assertThat(storedExpiresAt("my-stream")).isEqualTo(expiredAt);

        // The way back is an explicit acquire, which succeeds on an expired lease.
        assertThat(lockService.tryAcquire("my-stream", INSTANCE_A, TTL)).isTrue();
        assertThat(storedExpiresAt("my-stream")).isAfter(new Date());
    }

    @Test
    void getLockState_emptyIfExpired() {
        lockService.tryAcquire("my-stream", INSTANCE_A, TTL);
        expireInPlace("my-stream");

        assertThat(lockService.getLockState("my-stream")).isEmpty();
    }

    @Test
    void tryAcquire_returnsFalseInsteadOfThrowingWhenTheBackendIsUnreachable() {
        // Error contract of the SPI: a transient backend error is a false, not a throw.
        ReactiveMongoTemplate unreachable = new ReactiveMongoTemplate(
                openClient("mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=200&connectTimeoutMS=200"),
                "unreachable");
        LockService service = new ReactiveMongoLockService(unreachable);

        assertThat(service.tryAcquire("my-stream", INSTANCE_A, TTL)).isFalse();
        assertThat(service.renew("my-stream", INSTANCE_A, TTL)).isFalse();
        assertThat(service.getLockState("my-stream")).isEmpty();
    }

    private void expireInPlace(String streamName) {
        updateInPlace(streamName,
                new Document("expiresAt", Date.from(Instant.now().minusSeconds(10))));
    }

    private void updateInPlace(String streamName, Document fields) {
        reactiveMongoTemplate.getCollection(ReactiveMongoLockService.COLLECTION)
                .flatMap(collection -> Mono.from(collection.updateOne(
                        new Document("_id", streamName),
                        new Document("$set", fields))))
                .block();
    }

    private Date storedExpiresAt(String streamName) {
        Document doc = reactiveMongoTemplate.findById(streamName, Document.class,
                ReactiveMongoLockService.COLLECTION).block();
        assertThat(doc).isNotNull();
        return doc.getDate("expiresAt");
    }
}
