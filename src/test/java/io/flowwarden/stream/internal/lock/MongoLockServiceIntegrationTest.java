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

import com.mongodb.client.MongoClients;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import static org.assertj.core.api.Assertions.assertThat;

class MongoLockServiceIntegrationTest {

    private MongoTemplate mongoTemplate;
    private MongoLockService instanceA;
    private MongoLockService instanceB;

    @BeforeEach
    void setUp() {
        mongoTemplate = new MongoTemplate(
                MongoClients.create(SharedMongoContainer.MONGO.getReplicaSetUrl()), "test-locks"
        );
        mongoTemplate.remove(new Query(), MongoLockService.COLLECTION);
        instanceA = new MongoLockService(mongoTemplate, "host-a:app:8080");
        instanceB = new MongoLockService(mongoTemplate, "host-b:app:8080");
    }

    @Test
    void tryAcquire_succeedsOnFreeLock() {
        assertThat(instanceA.tryAcquire("my-stream")).isTrue();
    }

    @Test
    void tryAcquire_failsIfHeldByOtherInstance() {
        instanceA.tryAcquire("my-stream");
        assertThat(instanceB.tryAcquire("my-stream")).isFalse();
    }

    @Test
    void tryAcquire_succeedsIfAlreadyOwned() {
        instanceA.tryAcquire("my-stream");
        assertThat(instanceA.tryAcquire("my-stream")).isTrue();
    }

    @Test
    void tryAcquire_succeedsIfLockExpired() {
        // Acquire with instance A, then manually expire the lock
        instanceA.tryAcquire("my-stream");
        mongoTemplate.getDb().getCollection(MongoLockService.COLLECTION)
                .updateOne(
                        new org.bson.Document("_id", "my-stream"),
                        new org.bson.Document("$set", new org.bson.Document("expiresAt",
                                java.util.Date.from(java.time.Instant.now().minusSeconds(10)))));

        // Instance B should now be able to acquire
        assertThat(instanceB.tryAcquire("my-stream")).isTrue();
    }

    @Test
    void renew_extendsLock() {
        instanceA.tryAcquire("my-stream");
        assertThat(instanceA.renew("my-stream")).isTrue();
    }

    @Test
    void renew_failsIfLockStolenByOtherInstance() {
        instanceA.tryAcquire("my-stream");

        // Manually change the owner to B
        mongoTemplate.getDb().getCollection(MongoLockService.COLLECTION)
                .updateOne(
                        new org.bson.Document("_id", "my-stream"),
                        new org.bson.Document("$set", new org.bson.Document("instanceId", "host-b:app:8080")));

        assertThat(instanceA.renew("my-stream")).isFalse();
    }

    @Test
    void release_removesLock() {
        instanceA.tryAcquire("my-stream");
        instanceA.release("my-stream");

        // Now instance B can acquire
        assertThat(instanceB.tryAcquire("my-stream")).isTrue();
    }

    @Test
    void release_doesNothingIfNotOwner() {
        instanceA.tryAcquire("my-stream");
        instanceB.release("my-stream"); // B doesn't own it — no-op

        // A still owns it
        assertThat(instanceA.renew("my-stream")).isTrue();
    }

    @Test
    void releaseAll_releasesAllLocksForInstance() {
        instanceA.tryAcquire("stream-1");
        instanceA.tryAcquire("stream-2");
        instanceB.tryAcquire("stream-3");

        instanceA.releaseAll();

        // B can now acquire stream-1 and stream-2
        assertThat(instanceB.tryAcquire("stream-1")).isTrue();
        assertThat(instanceB.tryAcquire("stream-2")).isTrue();
        // stream-3 still held by B
        assertThat(instanceA.tryAcquire("stream-3")).isFalse();
    }

    @Test
    void currentLeader_returnsLeaderId() {
        instanceA.tryAcquire("my-stream");
        assertThat(instanceA.currentLeader("my-stream")).hasValue("host-a:app:8080");
    }

    @Test
    void currentLeader_emptyIfNoLock() {
        assertThat(instanceA.currentLeader("nonexistent")).isEmpty();
    }

    @Test
    void multipleLocks_independentStreams() {
        assertThat(instanceA.tryAcquire("stream-1")).isTrue();
        assertThat(instanceB.tryAcquire("stream-2")).isTrue();

        // Each instance is leader for its own stream
        assertThat(instanceA.currentLeader("stream-1")).hasValue("host-a:app:8080");
        assertThat(instanceB.currentLeader("stream-2")).hasValue("host-b:app:8080");
    }
}
