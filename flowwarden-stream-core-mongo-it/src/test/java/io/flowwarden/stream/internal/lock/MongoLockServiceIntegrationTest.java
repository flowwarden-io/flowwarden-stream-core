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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.flowwarden.stream.spi.LockService;
import io.flowwarden.stream.spi.testkit.LockServiceContractTest;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import java.time.Instant;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

class MongoLockServiceIntegrationTest extends LockServiceContractTest {

    private MongoClient mongoClient;
    private MongoTemplate mongoTemplate;

    @Override
    protected LockService createLockService() {
        ensureTemplate();
        return new MongoLockService(mongoTemplate);
    }

    @Override
    protected void cleanState() {
        ensureTemplate();
        mongoTemplate.remove(new Query(), MongoLockService.COLLECTION);
    }

    @Override
    protected boolean expireLeaseInPlace(String streamName) {
        expireInPlace(streamName);
        return true;
    }

    @AfterEach
    void closeClient() {
        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
            mongoTemplate = null;
        }
    }

    private void ensureTemplate() {
        if (mongoTemplate == null) {
            mongoClient = MongoClients.create(SharedMongoContainer.MONGO.getReplicaSetUrl());
            mongoTemplate = new MongoTemplate(mongoClient, "test-locks");
        }
    }

    @Test
    void tryAcquire_succeedsIfLockExpired() {
        // Acquire then manually expire by editing the document in place
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

    private void expireInPlace(String streamName) {
        updateInPlace(streamName,
                new Document("expiresAt", Date.from(Instant.now().minusSeconds(10))));
    }

    private void updateInPlace(String streamName, Document fields) {
        mongoTemplate.getDb().getCollection(MongoLockService.COLLECTION)
                .updateOne(new Document("_id", streamName), new Document("$set", fields));
    }

    private Date storedExpiresAt(String streamName) {
        Document doc = mongoTemplate.getDb().getCollection(MongoLockService.COLLECTION)
                .find(new Document("_id", streamName)).first();
        assertThat(doc).isNotNull();
        return doc.getDate("expiresAt");
    }
}
