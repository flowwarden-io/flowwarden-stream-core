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
import io.flowwarden.stream.spi.LockService;
import io.flowwarden.stream.spi.testkit.LockServiceContractTest;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import java.time.Instant;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

class MongoLockServiceIntegrationTest extends LockServiceContractTest {

    private MongoTemplate mongoTemplate;

    @Override
    protected LockService createLockService() {
        mongoTemplate = new MongoTemplate(
                MongoClients.create(SharedMongoContainer.MONGO.getReplicaSetUrl()), "test-locks"
        );
        return new MongoLockService(mongoTemplate);
    }

    @Override
    protected void cleanState() {
        if (mongoTemplate == null) {
            mongoTemplate = new MongoTemplate(
                    MongoClients.create(SharedMongoContainer.MONGO.getReplicaSetUrl()), "test-locks"
            );
        }
        mongoTemplate.remove(new Query(), MongoLockService.COLLECTION);
    }

    @Test
    void tryAcquire_succeedsIfLockExpired() {
        // Acquire then manually expire by editing the document in place
        lockService.tryAcquire("my-stream", INSTANCE_A, TTL);
        mongoTemplate.getDb().getCollection(MongoLockService.COLLECTION)
                .updateOne(
                        new Document("_id", "my-stream"),
                        new Document("$set", new Document("expiresAt",
                                Date.from(Instant.now().minusSeconds(10)))));

        assertThat(lockService.tryAcquire("my-stream", INSTANCE_B, TTL)).isTrue();
    }

    @Test
    void renew_failsIfLockStolenByOtherInstance() {
        lockService.tryAcquire("my-stream", INSTANCE_A, TTL);

        // Manually change the owner in the document
        mongoTemplate.getDb().getCollection(MongoLockService.COLLECTION)
                .updateOne(
                        new Document("_id", "my-stream"),
                        new Document("$set", new Document("instanceId", INSTANCE_B)));

        assertThat(lockService.renew("my-stream", INSTANCE_A, TTL)).isFalse();
    }

    @Test
    void getLockState_emptyIfExpired() {
        lockService.tryAcquire("my-stream", INSTANCE_A, TTL);
        mongoTemplate.getDb().getCollection(MongoLockService.COLLECTION)
                .updateOne(
                        new Document("_id", "my-stream"),
                        new Document("$set", new Document("expiresAt",
                                Date.from(Instant.now().minusSeconds(10)))));

        assertThat(lockService.getLockState("my-stream")).isEmpty();
    }
}
