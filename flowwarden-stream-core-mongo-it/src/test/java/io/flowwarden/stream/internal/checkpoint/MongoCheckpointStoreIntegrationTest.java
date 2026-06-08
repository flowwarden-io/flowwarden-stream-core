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
package io.flowwarden.stream.internal.checkpoint;

import com.mongodb.client.MongoClients;
import io.flowwarden.stream.spi.Checkpoint;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.testkit.CheckpointStoreContractTest;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

class MongoCheckpointStoreIntegrationTest extends CheckpointStoreContractTest {

    private MongoTemplate mongoTemplate;

    @Override
    protected CheckpointStore createCheckpointStore() {
        return new MongoCheckpointStore(mongoTemplate);
    }

    @Override
    protected void cleanState() {
        mongoTemplate = new MongoTemplate(
                MongoClients.create(SharedMongoContainer.MONGO.getReplicaSetUrl()), "test"
        );
        mongoTemplate.remove(new Query(), MongoCheckpointStore.COLLECTION);
    }

    @Test
    void tokenRoundTrip() {
        var seenToken = BsonDocument.parse(
                "{\"_data\": \"8263B5F100000000012B022C0100296E5A100484C0\"}"
        );
        var processedToken = BsonDocument.parse(
                "{\"_data\": \"8263B5F200000000012B022C0100296E5A100484C1\"}"
        );
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        store.save(new Checkpoint("token-test", null, seenToken, now,
                processedToken, now, Collections.emptyMap()));

        var found = store.findByStreamName("token-test").orElseThrow();
        assertEquals(seenToken, found.lastSeenToken());
        assertEquals(processedToken, found.lastProcessedToken());
        assertNotSame(seenToken, found.lastSeenToken());
        assertNotSame(processedToken, found.lastProcessedToken());
    }
}
