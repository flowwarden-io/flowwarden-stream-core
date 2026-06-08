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
package io.flowwarden.stream.internal.dlq;

import com.mongodb.client.MongoClients;
import io.flowwarden.stream.spi.DlqStore;
import io.flowwarden.stream.spi.FailedEvent;
import io.flowwarden.stream.spi.testkit.DlqStoreContractTest;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.query.Query;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MongoDlqStoreIntegrationTest extends DlqStoreContractTest {

    private static final String DEFAULT_COLLECTION = "_fw_dlq";
    private static final String CUSTOM_COLLECTION = "orders_dlq";

    private MongoTemplate mongoTemplate;
    private MongoDlqStore mongoStore;

    @Override
    protected DlqStore createDlqStore() {
        return mongoStore;
    }

    @Override
    protected void cleanState() {
        mongoTemplate = new MongoTemplate(
                MongoClients.create(SharedMongoContainer.MONGO.getReplicaSetUrl()), "test"
        );
        mongoTemplate.remove(new Query(), DEFAULT_COLLECTION);
        mongoTemplate.remove(new Query(), CUSTOM_COLLECTION);
        mongoStore = new MongoDlqStore(mongoTemplate, new MongoDlqProperties());
    }

    @Test
    void registerStreamRoutesToCustomCollection() {
        mongoStore.registerStream("orders-stream", CUSTOM_COLLECTION);

        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        mongoStore.save(makeEvent("evt-orders", "orders-stream", now), DEFAULT_POLICY);

        assertThat(mongoTemplate.findAll(Document.class, CUSTOM_COLLECTION)).hasSize(1);
        assertThat(mongoTemplate.findAll(Document.class, DEFAULT_COLLECTION)).isEmpty();

        List<FailedEvent> found = mongoStore.findByStreamName("orders-stream");
        assertEquals(1, found.size());
        assertEquals("evt-orders", found.get(0).id());
    }

    @Test
    void registerStreamWithEmptyCollectionUsesDefault() {
        mongoStore.registerStream("default-stream", "");

        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        mongoStore.save(makeEvent("evt-default", "default-stream", now), DEFAULT_POLICY);

        assertThat(mongoTemplate.findAll(Document.class, DEFAULT_COLLECTION)).hasSize(1);
    }

    @Test
    void registerStreamCreatesTtlIndexOnExpiresAt() {
        mongoStore.registerStream("ttl-stream", CUSTOM_COLLECTION);

        IndexInfo ttlIndex = mongoTemplate.indexOps(CUSTOM_COLLECTION).getIndexInfo().stream()
                .filter(idx -> idx.getIndexFields().stream()
                        .anyMatch(f -> "expiresAt".equals(f.getKey())))
                .findFirst()
                .orElseThrow(() -> new AssertionError("TTL index on expiresAt was not created"));
        assertThat(ttlIndex.getExpireAfter()).isPresent();
    }
}
