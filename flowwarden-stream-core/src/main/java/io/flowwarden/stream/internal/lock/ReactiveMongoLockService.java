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

import com.mongodb.MongoWriteException;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.reactivestreams.client.MongoCollection;
import io.flowwarden.stream.spi.LockService;
import io.flowwarden.stream.spi.LockState;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;

/**
 * MongoDB-backed {@link LockService} on the reactive streams driver, used by default for
 * {@code DeploymentMode.SINGLE_LEADER} in REACTIVE mode.
 *
 * <p>Uses blocking {@code .block()} calls because the {@link LockService} SPI is
 * synchronous — the same bridge as {@code ReactiveMongoCheckpointStore}. Lock
 * operations are single-document round trips issued from the election coordinator's
 * own threads, never from a reactive pipeline, so blocking there is acceptable. A
 * pure WebFlux application therefore needs only
 * {@code spring-boot-starter-data-mongodb-reactive}: nothing here touches the
 * synchronous driver.</p>
 *
 * <p>Shares its document layout and lease protocol ({@link MongoLockDocuments}) with
 * {@link MongoLockService}: an imperative instance and a reactive instance can contend
 * for the same lease on the same {@code _fw_locks} collection, and exactly one wins.</p>
 */
public class ReactiveMongoLockService implements LockService {

    private static final Logger log = LoggerFactory.getLogger(ReactiveMongoLockService.class);

    static final String COLLECTION = MongoLockDocuments.COLLECTION;

    private final ReactiveMongoTemplate reactiveMongoTemplate;

    public ReactiveMongoLockService(ReactiveMongoTemplate reactiveMongoTemplate) {
        this.reactiveMongoTemplate = reactiveMongoTemplate;
    }

    @Override
    public boolean tryAcquire(String streamName, String instanceId, Duration ttl) {
        try {
            MongoCollection<Document> collection = getCollection();
            Date now = new Date();
            Date expiresAt = Date.from(Instant.now().plus(ttl));

            FindOneAndUpdateOptions options = new FindOneAndUpdateOptions()
                    .upsert(false)
                    .returnDocument(ReturnDocument.AFTER);

            Document result = Mono.from(collection.findOneAndUpdate(
                            MongoLockDocuments.acquirableFilter(streamName, instanceId, now),
                            MongoLockDocuments.acquireUpdate(instanceId, now, expiresAt),
                            options))
                    .block();

            if (result != null) {
                log.info("Acquired lock for stream '{}' (instanceId={})", streamName, instanceId);
                return true;
            }

            // Lock doesn't exist yet — try to insert it
            try {
                Mono.from(collection.insertOne(
                                MongoLockDocuments.newLease(streamName, instanceId, now, expiresAt)))
                        .block();
                log.info("Created and acquired lock for stream '{}' (instanceId={})", streamName, instanceId);
                return true;
            } catch (MongoWriteException e) {
                if (e.getCode() == MongoLockDocuments.DUPLICATE_KEY) {
                    // Duplicate key — another instance just created it
                    log.debug("Lock for stream '{}' already held by another instance", streamName);
                    return false;
                }
                throw e;
            }
        } catch (Exception e) {
            log.warn("Failed to acquire lock for stream '{}': {}", streamName, e.getMessage());
            return false;
        }
    }

    @Override
    public boolean renew(String streamName, String instanceId, Duration ttl) {
        try {
            MongoCollection<Document> collection = getCollection();
            Date now = new Date();
            Date expiresAt = Date.from(Instant.now().plus(ttl));

            Document result = Mono.from(collection.findOneAndUpdate(
                            MongoLockDocuments.renewableFilter(streamName, instanceId, now),
                            MongoLockDocuments.renewUpdate(expiresAt)))
                    .block();
            return result != null;
        } catch (Exception e) {
            log.warn("Failed to renew lock for stream '{}': {}", streamName, e.getMessage());
            return false;
        }
    }

    @Override
    public void release(String streamName, String instanceId) {
        try {
            MongoCollection<Document> collection = getCollection();
            Long deleted = Mono.from(collection.deleteOne(
                            MongoLockDocuments.ownedFilter(streamName, instanceId)))
                    .map(result -> result.getDeletedCount())
                    .block();
            if (deleted != null && deleted > 0) {
                log.info("Released lock for stream '{}' (instanceId={})", streamName, instanceId);
            }
        } catch (Exception e) {
            log.warn("Failed to release lock for stream '{}': {}", streamName, e.getMessage());
        }
    }

    @Override
    public Optional<LockState> getLockState(String streamName) {
        try {
            MongoCollection<Document> collection = getCollection();
            Document doc = Mono.from(collection
                            .find(MongoLockDocuments.activeFilter(streamName, new Date()))
                            .first())
                    .block();
            return MongoLockDocuments.toLockState(streamName, doc);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private MongoCollection<Document> getCollection() {
        MongoCollection<Document> collection = reactiveMongoTemplate.getCollection(COLLECTION).block();
        if (collection == null) {
            throw new IllegalStateException("ReactiveMongoTemplate returned no collection for " + COLLECTION);
        }
        return collection;
    }
}
