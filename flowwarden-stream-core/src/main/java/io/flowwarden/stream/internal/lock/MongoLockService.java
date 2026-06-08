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

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import io.flowwarden.stream.spi.LockService;
import io.flowwarden.stream.spi.LockState;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * MongoDB-backed {@link LockService}, used by default for
 * {@code DeploymentMode.SINGLE_LEADER}.
 *
 * <p>Uses an atomic {@code findOneAndUpdate} on the {@code _fw_locks} collection so only
 * one instance holds a lock for a given stream at any time. The collection name is internal
 * to this implementation — the SPI does not expose it.</p>
 */
public class MongoLockService implements LockService {

    private static final Logger log = LoggerFactory.getLogger(MongoLockService.class);

    static final String COLLECTION = "_fw_locks";

    private final MongoTemplate mongoTemplate;

    public MongoLockService(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public boolean tryAcquire(String streamName, String instanceId, Duration ttl) {
        try {
            MongoCollection<Document> collection = getCollection();
            Date now = new Date();
            Date expiresAt = Date.from(Instant.now().plus(ttl));

            Document filter = new Document("_id", streamName)
                    .append("$or", List.of(
                            new Document("instanceId", instanceId),          // I already own it
                            new Document("expiresAt", new Document("$lt", now))  // Lock expired
                    ));

            Document update = new Document("$set", new Document()
                    .append("instanceId", instanceId)
                    .append("acquiredAt", now)
                    .append("expiresAt", expiresAt));

            FindOneAndUpdateOptions options = new FindOneAndUpdateOptions()
                    .upsert(false)
                    .returnDocument(ReturnDocument.AFTER);

            Document result = collection.findOneAndUpdate(filter, update, options);

            if (result != null) {
                log.info("Acquired lock for stream '{}' (instanceId={})", streamName, instanceId);
                return true;
            }

            // Lock doesn't exist yet — try to insert it
            try {
                collection.insertOne(new Document("_id", streamName)
                        .append("instanceId", instanceId)
                        .append("acquiredAt", now)
                        .append("expiresAt", expiresAt));
                log.info("Created and acquired lock for stream '{}' (instanceId={})", streamName, instanceId);
                return true;
            } catch (com.mongodb.MongoWriteException e) {
                if (e.getCode() == 11000) {
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
            Date expiresAt = Date.from(Instant.now().plus(ttl));

            Document filter = new Document("_id", streamName)
                    .append("instanceId", instanceId);

            Document update = new Document("$set", new Document()
                    .append("expiresAt", expiresAt));

            Document result = collection.findOneAndUpdate(filter, update);
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
            Document filter = new Document("_id", streamName)
                    .append("instanceId", instanceId);
            long deleted = collection.deleteOne(filter).getDeletedCount();
            if (deleted > 0) {
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
            Document doc = collection.find(new Document("_id", streamName)
                            .append("expiresAt", new Document("$gt", new Date())))
                    .first();
            if (doc == null) {
                return Optional.empty();
            }
            String instanceId = doc.getString("instanceId");
            Date acquiredAt = doc.getDate("acquiredAt");
            Date expiresAt = doc.getDate("expiresAt");
            if (instanceId == null || acquiredAt == null || expiresAt == null) {
                return Optional.empty();
            }
            return Optional.of(new LockState(
                    streamName,
                    instanceId,
                    acquiredAt.toInstant(),
                    expiresAt.toInstant()));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private MongoCollection<Document> getCollection() {
        return mongoTemplate.getDb().getCollection(COLLECTION);
    }
}
