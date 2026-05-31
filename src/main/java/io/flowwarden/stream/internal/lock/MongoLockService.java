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
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * MongoDB-based distributed lock service for SINGLE_LEADER deployment mode (ARCH-025).
 *
 * <p>Uses atomic {@code findOneAndUpdate} on the {@code _fw_locks} collection
 * to ensure only one instance holds a lock for a given stream at any time.</p>
 *
 * <p>Lock heartbeat (renewal) every 15 seconds, TTL 60 seconds.
 * If a leader fails to heartbeat, the lock expires and another instance can acquire it.</p>
 */
public class MongoLockService {

    private static final Logger log = LoggerFactory.getLogger(MongoLockService.class);

    static final String COLLECTION = "_fw_locks";
    static final long TTL_SECONDS = 60;

    private final MongoTemplate mongoTemplate;
    private final String instanceId;

    public MongoLockService(MongoTemplate mongoTemplate, String instanceId) {
        this.mongoTemplate = mongoTemplate;
        this.instanceId = instanceId;
    }

    /**
     * Attempts to acquire the lock for a stream.
     * Succeeds if the lock doesn't exist, has expired, or is already owned by this instance.
     *
     * @param streamName the stream to lock
     * @return true if this instance now holds the lock
     */
    public boolean tryAcquire(String streamName) {
        try {
            MongoCollection<Document> collection = getCollection();
            Date now = new Date();
            Date expiresAt = Date.from(Instant.now().plus(TTL_SECONDS, ChronoUnit.SECONDS));

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

    /**
     * Renews the lock for a stream. Only succeeds if this instance still holds it.
     *
     * @param streamName the stream to renew
     * @return true if renewal succeeded
     */
    public boolean renew(String streamName) {
        try {
            MongoCollection<Document> collection = getCollection();
            Date expiresAt = Date.from(Instant.now().plus(TTL_SECONDS, ChronoUnit.SECONDS));

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

    /**
     * Releases the lock for a stream if held by this instance.
     *
     * @param streamName the stream to release
     */
    public void release(String streamName) {
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

    /**
     * Releases all locks held by this instance. Called during graceful shutdown.
     */
    public void releaseAll() {
        try {
            MongoCollection<Document> collection = getCollection();
            Document filter = new Document("instanceId", instanceId);
            long deleted = collection.deleteMany(filter).getDeletedCount();
            if (deleted > 0) {
                log.info("Released {} lock(s) during shutdown (instanceId={})", deleted, instanceId);
            }
        } catch (Exception e) {
            log.warn("Failed to release locks during shutdown: {}", e.getMessage());
        }
    }

    /**
     * Returns the current leader for a stream, if any.
     *
     * @param streamName the stream to check
     * @return the leader's instance ID, or empty if no active lock
     */
    public Optional<String> currentLeader(String streamName) {
        try {
            MongoCollection<Document> collection = getCollection();
            Document doc = collection.find(new Document("_id", streamName)
                            .append("expiresAt", new Document("$gt", new Date())))
                    .first();
            return doc != null ? Optional.ofNullable(doc.getString("instanceId")) : Optional.empty();
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public String getInstanceId() {
        return instanceId;
    }

    private MongoCollection<Document> getCollection() {
        return mongoTemplate.getDb().getCollection(COLLECTION);
    }
}
