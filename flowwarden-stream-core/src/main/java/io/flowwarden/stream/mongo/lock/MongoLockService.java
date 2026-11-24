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
package io.flowwarden.stream.mongo.lock;

import com.mongodb.MongoWriteException;
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
import java.util.Optional;

/**
 * MongoDB-backed {@link LockService} on the synchronous driver, used by default for
 * {@code DeploymentMode.SINGLE_LEADER} in IMPERATIVE mode.
 *
 * <p>Uses an atomic {@code findOneAndUpdate} on the {@code _fw_locks} collection so only
 * one instance holds a lock for a given stream at any time. The collection lives in the
 * database of the {@link MongoTemplate} this service is built on; its name is a detail of
 * this implementation — the SPI does not expose it.</p>
 *
 * <p>Shares its document layout and lease protocol with {@link ReactiveMongoLockService}:
 * instances of both implementations can contend for the same lease on the same
 * collection. Every instance of an application must therefore build its lock service on
 * the same database — two instances looking at two databases would each elect a leader.</p>
 *
 * <p>Public API. The auto-configuration builds this service on the application's
 * default {@code MongoTemplate}; to keep the FlowWarden state in another database,
 * declare the bean yourself on the template of that database — the
 * auto-configured one backs off:</p>
 *
 * <pre>{@code
 * @Bean
 * LockService lockService(@Qualifier("stateMongoTemplate") MongoTemplate template) {
 *     return new MongoLockService(template);
 * }
 * }</pre>
 *
 * <p>The three state beans ({@code CheckpointStore}, {@code DlqStore},
 * {@code LockService}) are independent: redeclare all three to move the whole
 * state, and check the {@code FlowWarden state: …} line logged at startup.
 * {@code @ChangeStream(mongoTemplateRef = …)} selects the template a stream
 * <em>watches</em>; it does not move the state.</p>
 */
public class MongoLockService implements LockService {

    private static final Logger log = LoggerFactory.getLogger(MongoLockService.class);

    static final String COLLECTION = MongoLockDocuments.COLLECTION;

    private final MongoTemplate mongoTemplate;

    /**
     * @param mongoTemplate the template whose database receives {@code _fw_locks}
     */
    public MongoLockService(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    /**
     * {@code mongo[<database>]}: the database of the template this service was built on.
     */
    @Override
    public String backendDescription() {
        try {
            return "mongo[" + mongoTemplate.getDb().getName() + "]";
        } catch (RuntimeException e) {
            return "mongo[?]";
        }
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

            Document result = collection.findOneAndUpdate(
                    MongoLockDocuments.acquirableFilter(streamName, instanceId, now),
                    MongoLockDocuments.acquireUpdate(instanceId, now, expiresAt),
                    options);

            if (result != null) {
                log.info("Acquired lock for stream '{}' (instanceId={})", streamName, instanceId);
                return true;
            }

            // Lock doesn't exist yet — try to insert it
            try {
                collection.insertOne(MongoLockDocuments.newLease(streamName, instanceId, now, expiresAt));
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

            Document result = collection.findOneAndUpdate(
                    MongoLockDocuments.renewableFilter(streamName, instanceId, now),
                    MongoLockDocuments.renewUpdate(expiresAt));
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
            long deleted = collection
                    .deleteOne(MongoLockDocuments.ownedFilter(streamName, instanceId))
                    .getDeletedCount();
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
            Document doc = collection
                    .find(MongoLockDocuments.activeFilter(streamName, new Date()))
                    .first();
            return MongoLockDocuments.toLockState(streamName, doc);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private MongoCollection<Document> getCollection() {
        return mongoTemplate.getDb().getCollection(COLLECTION);
    }
}
