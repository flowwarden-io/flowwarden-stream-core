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

import io.flowwarden.stream.spi.LockState;
import org.bson.Document;

import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * The {@code _fw_locks} document layout and the lease protocol shared by
 * {@link MongoLockService} (synchronous driver) and {@link ReactiveMongoLockService}
 * (reactive streams driver).
 *
 * <p>Both implementations build their filters and updates here, so an imperative
 * instance and a reactive instance contending for the same lease always speak the
 * same protocol: the lease is one document keyed by the stream name, owned by
 * {@code instanceId} until {@code expiresAt}, and every transition is a single
 * atomic server-side operation.</p>
 */
final class MongoLockDocuments {

    static final String COLLECTION = "_fw_locks";

    static final String ID = "_id";
    static final String INSTANCE_ID = "instanceId";
    static final String ACQUIRED_AT = "acquiredAt";
    static final String EXPIRES_AT = "expiresAt";

    /** Server code of a duplicate-key write error. */
    static final int DUPLICATE_KEY = 11000;

    private MongoLockDocuments() {}

    /**
     * Filter matching the lease of {@code streamName} when the caller may take it over:
     * it already owns it, or the lease has expired as of {@code now}.
     */
    static Document acquirableFilter(String streamName, String instanceId, Date now) {
        return new Document(ID, streamName)
                .append("$or", List.of(
                        new Document(INSTANCE_ID, instanceId),              // I already own it
                        new Document(EXPIRES_AT, new Document("$lt", now))  // Lease expired
                ));
    }

    /** Update stamping ownership and the new expiry on an acquirable lease. */
    static Document acquireUpdate(String instanceId, Date now, Date expiresAt) {
        return new Document("$set", new Document()
                .append(INSTANCE_ID, instanceId)
                .append(ACQUIRED_AT, now)
                .append(EXPIRES_AT, expiresAt));
    }

    /** The lease document inserted when no document exists yet for {@code streamName}. */
    static Document newLease(String streamName, String instanceId, Date now, Date expiresAt) {
        return new Document(ID, streamName)
                .append(INSTANCE_ID, instanceId)
                .append(ACQUIRED_AT, now)
                .append(EXPIRES_AT, expiresAt);
    }

    /**
     * Filter matching the lease of {@code streamName} while {@code instanceId} owns it,
     * expired or not — for {@code release}, which must still delete a lease its owner let
     * expire.
     */
    static Document ownedFilter(String streamName, String instanceId) {
        return new Document(ID, streamName).append(INSTANCE_ID, instanceId);
    }

    /**
     * Filter matching the lease of {@code streamName} only while {@code instanceId} owns it
     * <em>and</em> it is still active as of {@code now} — for {@code renew}: an expired lease
     * is no longer the caller's to extend, even if nobody has taken it over yet, so the
     * caller goes through the leadership-loss path and re-acquires explicitly.
     */
    static Document renewableFilter(String streamName, String instanceId, Date now) {
        return ownedFilter(streamName, instanceId).append(EXPIRES_AT, new Document("$gt", now));
    }

    /** Update extending the expiry of an owned lease. */
    static Document renewUpdate(Date expiresAt) {
        return new Document("$set", new Document(EXPIRES_AT, expiresAt));
    }

    /** Filter matching the lease of {@code streamName} only while it is still active. */
    static Document activeFilter(String streamName, Date now) {
        return new Document(ID, streamName).append(EXPIRES_AT, new Document("$gt", now));
    }

    /**
     * Reads a lease document back into a {@link LockState}; empty when the document is
     * missing or incomplete.
     */
    static Optional<LockState> toLockState(String streamName, Document doc) {
        if (doc == null) {
            return Optional.empty();
        }
        String instanceId = doc.getString(INSTANCE_ID);
        Date acquiredAt = doc.getDate(ACQUIRED_AT);
        Date expiresAt = doc.getDate(EXPIRES_AT);
        if (instanceId == null || acquiredAt == null || expiresAt == null) {
            return Optional.empty();
        }
        return Optional.of(new LockState(
                streamName,
                instanceId,
                acquiredAt.toInstant(),
                expiresAt.toInstant()));
    }
}
