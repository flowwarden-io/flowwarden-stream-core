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
package io.flowwarden.stream.internal;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.flowwarden.stream.ChangeStreamContext;
import io.flowwarden.stream.OperationType;
import io.flowwarden.stream.TransactionInfo;
import io.flowwarden.stream.UpdateDescription;
import org.bson.BsonDocument;
import org.bson.BsonDateTime;
import org.bson.BsonInt64;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.springframework.data.mongodb.core.convert.MongoConverter;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default implementation that maps from MongoDB's {@link ChangeStreamDocument}.
 *
 * <p>This class is internal and not part of the public API.</p>
 *
 * @param <T> the document type
 */
public class DefaultChangeStreamContext<T> implements ChangeStreamContext<T> {

    /**
     * Strategy interface for deferred action implementations.
     * The runtime (REQ-004/005) will inject real implementations.
     */
    public interface ContextActions {
        void sendToDlq(String reason);

        /**
         * Persists the current token as the processed anchor immediately.
         * Returns whether the write was <em>confirmed</em> — the context
         * only records a manual save that is actually durable, so a store
         * failure leaves the anchor dirty for the automatic policy to retry.
         */
        boolean saveCheckpointNow();
    }

    /** No-op actions for use when DLQ/checkpoint are not yet available. */
    public static final ContextActions NOOP_ACTIONS = new ContextActions() {
        @Override
        public void sendToDlq(String reason) { /* no-op */ }

        @Override
        public boolean saveCheckpointNow() {
            return false; // nothing was persisted
        }
    };

    private final String eventId;
    private final ChangeStreamDocument<Document> raw;
    private final String streamName;
    private volatile ContextActions actions;
    private final MongoConverter mongoConverter;
    private final Map<String, Object> metadata = new ConcurrentHashMap<>();
    private int attemptNumber = 1;
    private volatile boolean checkpointSavedManually = false;

    public DefaultChangeStreamContext(
            ChangeStreamDocument<Document> raw,
            String streamName,
            ContextActions actions,
            MongoConverter mongoConverter) {
        this.raw = Objects.requireNonNull(raw, "raw must not be null");
        this.streamName = Objects.requireNonNull(streamName, "streamName must not be null");
        this.actions = Objects.requireNonNull(actions, "actions must not be null");
        this.mongoConverter = mongoConverter;
        this.eventId = UUID.randomUUID().toString();
    }

    // ---- Event metadata ----

    @Override
    public String getEventId() {
        return eventId;
    }

    @Override
    public OperationType getOperationType() {
        return mapOperationType(raw.getOperationType());
    }

    @Override
    public String getStreamName() {
        return streamName;
    }

    @Override
    public String getCollectionName() {
        var ns = raw.getNamespace();
        return ns != null ? ns.getCollectionName() : null;
    }

    @Override
    public String getDatabaseName() {
        var ns = raw.getNamespace();
        return ns != null ? ns.getDatabaseName() : null;
    }

    @Override
    public Instant getClusterTime() {
        BsonTimestamp ts = raw.getClusterTime();
        return ts != null ? Instant.ofEpochSecond(ts.getTime()) : null;
    }

    @Override
    public Instant getWallTime() {
        BsonDateTime wt = raw.getWallTime();
        return wt != null ? Instant.ofEpochMilli(wt.getValue()) : null;
    }

    @Override
    public BsonDocument getResumeToken() {
        return raw.getResumeToken();
    }

    @Override
    public Optional<TransactionInfo> getTransactionInfo() {
        BsonDocument lsid = raw.getLsid();
        BsonInt64 txnNumber = raw.getTxnNumber();
        if (lsid == null || txnNumber == null) {
            return Optional.empty();
        }
        return Optional.of(new TransactionInfo(lsid, txnNumber.getValue()));
    }

    @Override
    public int getAttemptNumber() {
        return attemptNumber;
    }

    /**
     * Sets the current attempt number. Used internally by stream managers during retry.
     */
    public void setAttemptNumber(int attemptNumber) {
        this.attemptNumber = attemptNumber;
    }

    /**
     * Replaces the actions delegate. Used internally by stream managers to wire
     * actions that need a reference back to this context (e.g. sendToDlq).
     */
    public void setActions(ContextActions actions) {
        this.actions = Objects.requireNonNull(actions, "actions must not be null");
    }

    // ---- Document access ----

    @Override
    @SuppressWarnings("unchecked")
    public Optional<T> getFullDocument(Class<T> type) {
        Document doc = raw.getFullDocument();
        if (doc == null) {
            return Optional.empty();
        }
        return Optional.of(convertDocument(doc, type));
    }

    @Override
    @SuppressWarnings("unchecked")
    public Optional<T> getFullDocumentBeforeChange(Class<T> type) {
        Document doc = raw.getFullDocumentBeforeChange();
        if (doc == null) {
            return Optional.empty();
        }
        return Optional.of(convertDocument(doc, type));
    }

    @SuppressWarnings("unchecked")
    private T convertDocument(Document doc, Class<T> type) {
        if (type == Document.class) {
            return (T) doc;
        }
        if (mongoConverter != null) {
            return mongoConverter.read(type, doc);
        }
        return type.cast(doc);
    }

    @Override
    public Optional<UpdateDescription> getUpdateDescription() {
        var driverDesc = raw.getUpdateDescription();
        if (driverDesc == null) {
            return Optional.empty();
        }
        return Optional.of(new DefaultUpdateDescription(driverDesc));
    }

    @Override
    public BsonValue getDocumentKey() {
        return raw.getDocumentKey();
    }

    // ---- Actions ----

    @Override
    public void sendToDlq(String reason) {
        actions.sendToDlq(reason);
    }

    @Override
    public void saveCheckpointNow() {
        if (actions.saveCheckpointNow()) {
            // Only a CONFIRMED write counts as a manual save: a failed
            // write must leave the anchor dirty so the automatic policy
            // (count or time threshold) retries it.
            this.checkpointSavedManually = true;
        }
    }

    /**
     * Returns {@code true} if {@link #saveCheckpointNow()} was called during this event's processing.
     * Used internally by stream managers to skip the automatic checkpoint save.
     */
    public boolean isCheckpointSavedManually() {
        return checkpointSavedManually;
    }

    @Override
    public void addMetadata(String key, Object value) {
        metadata.put(key, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V> Optional<V> getMetadata(String key, Class<V> type) {
        Object value = metadata.get(key);
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(type.cast(value));
    }

    @Override
    public Map<String, Object> getAllMetadata() {
        return Collections.unmodifiableMap(metadata);
    }

    // ---- Summary ----

    @Override
    public String summary() {
        StringBuilder sb = new StringBuilder();

        // Line 1: [OPERATION] collection.docKey… | stream=name | time
        sb.append('[').append(getOperationType()).append("] ");
        sb.append(getCollectionName() != null ? getCollectionName() : "?");

        BsonValue docKey = getDocumentKey();
        if (docKey != null && docKey.isDocument()) {
            BsonValue id = docKey.asDocument().get("_id");
            if (id != null) {
                String idStr = id.isObjectId()
                        ? id.asObjectId().getValue().toHexString()
                        : id.toString();
                sb.append('.').append(idStr.length() > 12 ? idStr.substring(0, 12) + "…" : idStr);
            }
        }

        sb.append(" | stream=").append(streamName);

        Instant ct = getClusterTime();
        if (ct != null) {
            sb.append(" | ").append(ct);
        }

        // Line 2: document (if present)
        Document fullDoc = raw.getFullDocument();
        if (fullDoc != null) {
            sb.append("\n  document: ").append(fullDoc.toJson());
        }

        // Line 3: updated fields (if present)
        var driverDesc = raw.getUpdateDescription();
        if (driverDesc != null && driverDesc.getUpdatedFields() != null
                && !driverDesc.getUpdatedFields().isEmpty()) {
            sb.append("\n  updated: ").append(driverDesc.getUpdatedFields().keySet());
        }
        if (driverDesc != null && driverDesc.getRemovedFields() != null
                && !driverDesc.getRemovedFields().isEmpty()) {
            sb.append("\n  removed: ").append(driverDesc.getRemovedFields());
        }

        return sb.toString();
    }

    // ---- Internal helpers ----

    private static OperationType mapOperationType(
            com.mongodb.client.model.changestream.OperationType driverType) {
        if (driverType == null) {
            return null;
        }
        String value = driverType.getValue();
        return switch (value) {
            case "insert" -> OperationType.INSERT;
            case "update" -> OperationType.UPDATE;
            case "replace" -> OperationType.REPLACE;
            case "delete" -> OperationType.DELETE;
            case "drop" -> OperationType.DROP;
            case "dropDatabase" -> OperationType.DROP_DATABASE;
            case "rename" -> OperationType.RENAME;
            case "invalidate" -> OperationType.INVALIDATE;
            default -> throw new IllegalArgumentException(
                    "Unsupported MongoDB operation type: " + value);
        };
    }
}
