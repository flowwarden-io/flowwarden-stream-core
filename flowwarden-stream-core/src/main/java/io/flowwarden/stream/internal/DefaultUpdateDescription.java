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

import io.flowwarden.stream.TruncatedArray;
import io.flowwarden.stream.UpdateDescription;
import org.bson.BsonValue;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Default implementation that maps from the MongoDB driver's
 * {@link com.mongodb.client.model.changestream.UpdateDescription}.
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public class DefaultUpdateDescription implements UpdateDescription {

    private final Map<String, Object> updatedFields;
    private final List<String> removedFields;
    private final List<TruncatedArray> truncatedArrays;

    public DefaultUpdateDescription(
            com.mongodb.client.model.changestream.UpdateDescription driverDescription) {
        Objects.requireNonNull(driverDescription, "driverDescription must not be null");

        this.updatedFields = mapUpdatedFields(driverDescription);
        this.removedFields = driverDescription.getRemovedFields() != null
                ? List.copyOf(driverDescription.getRemovedFields())
                : List.of();
        this.truncatedArrays = mapTruncatedArrays(driverDescription);
    }

    @Override
    public Map<String, Object> getUpdatedFields() {
        return updatedFields;
    }

    @Override
    public List<String> getRemovedFields() {
        return removedFields;
    }

    @Override
    public List<TruncatedArray> getTruncatedArrays() {
        return truncatedArrays;
    }

    @Override
    public boolean hasFieldChanged(String path) {
        return updatedFields.containsKey(path) || removedFields.contains(path);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V> Optional<V> getUpdatedFieldValue(String path, Class<V> type) {
        Object value = updatedFields.get(path);
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(type.cast(value));
    }

    private static Map<String, Object> mapUpdatedFields(
            com.mongodb.client.model.changestream.UpdateDescription desc) {
        if (desc.getUpdatedFields() == null) {
            return Map.of();
        }
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<String, BsonValue> entry : desc.getUpdatedFields().entrySet()) {
            result.put(entry.getKey(), bsonToObject(entry.getValue()));
        }
        return Collections.unmodifiableMap(result);
    }

    private static List<TruncatedArray> mapTruncatedArrays(
            com.mongodb.client.model.changestream.UpdateDescription desc) {
        if (desc.getTruncatedArrays() == null) {
            return List.of();
        }
        return desc.getTruncatedArrays().stream()
                .map(ta -> new TruncatedArray(ta.getField(), ta.getNewSize()))
                .toList();
    }

    private static Object bsonToObject(BsonValue bsonValue) {
        if (bsonValue == null || bsonValue.isNull()) {
            return null;
        }
        if (bsonValue.isString()) {
            return bsonValue.asString().getValue();
        }
        if (bsonValue.isInt32()) {
            return bsonValue.asInt32().getValue();
        }
        if (bsonValue.isInt64()) {
            return bsonValue.asInt64().getValue();
        }
        if (bsonValue.isDouble()) {
            return bsonValue.asDouble().getValue();
        }
        if (bsonValue.isBoolean()) {
            return bsonValue.asBoolean().getValue();
        }
        // Fallback: return the raw BsonValue
        return bsonValue;
    }
}
