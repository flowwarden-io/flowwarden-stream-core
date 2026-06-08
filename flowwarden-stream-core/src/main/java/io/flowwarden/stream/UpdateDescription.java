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
package io.flowwarden.stream;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Describes the fields modified by an UPDATE operation on a MongoDB document.
 *
 * <p>Returned by {@link ChangeStreamContext#getUpdateDescription()} when the
 * operation type is {@link OperationType#UPDATE}.</p>
 */
public interface UpdateDescription {

    /**
     * Returns all fields that were updated and their new values.
     */
    Map<String, Object> getUpdatedFields();

    /**
     * Returns the dot-notation paths of fields that were removed.
     */
    List<String> getRemovedFields();

    /**
     * Returns arrays that were truncated during the update.
     */
    List<TruncatedArray> getTruncatedArrays();

    /**
     * Checks whether the given field was modified (updated or removed).
     *
     * @param path dot-notation field path (e.g. {@code "address.city"})
     */
    boolean hasFieldChanged(String path);

    /**
     * Returns the new value of a specific updated field.
     *
     * @param path dot-notation field path
     * @param type expected value type
     * @param <V>  value type
     * @return the value, or empty if the field was not updated
     */
    <V> Optional<V> getUpdatedFieldValue(String path, Class<V> type);
}
