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
package io.flowwarden.stream.spi;

import org.bson.BsonDocument;

import java.time.Instant;
import java.util.Optional;

/**
 * Default no-op implementation of {@link CheckpointStore}.
 * All methods silently do nothing.
 */
final class NoOpCheckpointStore implements CheckpointStore {

    static final NoOpCheckpointStore INSTANCE = new NoOpCheckpointStore();

    private NoOpCheckpointStore() {
    }

    @Override
    public void save(Checkpoint checkpoint) {
    }

    @Override
    public Optional<Checkpoint> findByStreamName(String streamName) {
        return Optional.empty();
    }

    @Override
    public void saveSeen(String streamName, BsonDocument token, Instant timestamp) {
    }

    @Override
    public void saveSeen(String streamName, BsonDocument token, Instant timestamp,
                         Instant heartbeatTimestamp) {
    }

    @Override
    public void saveHeartbeat(String streamName, Instant heartbeatTimestamp) {
    }

    @Override
    public void saveProcessed(String streamName, BsonDocument token, Instant timestamp) {
    }

    @Override
    public void delete(String streamName) {
    }
}
