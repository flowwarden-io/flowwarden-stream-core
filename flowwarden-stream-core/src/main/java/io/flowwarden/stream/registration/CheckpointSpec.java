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
package io.flowwarden.stream.registration;

import io.flowwarden.stream.OnHistoryLost;
import io.flowwarden.stream.StartPosition;
import io.flowwarden.stream.annotation.Checkpoint;

import java.util.Objects;

/**
 * Plain-value equivalent of {@link Checkpoint}, for streams contributed via
 * {@link StreamDefinitionContributor} instead of annotated.
 *
 * <p>See {@link Checkpoint} for the meaning of each attribute — this type mirrors it 1:1,
 * including defaults, so behavior is identical regardless of how a stream was declared.
 * Built only through {@link #builder()}, not a canonical constructor, so a future attribute
 * addition doesn't break existing callers.</p>
 */
public final class CheckpointSpec {

    private final int saveEveryN;
    private final int saveIntervalSeconds;
    private final int idleHeartbeatIntervalSeconds;
    private final StartPosition startPosition;
    private final OnHistoryLost onHistoryLost;

    private CheckpointSpec(Builder builder) {
        this.saveEveryN = builder.saveEveryN;
        this.saveIntervalSeconds = builder.saveIntervalSeconds;
        this.idleHeartbeatIntervalSeconds = builder.idleHeartbeatIntervalSeconds;
        this.startPosition = builder.startPosition;
        this.onHistoryLost = builder.onHistoryLost;
    }

    public int saveEveryN() {
        return saveEveryN;
    }

    public int saveIntervalSeconds() {
        return saveIntervalSeconds;
    }

    public int idleHeartbeatIntervalSeconds() {
        return idleHeartbeatIntervalSeconds;
    }

    public StartPosition startPosition() {
        return startPosition;
    }

    public OnHistoryLost onHistoryLost() {
        return onHistoryLost;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Same defaults as {@link Checkpoint}'s unspecified attributes. */
    public static CheckpointSpec defaults() {
        return builder().build();
    }

    public static final class Builder {

        private int saveEveryN = 1;
        private int saveIntervalSeconds = 5;
        private int idleHeartbeatIntervalSeconds = 300;
        private StartPosition startPosition = StartPosition.RESUME;
        private OnHistoryLost onHistoryLost = OnHistoryLost.FAIL;

        private Builder() {
        }

        public Builder saveEveryN(int saveEveryN) {
            this.saveEveryN = saveEveryN;
            return this;
        }

        public Builder saveIntervalSeconds(int saveIntervalSeconds) {
            this.saveIntervalSeconds = saveIntervalSeconds;
            return this;
        }

        public Builder idleHeartbeatIntervalSeconds(int idleHeartbeatIntervalSeconds) {
            this.idleHeartbeatIntervalSeconds = idleHeartbeatIntervalSeconds;
            return this;
        }

        public Builder startPosition(StartPosition startPosition) {
            this.startPosition = Objects.requireNonNull(startPosition, "startPosition must not be null");
            return this;
        }

        public Builder onHistoryLost(OnHistoryLost onHistoryLost) {
            this.onHistoryLost = Objects.requireNonNull(onHistoryLost, "onHistoryLost must not be null");
            return this;
        }

        public CheckpointSpec build() {
            return new CheckpointSpec(this);
        }
    }
}
