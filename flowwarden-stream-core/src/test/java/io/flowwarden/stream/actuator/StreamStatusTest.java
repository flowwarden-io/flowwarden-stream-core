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
package io.flowwarden.stream.actuator;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StreamStatusTest {

    @Test
    void hasTwoValues() {
        assertThat(StreamStatus.values()).containsExactly(StreamStatus.UP, StreamStatus.DOWN);
    }

    @Test
    void upValueExists() {
        assertThat(StreamStatus.valueOf("UP")).isEqualTo(StreamStatus.UP);
    }

    @Test
    void downValueExists() {
        assertThat(StreamStatus.valueOf("DOWN")).isEqualTo(StreamStatus.DOWN);
    }
}
