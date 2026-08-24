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

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The publish-only path promised by the SPI: a backend that implements
 * {@code save} alone (the interface's single abstract method — a lambda
 * suffices) inherits honest empty reads. This test exercises the interface
 * defaults directly, independently of any shipped implementation's
 * overrides.
 */
class DlqStoreDefaultsTest {

    @Test
    void saveOnlyBackend_inheritsEmptyReads() {
        List<FailedEvent> published = new ArrayList<>();
        DlqStore publishOnly = (event, policy) -> published.add(event);

        var now = Instant.now();
        var event = new FailedEvent(
                "evt-1", "stream-a", "INSERT",
                null, null, null,
                new FailedEvent.ErrorInfo("Ex", "boom", null),
                1, FailedEvent.STATUS_PENDING,
                now, now, now, null, Collections.emptyMap()
        );
        publishOnly.save(event, new DlqPolicy(30, true, true));

        assertThat(published).containsExactly(event);
        assertThat(publishOnly.findById("evt-1")).isEmpty();
        assertThat(publishOnly.findByStreamName("stream-a")).isEmpty();
    }

    @Test
    void defaultReads_returnEmptyWithoutAnyPriorWrite() {
        DlqStore publishOnly = (event, policy) -> { };

        assertThat(publishOnly.findById("anything")).isEmpty();
        assertThat(publishOnly.findByStreamName("anything")).isEmpty();
    }

    @Test
    void defaultCount_isMinusOne_meaningCannotCount() {
        DlqStore publishOnly = (event, policy) -> { };

        // -1 = "this backend cannot serve counts" — distinct from the no-op
        // store's truthful 0. The runtime only emits a backlog gauge for
        // non-negative values.
        assertThat(publishOnly.count("anything")).isEqualTo(-1);
        assertThat(DlqStore.noOp().count("anything")).isZero();
    }
}
