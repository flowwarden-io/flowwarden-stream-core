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
package io.flowwarden.stream.internal.checkpoint;

import io.flowwarden.stream.DeploymentMode;
import io.flowwarden.stream.FullDocumentBeforeChangeMode;
import io.flowwarden.stream.FullDocumentMode;
import io.flowwarden.stream.internal.discovery.ChangeStreamDefinition;
import io.flowwarden.stream.internal.discovery.ErrorHandlerResolver;
import io.flowwarden.stream.internal.discovery.StreamConfig;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ChangeStreamProbeCommandsTest {

    private static ChangeStreamDefinition definition() {
        StreamConfig config = new StreamConfig(true, false, Document.class, "",
                FullDocumentMode.DEFAULT, FullDocumentBeforeChangeMode.OFF,
                DeploymentMode.ALL_INSTANCES);
        return new ChangeStreamDefinition("probe-commands", "probe_commands", "", "",
                new Object(), null, Map.of(), config, null, null,
                null, null, null, null,
                new ErrorHandlerResolver(List.of()), Map.of());
    }

    @Test
    void aggregateCommand_boundsCursorCreationWithMaxTimeMs() {
        Document command = ChangeStreamProbeCommands.aggregateCommand(definition(), null, null);

        // Probe aggregates run on lifecycle paths (bootstrap, post-invalidate
        // repair): the server must not execute the cursor creation
        // open-endedly. Server-side bound only — server selection and socket
        // reads are governed by the Mongo client's own timeouts.
        assertThat(command.getLong("maxTimeMS"))
                .isEqualTo(ChangeStreamProbeCommands.AGGREGATE_MAX_TIME_MS)
                .isEqualTo(5_000L);
    }

    @Test
    void aggregateCommand_resumeVariantsKeepTheBound() {
        Document resuming = ChangeStreamProbeCommands.aggregateCommand(definition(), null,
                BsonDocument.parse("{\"_data\": \"token\"}"));
        Document fromOplog = ChangeStreamProbeCommands.aggregateCommand(definition(), null,
                null, new BsonTimestamp(42, 1));

        assertThat(resuming.getLong("maxTimeMS"))
                .isEqualTo(ChangeStreamProbeCommands.AGGREGATE_MAX_TIME_MS);
        assertThat(fromOplog.getLong("maxTimeMS"))
                .isEqualTo(ChangeStreamProbeCommands.AGGREGATE_MAX_TIME_MS);
    }
}
