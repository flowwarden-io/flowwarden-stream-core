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

import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.DlqStore;
import io.flowwarden.stream.spi.LockService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;

import java.util.function.Function;

/**
 * Logs, once at startup, where each kind of FlowWarden state lives:
 *
 * <pre>
 * FlowWarden state: checkpoints=mongo[stateDb] dlq=mongo[mainDb] locks=mongo[stateDb]
 * </pre>
 *
 * <p>The three state beans are independent and each can be redeclared on its own;
 * redeclaring one and forgetting the others silently splits the state across two
 * backends or two databases. This line makes the split visible, whatever the
 * backend: each value is the bean's own {@code backendDescription()}.</p>
 *
 * <p>Purely informational and never blocking: a store that throws while describing
 * itself is reported as {@code ?}, a role without a single candidate bean as
 * {@code none}.</p>
 */
public class StateBackendReporter implements SmartInitializingSingleton {

    private static final Logger log = LoggerFactory.getLogger(StateBackendReporter.class);

    private final ObjectProvider<CheckpointStore> checkpointStore;
    private final ObjectProvider<DlqStore> dlqStore;
    private final ObjectProvider<LockService> lockService;

    public StateBackendReporter(ObjectProvider<CheckpointStore> checkpointStore,
                                ObjectProvider<DlqStore> dlqStore,
                                ObjectProvider<LockService> lockService) {
        this.checkpointStore = checkpointStore;
        this.dlqStore = dlqStore;
        this.lockService = lockService;
    }

    @Override
    public void afterSingletonsInstantiated() {
        try {
            log.info("FlowWarden state: {}", describe());
        } catch (RuntimeException e) {
            // Informational only: never a reason to fail the startup.
            log.debug("Could not describe the FlowWarden state backends", e);
        }
    }

    /** The {@code checkpoints=… dlq=… locks=…} part of the line. */
    public String describe() {
        return "checkpoints=" + describe(checkpointStore, CheckpointStore::backendDescription)
                + " dlq=" + describe(dlqStore, DlqStore::backendDescription)
                + " locks=" + describe(lockService, LockService::backendDescription);
    }

    private static <T> String describe(ObjectProvider<T> provider, Function<T, String> description) {
        try {
            T bean = provider.getIfUnique();
            if (bean == null) {
                return "none";
            }
            String value = description.apply(bean);
            return value == null || value.isBlank() ? "?" : value.strip();
        } catch (RuntimeException e) {
            log.debug("A FlowWarden state backend failed to describe itself", e);
            return "?";
        }
    }
}
