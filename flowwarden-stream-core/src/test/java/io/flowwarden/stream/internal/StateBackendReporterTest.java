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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.flowwarden.stream.spi.Checkpoint;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.DlqPolicy;
import io.flowwarden.stream.spi.DlqStore;
import io.flowwarden.stream.spi.FailedEvent;
import io.flowwarden.stream.spi.LockService;
import io.flowwarden.stream.spi.LockState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class StateBackendReporterTest {

    private final Logger logger = (Logger) LoggerFactory.getLogger(StateBackendReporter.class);
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();
    private final DefaultListableBeanFactory beans = new DefaultListableBeanFactory();

    @BeforeEach
    void attach() {
        appender.start();
        logger.addAppender(appender);
    }

    @AfterEach
    void detach() {
        logger.detachAppender(appender);
    }

    private StateBackendReporter reporter() {
        return new StateBackendReporter(
                beans.getBeanProvider(CheckpointStore.class),
                beans.getBeanProvider(DlqStore.class),
                beans.getBeanProvider(LockService.class));
    }

    private List<String> infoLines() {
        return appender.list.stream()
                .filter(e -> e.getLevel() == Level.INFO)
                .map(ILoggingEvent::getFormattedMessage)
                .toList();
    }

    @Test
    void logsOneLineNamingWhereEachRoleLives() {
        beans.registerSingleton("checkpointStore", new DescribedCheckpointStore("mongo[stateDb]"));
        beans.registerSingleton("dlqStore", new DescribedDlqStore("mongo[mainDb]"));
        beans.registerSingleton("lockService", new DescribedLockService("mongo[stateDb]"));

        reporter().afterSingletonsInstantiated();

        assertThat(infoLines()).containsExactly(
                "FlowWarden state: checkpoints=mongo[stateDb] dlq=mongo[mainDb] locks=mongo[stateDb]");
    }

    @Test
    void aBackendThatDoesNotDescribeItselfIsNamedByItsClass() {
        beans.registerSingleton("checkpointStore", new ThirdPartyCheckpointStore());
        beans.registerSingleton("dlqStore", new DescribedDlqStore("amqp[fw.dlq]"));
        beans.registerSingleton("lockService", new DescribedLockService("redis[fw:]"));

        assertThat(reporter().describe())
                .isEqualTo("checkpoints=ThirdPartyCheckpointStore dlq=amqp[fw.dlq] locks=redis[fw:]");
    }

    @Test
    void noOpBackendsAreReadable() {
        beans.registerSingleton("checkpointStore", CheckpointStore.noOp());
        beans.registerSingleton("dlqStore", DlqStore.noOp());
        beans.registerSingleton("lockService", LockService.noOp());

        assertThat(reporter().describe())
                .isEqualTo("checkpoints=none (no-op) dlq=none (no-op) locks=none (no-op)");
    }

    @Test
    void aRoleWithoutABeanIsReportedAsNone() {
        beans.registerSingleton("checkpointStore", new DescribedCheckpointStore("mongo[db]"));

        assertThat(reporter().describe()).isEqualTo("checkpoints=mongo[db] dlq=none locks=none");
    }

    @Test
    void aBackendThatThrowsOrAnswersNothingNeverBreaksTheStartup() {
        beans.registerSingleton("checkpointStore", new DescribedCheckpointStore(null) {
            @Override
            public String backendDescription() {
                throw new IllegalStateException("backend unreachable");
            }
        });
        beans.registerSingleton("dlqStore", new DescribedDlqStore(null));
        beans.registerSingleton("lockService", new DescribedLockService("  "));

        StateBackendReporter reporter = reporter();

        assertThatCode(reporter::afterSingletonsInstantiated).doesNotThrowAnyException();
        assertThat(infoLines()).containsExactly("FlowWarden state: checkpoints=? dlq=? locks=?");
    }

    // --- fixtures ---

    static class ThirdPartyCheckpointStore implements CheckpointStore {
        @Override
        public void save(Checkpoint checkpoint) {
        }

        @Override
        public Optional<Checkpoint> findByStreamName(String streamName) {
            return Optional.empty();
        }

        @Override
        public void delete(String streamName) {
        }
    }

    static class DescribedCheckpointStore extends ThirdPartyCheckpointStore {
        private final String description;

        DescribedCheckpointStore(String description) {
            this.description = description;
        }

        @Override
        public String backendDescription() {
            return description;
        }
    }

    static class DescribedDlqStore implements DlqStore {
        private final String description;

        DescribedDlqStore(String description) {
            this.description = description;
        }

        @Override
        public void save(FailedEvent event, DlqPolicy policy) {
        }

        @Override
        public String backendDescription() {
            return description;
        }
    }

    static class DescribedLockService implements LockService {
        private final String description;

        DescribedLockService(String description) {
            this.description = description;
        }

        @Override
        public boolean tryAcquire(String streamName, String instanceId, Duration ttl) {
            return true;
        }

        @Override
        public boolean renew(String streamName, String instanceId, Duration ttl) {
            return true;
        }

        @Override
        public void release(String streamName, String instanceId) {
        }

        @Override
        public Optional<LockState> getLockState(String streamName) {
            return Optional.empty();
        }

        @Override
        public String backendDescription() {
            return description;
        }
    }
}
