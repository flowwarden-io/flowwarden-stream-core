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
package io.flowwarden.stream.autoconfigure;

import io.flowwarden.stream.internal.checkpoint.MongoCheckpointStore;
import io.flowwarden.stream.internal.discovery.StreamRegistry;
import io.flowwarden.stream.internal.dlq.MongoDlqStore;
import io.flowwarden.stream.internal.MongoTemplateRegistry;
import io.flowwarden.stream.internal.imperative.ImperativeStreamManager;
import io.flowwarden.stream.internal.lock.LeaderElectionCoordinator;
import io.flowwarden.stream.internal.lock.MongoLockService;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.DlqStore;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.net.InetAddress;

/**
 * Auto-configuration for FlowWarden in IMPERATIVE mode.
 *
 * <p>Activated when {@code flowwarden.default-mode=IMPERATIVE}.</p>
 */
@AutoConfiguration(after = FlowWardenAutoConfiguration.class)
@ConditionalOnProperty(name = "flowwarden.default-mode", havingValue = "IMPERATIVE", matchIfMissing = true)
public class ImperativeFlowWardenAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public CheckpointStore checkpointStore(MongoTemplate mongoTemplate) {
        return new MongoCheckpointStore(mongoTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    public DlqStore dlqStore(MongoTemplate mongoTemplate) {
        return new MongoDlqStore(mongoTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    public MongoLockService mongoLockService(MongoTemplate mongoTemplate, Environment env) {
        String instanceId = resolveInstanceId(env);
        return new MongoLockService(mongoTemplate, instanceId);
    }

    @Bean
    @ConditionalOnMissingBean
    public LeaderElectionCoordinator leaderElectionCoordinator(MongoLockService lockService) {
        return new LeaderElectionCoordinator(lockService);
    }

    @Bean
    @ConditionalOnMissingBean
    public MongoTemplateRegistry mongoTemplateRegistry(ApplicationContext applicationContext,
                                                       MongoTemplate mongoTemplate) {
        return new MongoTemplateRegistry(applicationContext, mongoTemplate, null);
    }

    @Bean
    public ImperativeStreamManager imperativeStreamManager(
            MongoTemplateRegistry mongoTemplateRegistry,
            StreamRegistry registry,
            CheckpointStore checkpointStore,
            DlqStore dlqStore,
            LeaderElectionCoordinator leaderElectionCoordinator) {
        return new ImperativeStreamManager(mongoTemplateRegistry, registry, checkpointStore, dlqStore,
                leaderElectionCoordinator);
    }

    /**
     * Resolves the instance ID following the ARCH-037 pattern: hostname:appName:port.
     */
    static String resolveInstanceId(Environment env) {
        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            hostname = "unknown";
        }
        String appName = env.getProperty("spring.application.name", "unknown");
        String port = env.getProperty("server.port", "8080");
        return hostname + ":" + appName + ":" + port;
    }
}
