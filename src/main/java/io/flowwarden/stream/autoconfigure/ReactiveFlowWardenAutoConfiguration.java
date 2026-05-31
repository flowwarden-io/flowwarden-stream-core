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

import io.flowwarden.stream.internal.checkpoint.ReactiveMongoCheckpointStore;
import io.flowwarden.stream.internal.discovery.StreamRegistry;
import io.flowwarden.stream.internal.dlq.ReactiveMongoDlqStore;
import io.flowwarden.stream.internal.MongoTemplateRegistry;
import io.flowwarden.stream.internal.lock.LeaderElectionCoordinator;
import io.flowwarden.stream.internal.lock.MongoLockService;
import io.flowwarden.stream.internal.reactive.ReactiveStreamManager;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.DlqStore;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;

/**
 * Auto-configuration for FlowWarden in REACTIVE mode.
 *
 * <p>Activated when {@code flowwarden.default-mode=REACTIVE}.</p>
 */
@AutoConfiguration(after = FlowWardenAutoConfiguration.class)
@ConditionalOnProperty(name = "flowwarden.default-mode", havingValue = "REACTIVE")
public class ReactiveFlowWardenAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public CheckpointStore checkpointStore(ReactiveMongoTemplate reactiveMongoTemplate) {
        return new ReactiveMongoCheckpointStore(reactiveMongoTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    public DlqStore dlqStore(ReactiveMongoTemplate reactiveMongoTemplate) {
        return new ReactiveMongoDlqStore(reactiveMongoTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    public MongoLockService mongoLockService(MongoDatabaseFactory factory, Environment env) {
        // Lock service needs blocking MongoTemplate — create one from the shared factory
        String instanceId = ImperativeFlowWardenAutoConfiguration.resolveInstanceId(env);
        return new MongoLockService(new MongoTemplate(factory), instanceId);
    }

    @Bean
    @ConditionalOnMissingBean
    public LeaderElectionCoordinator leaderElectionCoordinator(MongoLockService lockService) {
        return new LeaderElectionCoordinator(lockService);
    }

    @Bean
    @ConditionalOnMissingBean
    public MongoTemplateRegistry mongoTemplateRegistry(ApplicationContext applicationContext,
                                                       ReactiveMongoTemplate reactiveMongoTemplate) {
        return new MongoTemplateRegistry(applicationContext, null, reactiveMongoTemplate);
    }

    @Bean
    public ReactiveStreamManager reactiveStreamManager(
            MongoTemplateRegistry mongoTemplateRegistry,
            StreamRegistry registry,
            CheckpointStore checkpointStore,
            DlqStore dlqStore,
            LeaderElectionCoordinator leaderElectionCoordinator) {
        return new ReactiveStreamManager(mongoTemplateRegistry, registry, checkpointStore, dlqStore,
                leaderElectionCoordinator);
    }
}
