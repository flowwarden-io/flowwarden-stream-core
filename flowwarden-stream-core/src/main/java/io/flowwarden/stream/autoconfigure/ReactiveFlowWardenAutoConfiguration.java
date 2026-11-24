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

import io.flowwarden.stream.mongo.checkpoint.ReactiveMongoCheckpointStore;
import io.flowwarden.stream.internal.discovery.StreamRegistry;
import io.flowwarden.stream.mongo.dlq.MongoDlqProperties;
import io.flowwarden.stream.mongo.dlq.ReactiveMongoDlqStore;
import io.flowwarden.stream.internal.MongoTemplateRegistry;
import io.flowwarden.stream.internal.lock.LeaderElectionCoordinator;
import io.flowwarden.stream.mongo.lock.ReactiveMongoLockService;
import io.flowwarden.stream.internal.reactive.ReactiveStreamManager;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.DlqStore;
import io.flowwarden.stream.spi.LockService;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;

/**
 * Auto-configuration for FlowWarden in REACTIVE mode.
 *
 * <p>Activated when {@code flowwarden.default-mode=REACTIVE}. Every default bean is
 * built on {@link ReactiveMongoTemplate}: the synchronous Mongo driver is not required.</p>
 *
 * <p>The three state beans — {@link CheckpointStore}, {@link DlqStore} and
 * {@link LockService} — are built on the application's default
 * {@code ReactiveMongoTemplate}: the FlowWarden state ({@code _fw_checkpoints},
 * {@code _fw_dlq}, {@code _fw_locks}) lives in that database.
 * {@code @ChangeStream(mongoTemplateRef = …)} selects the template a stream
 * <em>watches</em> and does not move the state. Each bean is
 * {@code @ConditionalOnMissingBean}: an application whose state belongs in another
 * database declares the three beans in its own {@code @Configuration}, on the
 * template of that database:</p>
 *
 * <pre>{@code
 * @Bean
 * CheckpointStore checkpointStore(@Qualifier("stateMongoTemplate") ReactiveMongoTemplate template) {
 *     return new ReactiveMongoCheckpointStore(template);
 * }
 *
 * @Bean
 * DlqStore dlqStore(@Qualifier("stateMongoTemplate") ReactiveMongoTemplate template,
 *                   MongoDlqProperties properties) {
 *     return new ReactiveMongoDlqStore(template, properties);
 * }
 *
 * @Bean
 * LockService lockService(@Qualifier("stateMongoTemplate") ReactiveMongoTemplate template) {
 *     return new ReactiveMongoLockService(template);
 * }
 * }</pre>
 *
 * <p>Spring Boot's own {@code ReactiveMongoTemplate} backs off as soon as the application
 * declares one, so an application with two databases declares both templates and
 * marks the one FlowWarden watches by default {@code @Primary}.</p>
 *
 * <p>The beans are independent: redeclaring only one of them splits the state
 * across two databases. The {@code FlowWarden state: checkpoints=… dlq=… locks=…}
 * line logged at startup shows where each one lives. When the state and a
 * handler's writes go through different database factories, they do not share a
 * transaction.</p>
 */
@AutoConfiguration(after = FlowWardenAutoConfiguration.class)
@ConditionalOnProperty(name = "flowwarden.default-mode", havingValue = "REACTIVE")
@EnableConfigurationProperties(MongoDlqProperties.class)
public class ReactiveFlowWardenAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public CheckpointStore checkpointStore(ReactiveMongoTemplate reactiveMongoTemplate) {
        return new ReactiveMongoCheckpointStore(reactiveMongoTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    public DlqStore dlqStore(ReactiveMongoTemplate reactiveMongoTemplate,
                             MongoDlqProperties dlqProperties) {
        return new ReactiveMongoDlqStore(reactiveMongoTemplate, dlqProperties);
    }

    @Bean
    @ConditionalOnMissingBean
    public LockService lockService(ReactiveMongoTemplate reactiveMongoTemplate) {
        // Built on the reactive driver like the checkpoint and DLQ stores, so a pure
        // WebFlux application needs no synchronous Mongo driver on its classpath.
        return new ReactiveMongoLockService(reactiveMongoTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    public LeaderElectionCoordinator leaderElectionCoordinator(LockService lockService, Environment env) {
        return new LeaderElectionCoordinator(lockService,
                ImperativeFlowWardenAutoConfiguration.resolveInstanceId(env));
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
