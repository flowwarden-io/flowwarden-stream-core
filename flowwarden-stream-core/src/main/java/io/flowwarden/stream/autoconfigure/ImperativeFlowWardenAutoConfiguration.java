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

import io.flowwarden.stream.mongo.checkpoint.MongoCheckpointStore;
import io.flowwarden.stream.internal.discovery.StreamRegistry;
import io.flowwarden.stream.mongo.dlq.MongoDlqProperties;
import io.flowwarden.stream.mongo.dlq.MongoDlqStore;
import io.flowwarden.stream.internal.MongoTemplateRegistry;
import io.flowwarden.stream.internal.imperative.ImperativeStreamManager;
import io.flowwarden.stream.internal.lock.LeaderElectionCoordinator;
import io.flowwarden.stream.mongo.lock.MongoLockService;
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
import org.springframework.data.mongodb.core.MongoTemplate;

import java.net.InetAddress;

/**
 * Auto-configuration for FlowWarden in IMPERATIVE mode.
 *
 * <p>Activated when {@code flowwarden.default-mode=IMPERATIVE}.</p>
 *
 * <p>The three state beans — {@link CheckpointStore}, {@link DlqStore} and
 * {@link LockService} — are built on the application's default
 * {@code MongoTemplate}: the FlowWarden state ({@code _fw_checkpoints},
 * {@code _fw_dlq}, {@code _fw_locks}) lives in that database.
 * {@code @ChangeStream(mongoTemplateRef = …)} selects the template a stream
 * <em>watches</em> and does not move the state. Each bean is
 * {@code @ConditionalOnMissingBean}: an application whose state belongs in another
 * database declares the three beans in its own {@code @Configuration}, on the
 * template of that database:</p>
 *
 * <pre>{@code
 * @Bean
 * CheckpointStore checkpointStore(@Qualifier("stateMongoTemplate") MongoTemplate template) {
 *     return new MongoCheckpointStore(template);
 * }
 *
 * @Bean
 * DlqStore dlqStore(@Qualifier("stateMongoTemplate") MongoTemplate template,
 *                   MongoDlqProperties properties) {
 *     return new MongoDlqStore(template, properties);
 * }
 *
 * @Bean
 * LockService lockService(@Qualifier("stateMongoTemplate") MongoTemplate template) {
 *     return new MongoLockService(template);
 * }
 * }</pre>
 *
 * <p>Spring Boot's own {@code MongoTemplate} backs off as soon as the application
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
@ConditionalOnProperty(name = "flowwarden.default-mode", havingValue = "IMPERATIVE", matchIfMissing = true)
@EnableConfigurationProperties(MongoDlqProperties.class)
public class ImperativeFlowWardenAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public CheckpointStore checkpointStore(MongoTemplate mongoTemplate) {
        return new MongoCheckpointStore(mongoTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    public DlqStore dlqStore(MongoTemplate mongoTemplate, MongoDlqProperties dlqProperties) {
        return new MongoDlqStore(mongoTemplate, dlqProperties);
    }

    @Bean
    @ConditionalOnMissingBean
    public LockService lockService(MongoTemplate mongoTemplate) {
        return new MongoLockService(mongoTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    public LeaderElectionCoordinator leaderElectionCoordinator(LockService lockService, Environment env) {
        return new LeaderElectionCoordinator(lockService, resolveInstanceId(env));
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
     * Resolves the instance ID as {@code hostname:appName:port}, used as the lock owner identity
     * in {@link LeaderElectionCoordinator}.
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
