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

import io.flowwarden.stream.ChangeStreamContext;
import io.flowwarden.stream.DeploymentMode;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnChange;
import io.flowwarden.stream.core.FlowWardenStreamManager;
import io.flowwarden.stream.internal.lock.LeaderElectionCoordinator;
import io.flowwarden.stream.internal.lock.ReactiveMongoLockService;
import io.flowwarden.stream.internal.reactive.ReactiveStreamManager;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.DlqStore;
import io.flowwarden.stream.spi.LockService;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.data.mongo.MongoReactiveDataAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoReactiveAutoConfiguration;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import reactor.core.publisher.Mono;

import java.util.Set;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A pure WebFlux application — {@code spring-boot-starter-data-mongodb-reactive} as its
 * only Mongo dependency — must start FlowWarden in REACTIVE mode.
 *
 * <p>The synchronous driver is hidden from the context's class loader: every class of
 * {@code mongodb-driver-sync} (packages {@code com.mongodb.client},
 * {@code com.mongodb.client.internal}, {@code com.mongodb.client.gridfs},
 * {@code com.mongodb.client.vault}; {@code com.mongodb.client.model} and friends live in
 * {@code mongodb-driver-core} and stay visible, the reactive driver needs them) is
 * unloadable, so Spring Boot's {@code MongoAutoConfiguration} backs off exactly as it does
 * when the starter is absent, and no {@code MongoDatabaseFactory} / {@code MongoTemplate}
 * bean exists. Before the reactive lock service, the auto-configured {@code LockService}
 * then failed with {@code NoSuchBeanDefinitionException: MongoDatabaseFactory}, taking
 * the election coordinator and the stream manager down with it.</p>
 */
class ReactiveOnlyClasspathAutoConfigurationTest {

    private static final Set<String> SYNC_DRIVER_PACKAGES = Set.of(
            "com.mongodb.client",
            "com.mongodb.client.internal",
            "com.mongodb.client.gridfs",
            "com.mongodb.client.vault");

    private static final Predicate<String> SYNC_DRIVER_CLASSES = name -> {
        int lastDot = name.lastIndexOf('.');
        return lastDot > 0 && SYNC_DRIVER_PACKAGES.contains(name.substring(0, lastDot));
    };

    @SuppressWarnings("unchecked")
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withClassLoader(new FilteredClassLoader(SYNC_DRIVER_CLASSES))
            .withConfiguration(AutoConfigurations.of(
                    // Boot's synchronous Mongo auto-configurations are deliberately on the
                    // list: they must back off on this class path, as they do in a real
                    // reactive-only application.
                    MongoAutoConfiguration.class,
                    MongoDataAutoConfiguration.class,
                    MongoReactiveAutoConfiguration.class,
                    MongoReactiveDataAutoConfiguration.class,
                    FlowWardenAutoConfiguration.class,
                    ImperativeFlowWardenAutoConfiguration.class,
                    ReactiveFlowWardenAutoConfiguration.class))
            .withPropertyValues(
                    "flowwarden.default-mode=REACTIVE",
                    // Nothing connects at bean creation; the URI only has to parse.
                    "spring.data.mongodb.uri=mongodb://localhost:27017/reactive-only");

    @Test
    void synchronousDriverIsReallyHidden() {
        contextRunner.withUserConfiguration(AllInstancesApp.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(MongoDatabaseFactory.class);
            assertThat(context).doesNotHaveBean(MongoTemplate.class);
            assertThat(context).hasSingleBean(ReactiveMongoTemplate.class);
        });
    }

    @Test
    void reactiveMode_allInstances_startsWithoutTheSynchronousDriver() {
        contextRunner.withUserConfiguration(AllInstancesApp.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(FlowWardenStreamManager.class);
            assertThat(context.getBean(FlowWardenStreamManager.class))
                    .isInstanceOf(ReactiveStreamManager.class);
            assertThat(context).hasSingleBean(CheckpointStore.class);
            assertThat(context).hasSingleBean(DlqStore.class);
        });
    }

    @Test
    void reactiveMode_singleLeader_startsWithoutTheSynchronousDriver() {
        contextRunner.withUserConfiguration(SingleLeaderApp.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(FlowWardenStreamManager.class);
            assertThat(context).hasSingleBean(LeaderElectionCoordinator.class);
            assertThat(context).hasSingleBean(LockService.class);
            assertThat(context.getBean(LockService.class))
                    .isInstanceOf(ReactiveMongoLockService.class);
        });
    }

    @Test
    void userProvidedLockService_stillWins() {
        LockService custom = LockService.noOp();
        contextRunner
                .withUserConfiguration(SingleLeaderApp.class)
                .withBean("customLockService", LockService.class, () -> custom)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(LockService.class);
                    assertThat(context.getBean(LockService.class)).isSameAs(custom);
                });
    }

    // --- Test applications ---

    @Configuration(proxyBeanMethods = false)
    @EnableFlowWarden
    @Import(AllInstancesHandler.class)
    static class AllInstancesApp {
    }

    @Configuration(proxyBeanMethods = false)
    @EnableFlowWarden
    @Import(SingleLeaderHandler.class)
    static class SingleLeaderApp {
    }

    @ChangeStream(name = "orders-all-instances", collection = "orders")
    static class AllInstancesHandler {
        @OnChange
        Mono<Void> handle(ChangeStreamContext<?> ctx) {
            return Mono.empty();
        }
    }

    @ChangeStream(name = "orders-single-leader", collection = "orders",
            deploymentMode = DeploymentMode.SINGLE_LEADER)
    static class SingleLeaderHandler {
        @OnChange
        Mono<Void> handle(ChangeStreamContext<?> ctx) {
            return Mono.empty();
        }
    }
}
