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
package io.flowwarden.stream.mongo;

import com.mongodb.client.MongoDatabase;
import io.flowwarden.stream.mongo.checkpoint.MongoCheckpointStore;
import io.flowwarden.stream.mongo.checkpoint.ReactiveMongoCheckpointStore;
import io.flowwarden.stream.mongo.dlq.MongoDlqProperties;
import io.flowwarden.stream.mongo.dlq.MongoDlqStore;
import io.flowwarden.stream.mongo.dlq.ReactiveMongoDlqStore;
import io.flowwarden.stream.mongo.lock.MongoLockService;
import io.flowwarden.stream.mongo.lock.ReactiveMongoLockService;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import reactor.core.publisher.Mono;

import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The MongoDB stores are public so that an application can rebuild them on another
 * template. That only holds while everything an application can see of them is
 * itself public API.
 */
class MongoStatePublicApiTest {

    private static final String INTERNAL = "io.flowwarden.stream.internal";

    private static final List<Class<?>> PUBLISHED = List.of(
            MongoCheckpointStore.class, ReactiveMongoCheckpointStore.class,
            MongoDlqStore.class, ReactiveMongoDlqStore.class, MongoDlqProperties.class,
            MongoLockService.class, ReactiveMongoLockService.class);

    @Test
    void publishedClassesArePublic() {
        assertThat(PUBLISHED).allSatisfy(type ->
                assertThat(Modifier.isPublic(type.getModifiers())).as(type.getName()).isTrue());
    }

    @Test
    void noInternalTypeAppearsInAPublicOrProtectedSignature() {
        List<String> leaks = new ArrayList<>();
        for (Class<?> type : PUBLISHED) {
            for (Class<?> t = type; t != null && t != Object.class; t = t.getSuperclass()) {
                check(leaks, type, "extends", t);
            }
            for (Class<?> itf : type.getInterfaces()) {
                check(leaks, type, "implements", itf);
            }
            for (Constructor<?> constructor : type.getDeclaredConstructors()) {
                if (visible(constructor.getModifiers())) {
                    checkExecutable(leaks, type, constructor);
                }
            }
            for (Method method : type.getDeclaredMethods()) {
                if (visible(method.getModifiers()) && !method.isSynthetic()) {
                    check(leaks, type, method.getName() + " returns", method.getReturnType());
                    checkExecutable(leaks, type, method);
                }
            }
            for (Field field : type.getDeclaredFields()) {
                if (visible(field.getModifiers()) && !field.isSynthetic()) {
                    check(leaks, type, "field " + field.getName(), field.getType());
                }
            }
        }
        assertThat(leaks).isEmpty();
    }

    @Test
    void imperativeStoresDescribeThemselvesByTheirDatabase() {
        MongoTemplate template = mock(MongoTemplate.class);
        MongoDatabase database = mock(MongoDatabase.class);
        when(template.getDb()).thenReturn(database);
        when(database.getName()).thenReturn("stateDb");

        assertThat(new MongoCheckpointStore(template).backendDescription()).isEqualTo("mongo[stateDb]");
        assertThat(new MongoDlqStore(template, new MongoDlqProperties()).backendDescription())
                .isEqualTo("mongo[stateDb]");
        assertThat(new MongoLockService(template).backendDescription()).isEqualTo("mongo[stateDb]");
    }

    @Test
    void reactiveStoresDescribeThemselvesByTheirDatabase() {
        ReactiveMongoTemplate template = mock(ReactiveMongoTemplate.class);
        com.mongodb.reactivestreams.client.MongoDatabase database =
                mock(com.mongodb.reactivestreams.client.MongoDatabase.class);
        when(template.getMongoDatabase()).thenReturn(Mono.just(database));
        when(database.getName()).thenReturn("stateDb");

        assertThat(new ReactiveMongoCheckpointStore(template).backendDescription()).isEqualTo("mongo[stateDb]");
        assertThat(new ReactiveMongoDlqStore(template, new MongoDlqProperties()).backendDescription())
                .isEqualTo("mongo[stateDb]");
        assertThat(new ReactiveMongoLockService(template).backendDescription()).isEqualTo("mongo[stateDb]");
    }

    @Test
    void aDatabaseThatCannotBeResolvedNeverMakesTheDescriptionThrow() {
        MongoTemplate template = mock(MongoTemplate.class);
        when(template.getDb()).thenThrow(new IllegalStateException("no database"));
        ReactiveMongoTemplate reactiveTemplate = mock(ReactiveMongoTemplate.class);
        when(reactiveTemplate.getMongoDatabase()).thenReturn(Mono.error(new IllegalStateException("no database")));

        assertThat(new MongoCheckpointStore(template).backendDescription()).isEqualTo("mongo[?]");
        assertThat(new ReactiveMongoLockService(reactiveTemplate).backendDescription()).isEqualTo("mongo[?]");
    }

    private static boolean visible(int modifiers) {
        return Modifier.isPublic(modifiers) || Modifier.isProtected(modifiers);
    }

    private static void checkExecutable(List<String> leaks, Class<?> owner, Executable executable) {
        for (Class<?> parameter : executable.getParameterTypes()) {
            check(leaks, owner, executable.getName() + " parameter", parameter);
        }
        for (Class<?> thrown : executable.getExceptionTypes()) {
            check(leaks, owner, executable.getName() + " throws", thrown);
        }
    }

    private static void check(List<String> leaks, Class<?> owner, String where, Class<?> used) {
        Class<?> component = used;
        while (component.isArray()) {
            component = component.getComponentType();
        }
        if (component.getName().startsWith(INTERNAL)) {
            leaks.add(owner.getSimpleName() + " — " + where + " " + component.getName());
        }
    }
}
