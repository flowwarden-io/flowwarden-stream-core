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

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link CursorCommentStamping} serves both execution modes from one class,
 * and an application ships only one Mongo driver: the reactive one alone
 * since {@code 1.0.0-rc.8}, the synchronous one alone in imperative mode.
 * The bytecode executed on one path must therefore never name a type of the
 * other driver — the JVM resolves the type of an {@code instanceof} when the
 * instruction runs, and a shared check would throw
 * {@code NoClassDefFoundError} on the first {@code watch(...)}.
 *
 * <p>Spring's {@code FilteredClassLoader} cannot prove this: it only hides
 * classes from class-presence checks, while an {@code instanceof} resolves
 * through the defining loader of {@code CursorCommentStamping}, which on the
 * test class path still sees both drivers. These tests define the class in a
 * loader of their own that refuses one driver family outright.</p>
 */
class CursorCommentStampingDriverIsolationTest {

    private static final String COMMENT = CursorCommentStamping.commentFor("orders-stream");

    private static final Set<String> SYNC_DRIVER_PACKAGES = Set.of(
            "com.mongodb.client",
            "com.mongodb.client.internal",
            "com.mongodb.client.gridfs",
            "com.mongodb.client.vault");

    private static final Set<String> REACTIVE_DRIVER_PACKAGES = Set.of(
            "com.mongodb.reactivestreams.client",
            "com.mongodb.reactivestreams.client.internal",
            "com.mongodb.reactivestreams.client.gridfs",
            "com.mongodb.reactivestreams.client.vault");

    @Test
    @SuppressWarnings("unchecked")
    void reactiveWatch_getsTheComment_withoutTheSynchronousDriver() throws Exception {
        Class<?> stamping = new DriverHidingLoader(SYNC_DRIVER_PACKAGES)
                .loadClass(CursorCommentStamping.class.getName());
        Method stamp = stamping.getMethod("stamp", ReactiveMongoDatabaseFactory.class, String.class);

        ReactiveMongoDatabaseFactory delegate = mock(ReactiveMongoDatabaseFactory.class);
        com.mongodb.reactivestreams.client.MongoDatabase db =
                mock(com.mongodb.reactivestreams.client.MongoDatabase.class);
        com.mongodb.reactivestreams.client.ChangeStreamPublisher<Document> publisher =
                mock(com.mongodb.reactivestreams.client.ChangeStreamPublisher.class);
        com.mongodb.reactivestreams.client.ChangeStreamPublisher<Document> commented =
                mock(com.mongodb.reactivestreams.client.ChangeStreamPublisher.class);
        when(delegate.getMongoDatabase()).thenReturn(Mono.just(db));
        when(db.watch()).thenReturn(publisher);
        when(publisher.comment(anyString())).thenReturn(commented);

        ReactiveMongoDatabaseFactory stamped =
                (ReactiveMongoDatabaseFactory) stamp.invoke(null, delegate, COMMENT);

        // The unguarded version died here with NoClassDefFoundError: com/mongodb/client/ChangeStreamIterable.
        Object result = stamped.getMongoDatabase().block().watch();

        assertThat(result).isSameAs(commented);
        verify(publisher).comment(COMMENT);
    }

    @Test
    @SuppressWarnings("unchecked")
    void synchronousWatch_getsTheComment_withoutTheReactiveDriver() throws Exception {
        Class<?> stamping = new DriverHidingLoader(REACTIVE_DRIVER_PACKAGES)
                .loadClass(CursorCommentStamping.class.getName());
        Method stamp = stamping.getMethod("stamp", MongoDatabaseFactory.class, String.class);

        MongoDatabaseFactory delegate = mock(MongoDatabaseFactory.class);
        com.mongodb.client.MongoDatabase db = mock(com.mongodb.client.MongoDatabase.class);
        com.mongodb.client.ChangeStreamIterable<Document> iterable =
                mock(com.mongodb.client.ChangeStreamIterable.class);
        com.mongodb.client.ChangeStreamIterable<Document> commented =
                mock(com.mongodb.client.ChangeStreamIterable.class);
        when(delegate.getMongoDatabase()).thenReturn(db);
        when(db.watch()).thenReturn(iterable);
        when(iterable.comment(anyString())).thenReturn(commented);

        MongoDatabaseFactory stamped = (MongoDatabaseFactory) stamp.invoke(null, delegate, COMMENT);

        Object result = stamped.getMongoDatabase().watch();

        assertThat(result).isSameAs(commented);
        verify(iterable).comment(COMMENT);
    }

    @Test
    void theLoaderReallyHidesTheDriver() {
        DriverHidingLoader loader = new DriverHidingLoader(SYNC_DRIVER_PACKAGES);

        assertThat(loadable(loader, "com.mongodb.client.ChangeStreamIterable")).isFalse();
        assertThat(loadable(loader, "com.mongodb.client.model.Filters")).isTrue();      // driver-core, kept
        assertThat(loadable(loader, "com.mongodb.reactivestreams.client.ChangeStreamPublisher")).isTrue();
    }

    private static boolean loadable(ClassLoader loader, String name) {
        try {
            Class.forName(name, false, loader);
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    /**
     * Defines {@code CursorCommentStamping} and its nested classes itself (so
     * their symbolic references resolve through this loader) and refuses every
     * class of the hidden driver packages; everything else comes from the
     * test class path.
     */
    private static final class DriverHidingLoader extends ClassLoader {

        private final Set<String> hiddenPackages;

        DriverHidingLoader(Set<String> hiddenPackages) {
            super(CursorCommentStampingDriverIsolationTest.class.getClassLoader());
            this.hiddenPackages = hiddenPackages;
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            int lastDot = name.lastIndexOf('.');
            if (lastDot > 0 && hiddenPackages.contains(name.substring(0, lastDot))) {
                throw new ClassNotFoundException(name + " (driver hidden by " + getClass().getSimpleName() + ")");
            }
            if (!name.startsWith(CursorCommentStamping.class.getName())) {
                return super.loadClass(name, resolve);
            }
            synchronized (getClassLoadingLock(name)) {
                Class<?> loaded = findLoadedClass(name);
                if (loaded == null) {
                    loaded = defineFromParentBytes(name);
                }
                if (resolve) {
                    resolveClass(loaded);
                }
                return loaded;
            }
        }

        private Class<?> defineFromParentBytes(String name) throws ClassNotFoundException {
            String resource = name.replace('.', '/') + ".class";
            try (InputStream in = getParent().getResourceAsStream(resource)) {
                if (in == null) {
                    throw new ClassNotFoundException(name);
                }
                byte[] bytes = in.readAllBytes();
                return defineClass(name, bytes, 0, bytes.length);
            } catch (IOException e) {
                throw new ClassNotFoundException(name, e);
            }
        }
    }
}
