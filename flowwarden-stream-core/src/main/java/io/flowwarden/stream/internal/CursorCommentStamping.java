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

import com.mongodb.ClientSessionOptions;
import io.flowwarden.stream.FlowWarden;
import org.bson.codecs.configuration.CodecRegistry;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import reactor.core.publisher.Mono;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;

/**
 * Stamps {@code comment: "flowwarden:<streamName>"} on every change stream
 * cursor opened through a decorated database factory, so cursors are
 * attributable to their stream in {@code $currentOp}, server logs and the
 * profiler.
 *
 * <p>Why a decoration: Spring Data's {@code ChangeStreamOptions} exposes no
 * {@code comment} option (checked through 4.2.x), while the driver does
 * ({@code ChangeStreamIterable#comment} / {@code ChangeStreamPublisher#comment}).
 * Both Spring Data cursor-creation paths obtain their {@code MongoDatabase}
 * from the template's database factory and immediately call
 * {@code watch(...)} on it — decorating the factory intercepts exactly that
 * point and nothing else. Metadata only, no behavioral change; if Spring
 * Data ever exposes the option natively, this class disappears in favor of
 * a builder call.</p>
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public final class CursorCommentStamping {

    private CursorCommentStamping() {
    }

    /** The comment stamped on a stream's main cursor. */
    public static String commentFor(String streamName) {
        return FlowWarden.CURSOR_COMMENT_PREFIX + streamName;
    }

    /** The comment stamped on the ephemeral resume-token validation cursors. */
    public static String validationCommentFor(String streamName) {
        return FlowWarden.CURSOR_COMMENT_PREFIX + "resume-validation:" + streamName;
    }

    /** The comment stamped on heartbeat probe cursors. */
    public static String heartbeatCommentFor(String streamName) {
        return FlowWarden.CURSOR_COMMENT_PREFIX + "heartbeat:" + streamName;
    }

    public static MongoDatabaseFactory stamp(MongoDatabaseFactory delegate, String comment) {
        return new StampingMongoDatabaseFactory(delegate, comment);
    }

    public static ReactiveMongoDatabaseFactory stamp(ReactiveMongoDatabaseFactory delegate,
                                                     String comment) {
        return new StampingReactiveMongoDatabaseFactory(delegate, comment);
    }

    private record StampingMongoDatabaseFactory(MongoDatabaseFactory delegate, String comment)
            implements MongoDatabaseFactory {

        @Override
        public com.mongodb.client.MongoDatabase getMongoDatabase() {
            return (com.mongodb.client.MongoDatabase) stampingProxy(
                    com.mongodb.client.MongoDatabase.class, delegate.getMongoDatabase(), comment);
        }

        @Override
        public com.mongodb.client.MongoDatabase getMongoDatabase(String dbName) {
            return (com.mongodb.client.MongoDatabase) stampingProxy(
                    com.mongodb.client.MongoDatabase.class, delegate.getMongoDatabase(dbName), comment);
        }

        @Override
        public PersistenceExceptionTranslator getExceptionTranslator() {
            return delegate.getExceptionTranslator();
        }

        @Override
        public CodecRegistry getCodecRegistry() {
            return delegate.getCodecRegistry();
        }

        @Override
        public com.mongodb.client.ClientSession getSession(ClientSessionOptions options) {
            return delegate.getSession(options);
        }

        @Override
        public MongoDatabaseFactory withSession(com.mongodb.client.ClientSession session) {
            return new StampingMongoDatabaseFactory(delegate.withSession(session), comment);
        }

        @Override
        public boolean isTransactionActive() {
            return delegate.isTransactionActive();
        }
    }

    private record StampingReactiveMongoDatabaseFactory(ReactiveMongoDatabaseFactory delegate,
                                                        String comment)
            implements ReactiveMongoDatabaseFactory {

        @Override
        public Mono<com.mongodb.reactivestreams.client.MongoDatabase> getMongoDatabase() {
            return delegate.getMongoDatabase().map(db ->
                    (com.mongodb.reactivestreams.client.MongoDatabase) stampingProxy(
                            com.mongodb.reactivestreams.client.MongoDatabase.class, db, comment));
        }

        @Override
        public Mono<com.mongodb.reactivestreams.client.MongoDatabase> getMongoDatabase(String dbName) {
            return delegate.getMongoDatabase(dbName).map(db ->
                    (com.mongodb.reactivestreams.client.MongoDatabase) stampingProxy(
                            com.mongodb.reactivestreams.client.MongoDatabase.class, db, comment));
        }

        @Override
        public PersistenceExceptionTranslator getExceptionTranslator() {
            return delegate.getExceptionTranslator();
        }

        @Override
        public CodecRegistry getCodecRegistry() {
            return delegate.getCodecRegistry();
        }

        @Override
        public Mono<com.mongodb.reactivestreams.client.ClientSession> getSession(
                ClientSessionOptions options) {
            return delegate.getSession(options);
        }

        @Override
        public ReactiveMongoDatabaseFactory withSession(
                com.mongodb.reactivestreams.client.ClientSession session) {
            return new StampingReactiveMongoDatabaseFactory(delegate.withSession(session), comment);
        }

        @Override
        public boolean isTransactionActive() {
            return delegate.isTransactionActive();
        }
    }

    /**
     * Reflective proxy over a driver {@code MongoDatabase}/{@code MongoCollection}
     * (sync or reactive): {@code watch(...)} results get the comment,
     * {@code getCollection(...)} results are re-proxied, methods returning a
     * reconfigured instance of the same interface ({@code withCodecRegistry},
     * {@code withReadPreference}, …) are re-proxied so the stamping survives,
     * and everything else delegates untouched. Reflective on purpose: driver
     * interface additions never break the decoration.
     */
    private static Object stampingProxy(Class<?> iface, Object target, String comment) {
        return Proxy.newProxyInstance(iface.getClassLoader(), new Class<?>[]{iface},
                (proxy, method, args) -> {
                    Object result;
                    try {
                        result = method.invoke(target, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                    if (result == null) {
                        return null;
                    }
                    if ("watch".equals(method.getName())) {
                        if (result instanceof com.mongodb.client.ChangeStreamIterable<?> iterable) {
                            return iterable.comment(comment);
                        }
                        if (result instanceof
                                com.mongodb.reactivestreams.client.ChangeStreamPublisher<?> publisher) {
                            return publisher.comment(comment);
                        }
                        return result;
                    }
                    if ("getCollection".equals(method.getName())) {
                        return stampingProxy(method.getReturnType(), result, comment);
                    }
                    if (method.getReturnType().equals(iface)) {
                        return stampingProxy(iface, result, comment);
                    }
                    return result;
                });
    }
}
