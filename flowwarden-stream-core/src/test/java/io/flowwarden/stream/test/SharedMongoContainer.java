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
package io.flowwarden.stream.test;

import org.testcontainers.containers.MongoDBContainer;

public final class SharedMongoContainer {
    public static final MongoDBContainer MONGO = new MongoDBContainer("mongo:6.0");

    static {
        // enableTestCommands allows failCommand failpoints (deterministic
        // cursor-death injection). The --replSet argument must be repeated:
        // withCommand replaces the container's default command entirely.
        MONGO.withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");
        MONGO.start();
        // NO manual shutdown hook: JVM hook ordering is unspecified, and a
        // hook killing Mongo while Spring's own shutdown hooks are still
        // cancelling reading tasks leaves those cancels stuck in the
        // driver's 30s server selection (Surefire then kills the fork).
        // Testcontainers' ryuk reaper removes the container after the JVM
        // exits — the manual hook was redundant.
    }

    private SharedMongoContainer() {}
}
