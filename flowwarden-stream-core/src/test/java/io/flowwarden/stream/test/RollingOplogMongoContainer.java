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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.testcontainers.containers.MongoDBContainer;

import java.time.Duration;
import java.time.Instant;

/**
 * A replica set whose oplog actually rolls over: 1 MB oplog, storage
 * checkpoint every second (WiredTiger only truncates the oplog behind the
 * last checkpoint — with the default 60 s cadence a rollover takes a minute).
 *
 * <p>Separate from {@link SharedMongoContainer} on purpose: the shared one
 * never rolls over, which is also why the server's history-lost check never
 * fired in the integration tests — a fresh replica set's first oplog entry is
 * the {@code initiating set} noop, which the server exempts from the check.
 * {@link #rollOver(BsonTimestamp)} writes to a filler collection until the
 * oplog has truncated past a given timestamp, so a position taken before the
 * call is really gone.</p>
 */
public final class RollingOplogMongoContainer {

    public static final MongoDBContainer MONGO = new MongoDBContainer("mongo:6.0");

    private static final MongoClient CLIENT;

    static {
        MONGO.withCommand("--replSet", "docker-rs", "--oplogSize", "1", "--syncdelay", "1",
                "--setParameter", "enableTestCommands=1");
        MONGO.start();
        CLIENT = MongoClients.create(MONGO.getReplicaSetUrl());
    }

    private RollingOplogMongoContainer() {
    }

    /** The timestamp of the newest oplog entry — a mark that {@link #rollOver} can truncate past. */
    public static BsonTimestamp newestOplogTimestamp() {
        Document newest = local().getCollection("oplog.rs").find()
                .sort(new Document("$natural", -1)).limit(1).first();
        if (newest == null) {
            throw new IllegalStateException("empty oplog");
        }
        return newest.get("ts", BsonTimestamp.class);
    }

    /** The timestamp of the oldest oplog entry. */
    public static BsonTimestamp oldestOplogTimestamp() {
        Document oldest = local().getCollection("oplog.rs").find()
                .sort(new Document("$natural", 1)).limit(1).first();
        if (oldest == null) {
            throw new IllegalStateException("empty oplog");
        }
        return oldest.get("ts", BsonTimestamp.class);
    }

    /**
     * Writes 100 KB documents to a filler collection (a noisy neighbour:
     * never a watched collection) until the oldest oplog entry is newer than
     * {@code mark} — every position at or before the mark has rolled out.
     */
    public static void rollOver(BsonTimestamp mark) {
        MongoDatabase app = CLIENT.getDatabase(MONGO.getReplicaSetUrl().replaceAll(".*/", "").replaceAll("\\?.*", ""));
        String filler = "x".repeat(100 * 1024);
        Instant deadline = Instant.now().plus(Duration.ofSeconds(90));
        int batch = 0;
        while (oldestOplogTimestamp().compareTo(mark) <= 0) {
            if (Instant.now().isAfter(deadline)) {
                throw new IllegalStateException("oplog did not roll past " + mark + " within 90s");
            }
            for (int i = 0; i < 10; i++) {
                app.getCollection("_fw_oplog_filler").insertOne(
                        new Document("batch", batch).append("i", i).append("filler", filler));
            }
            batch++;
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while rolling the oplog over", e);
            }
        }
    }

    private static MongoDatabase local() {
        return CLIENT.getDatabase("local");
    }
}
