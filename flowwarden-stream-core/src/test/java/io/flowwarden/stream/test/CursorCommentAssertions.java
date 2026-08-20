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
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shared {@code $currentOp} assertion for the cursor-attribution tests: a
 * change stream cursor stamped with the given comment must be visible on
 * the shared container, on the expected namespace. The comment surfaces at
 * {@code cursor.originatingCommand.comment} while the cursor idles between
 * {@code getMore}s and at {@code command.comment} while one is in flight —
 * both locations are accepted.
 */
public final class CursorCommentAssertions {

    private CursorCommentAssertions() {
    }

    public static void assertCursorStamped(String expectedComment, String collection) {
        try (MongoClient client = MongoClients.create(SharedMongoContainer.MONGO.getReplicaSetUrl())) {
            List<Document> ops = new ArrayList<>();
            client.getDatabase("admin")
                    .aggregate(List.of(new Document("$currentOp",
                            new Document("idleCursors", true).append("allUsers", true))))
                    .into(ops);

            List<Document> stamped = ops.stream()
                    .filter(op -> expectedComment.equals(commentOf(op)))
                    .toList();

            assertThat(stamped)
                    .as("a cursor stamped '%s' must be visible in $currentOp", expectedComment)
                    .isNotEmpty();
            assertThat(stamped)
                    .as("the stamped cursor must watch the expected namespace")
                    .anyMatch(op -> {
                        String ns = op.getString("ns");
                        return ns != null && ns.endsWith("." + collection);
                    });
        }
    }

    private static String commentOf(Document op) {
        Document cursor = op.get("cursor", Document.class);
        if (cursor != null) {
            Document originating = cursor.get("originatingCommand", Document.class);
            if (originating != null && originating.get("comment") instanceof String s) {
                return s;
            }
        }
        Document command = op.get("command", Document.class);
        if (command != null && command.get("comment") instanceof String s) {
            return s;
        }
        return null;
    }
}
