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
package io.flowwarden.stream.internal.discovery;

import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Rewrites a pipeline written against the document into one that applies to
 * the change event ({@code PipelineScope.DOCUMENT}).
 *
 * <p>Only the field names of {@code $match} stages are rewritten: a name
 * whose first segment is not a top-level field of the change event is
 * prefixed with {@code fullDocument.}. The rewrite descends into
 * {@code $and} / {@code $or} / {@code $nor} and nowhere else — the operand
 * of a field ({@code $elemMatch}, an embedded document compared by equality,
 * an array of documents) is relative to that field and left alone, as are
 * {@code $expr} and every other {@code $}-operator. It stops at the first
 * stage that reshapes the event ({@code $project}, {@code $addFields},
 * {@code $replaceRoot}, …): what follows no longer reads a change event.</p>
 *
 * <p>The input documents are never mutated.</p>
 */
final class DocumentScopePipeline {

    private DocumentScopePipeline() {
    }

    static List<Document> toEventScope(List<Document> pipeline) {
        List<Document> result = new ArrayList<>(pipeline.size());
        boolean reshaped = false;
        for (Document stage : pipeline) {
            if (!reshaped && stage.size() == 1 && stage.get("$match") instanceof Map<?, ?> match) {
                result.add(new Document("$match", prefixQuery(match)));
                continue;
            }
            if (stage.keySet().stream().anyMatch(ChangeEventFields.RESHAPING_STAGES::contains)) {
                reshaped = true;
            }
            result.add(stage);
        }
        return result;
    }

    private static Document prefixQuery(Map<?, ?> query) {
        Document result = new Document();
        for (Map.Entry<?, ?> entry : query.entrySet()) {
            String key = String.valueOf(entry.getKey());
            Object value = entry.getValue();
            if (ChangeEventFields.LOGICAL_OPERATORS.contains(key) && value instanceof Iterable<?> operands) {
                List<Object> prefixed = new ArrayList<>();
                for (Object operand : operands) {
                    prefixed.add(operand instanceof Map<?, ?> nested ? prefixQuery(nested) : operand);
                }
                result.append(key, prefixed);
            } else if (key.startsWith("$") || ChangeEventFields.isTopLevel(key)) {
                result.append(key, value);
            } else {
                result.append(ChangeEventFields.FULL_DOCUMENT + "." + key, value);
            }
        }
        return result;
    }
}
