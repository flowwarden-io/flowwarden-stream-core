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

import java.util.Set;

/**
 * The top-level fields of a MongoDB change event. The set is closed: a path
 * whose first segment is not one of them designates nothing in an event that
 * no earlier stage has reshaped.
 */
final class ChangeEventFields {

    /** Where the document sits in a change event. */
    static final String FULL_DOCUMENT = "fullDocument";

    /** The event identifier (its resume token) — not the document's {@code _id}. */
    static final String EVENT_ID = "_id";

    static final Set<String> TOP_LEVEL = Set.of(
            EVENT_ID,
            "operationType",
            FULL_DOCUMENT,
            "fullDocumentBeforeChange",
            "ns",
            "nsType",
            "to",
            "documentKey",
            "updateDescription",
            "clusterTime",
            "wallTime",
            "txnNumber",
            "lsid",
            "collectionUUID",
            "operationDescription",
            "stateBeforeChange",
            "splitEvent");

    /** Stages after which the event no longer has its original shape. */
    static final Set<String> RESHAPING_STAGES = Set.of(
            "$addFields", "$set", "$project", "$replaceRoot", "$replaceWith", "$unset");

    /** Stages that leave the shape of the event untouched. */
    static final Set<String> SHAPE_PRESERVING_STAGES = Set.of(
            "$match", "$redact", "$changeStreamSplitLargeEvent");

    /** Query operators whose operands are themselves queries on the same level. */
    static final Set<String> LOGICAL_OPERATORS = Set.of("$and", "$or", "$nor");

    private ChangeEventFields() {
    }

    /** The first segment of a dotted path. */
    static String firstSegment(String path) {
        int dot = path.indexOf('.');
        return dot < 0 ? path : path.substring(0, dot);
    }

    static boolean isTopLevel(String path) {
        return TOP_LEVEL.contains(firstSegment(path));
    }
}
