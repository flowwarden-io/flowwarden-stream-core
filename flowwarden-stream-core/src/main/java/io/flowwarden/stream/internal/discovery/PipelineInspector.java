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

import io.flowwarden.stream.FullDocumentMode;
import io.flowwarden.stream.OperationType;
import io.flowwarden.stream.PipelineScope;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Startup check of a resolved pipeline: reports the {@code $match} conditions
 * that can never select a change event, so that a filter written against the
 * document does not leave a stream running and silent.
 *
 * <p>The inspection reads the {@code $match} stages up to the first stage
 * that reshapes the event, descends into {@code $and} / {@code $or} /
 * {@code $nor}, and ignores {@code $expr} and every other operator. It only
 * warns, and never fails a start.</p>
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public final class PipelineInspector {

    private static final Logger log = LoggerFactory.getLogger(PipelineInspector.class);

    private PipelineInspector() {
    }

    /**
     * What the {@code $match} stages of a pipeline reference.
     *
     * @param unknownFields          paths whose first segment is not a field of a change event, in order
     * @param referencesFullDocument whether a condition is on {@code fullDocument} or below
     * @param matchesOnEventId       whether a condition is on the event's {@code _id}
     */
    record Findings(Set<String> unknownFields, boolean referencesFullDocument, boolean matchesOnEventId) {
    }

    /**
     * Inspects the resolved pipeline of a stream and logs a WARN for each
     * finding. Never throws.
     */
    public static void inspect(ChangeStreamDefinition def, List<Document> pipeline) {
        try {
            if (pipeline == null || pipeline.isEmpty()) {
                return;
            }
            Findings findings = analyze(pipeline);
            String stream = def.streamName();
            PipelineScope scope = def.pipelineMethod() != null ? def.pipelineMethod().scope() : PipelineScope.EVENT;

            if (!findings.unknownFields().isEmpty()) {
                String first = findings.unknownFields().iterator().next();
                log.warn("Stream '{}': the pipeline has a $match on {} — not a field of a change event, so "
                                + "the condition can never match and the stream will receive no event for it. "
                                + "The stages apply to the change event, in which the document sits under "
                                + "'fullDocument': write '{}.{}', or declare the pipeline as an Aggregation with "
                                + "scope = PipelineScope.DOCUMENT.",
                        stream, findings.unknownFields(), ChangeEventFields.FULL_DOCUMENT, first);
            }
            if (scope == PipelineScope.DOCUMENT && findings.matchesOnEventId()) {
                log.warn("Stream '{}': the pipeline has a $match on '_id' with scope = DOCUMENT. '_id' is a "
                        + "field of the change event (its resume token) and is not prefixed: the condition "
                        + "does not compare the document's id. Write 'documentKey._id' (present on every "
                        + "data event, deletes included) or 'fullDocument._id'.", stream);
            }
            if (findings.referencesFullDocument()
                    && def.config() != null
                    && def.config().fullDocument() == FullDocumentMode.DEFAULT
                    && def.typedHandlers() != null
                    && def.typedHandlers().containsKey(OperationType.UPDATE)) {
                log.warn("Stream '{}': the pipeline has a $match on 'fullDocument' and the stream uses "
                        + "fullDocument = DEFAULT with an onUpdate handler. Update events carry no fullDocument "
                        + "in DEFAULT mode, so such a condition drops them server-side and the handler does "
                        + "not run. Set fullDocument = FullDocumentMode.UPDATE_LOOKUP.", stream);
            }
        } catch (RuntimeException e) {
            log.debug("Pipeline inspection skipped for stream '{}'", def != null ? def.streamName() : "?", e);
        }
    }

    static Findings analyze(List<Document> pipeline) {
        Set<String> unknown = new LinkedHashSet<>();
        boolean[] flags = new boolean[2]; // [0] fullDocument, [1] event _id
        for (Document stage : pipeline) {
            if (stage == null || stage.size() != 1) {
                break;
            }
            String operator = stage.keySet().iterator().next();
            if (!ChangeEventFields.SHAPE_PRESERVING_STAGES.contains(operator)) {
                break; // reshaped, or a stage this check does not know: stop rather than guess
            }
            if ("$match".equals(operator) && stage.get(operator) instanceof Map<?, ?> query) {
                walk(query, unknown, flags);
            }
        }
        return new Findings(Collections.unmodifiableSet(unknown), flags[0], flags[1]);
    }

    private static void walk(Map<?, ?> query, Set<String> unknown, boolean[] flags) {
        for (Map.Entry<?, ?> entry : query.entrySet()) {
            String key = String.valueOf(entry.getKey());
            if (ChangeEventFields.LOGICAL_OPERATORS.contains(key)) {
                if (entry.getValue() instanceof Iterable<?> operands) {
                    for (Object operand : operands) {
                        if (operand instanceof Map<?, ?> nested) {
                            walk(nested, unknown, flags);
                        }
                    }
                }
            } else if (key.startsWith("$")) {
                continue; // $expr, $comment, $jsonSchema, …: not a field path
            } else if (!ChangeEventFields.isTopLevel(key)) {
                unknown.add(key);
            } else {
                String first = ChangeEventFields.firstSegment(key);
                if (ChangeEventFields.FULL_DOCUMENT.equals(first)) {
                    flags[0] = true;
                } else if (ChangeEventFields.EVENT_ID.equals(first)) {
                    flags[1] = true;
                }
            }
        }
    }
}
