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
import org.bson.BsonNull;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Startup check of a resolved pipeline: reports the {@code $match} conditions
 * that can never be satisfied by a change event, so that a filter written
 * against the document does not leave a stream running and silent.
 *
 * <p>A path whose first segment is not a field of a change event designates a
 * field that is <em>always missing</em>, so a condition on it is a constant —
 * but which constant depends on the operator: {@code {status: "OPEN"}} is
 * always false, whereas {@code {status: {$exists: false}}},
 * {@code {status: null}} or {@code {status: {$ne: "OPEN"}}} are always true.
 * The inspection therefore evaluates each condition as MongoDB does on a
 * missing field, in three-valued logic (true, false, unknown), through
 * {@code $and} / {@code $or} / {@code $nor}, and reports a condition only when
 * it is certainly unsatisfiable where it stands: always false, or always true
 * under a {@code $nor}. Anything it cannot decide ({@code $expr}, an operator
 * it does not know) is unknown and never reported.</p>
 *
 * <p>The inspection reads the {@code $match} stages up to the first stage
 * that reshapes the event. It only warns, and never fails a start.</p>
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public final class PipelineInspector {

    private static final Logger log = LoggerFactory.getLogger(PipelineInspector.class);

    private static final String OPERATION_TYPE = "operationType";
    private static final String UPDATE = "update";

    /** Operators that never match a missing field, whatever their operand. */
    private static final Set<String> NEVER_ON_MISSING = Set.of(
            "$gt", "$lt", "$regex", "$elemMatch", "$size", "$type", "$mod");

    /** Operators that only qualify a sibling operator. */
    private static final Set<String> NEUTRAL = Set.of("$options", "$comment");

    private PipelineInspector() {
    }

    /** The outcome of a condition: certain either way, or not decidable by this check. */
    enum Truth {
        TRUE, FALSE, UNKNOWN;

        Truth not() {
            return this == TRUE ? FALSE : this == FALSE ? TRUE : UNKNOWN;
        }

        static Truth of(boolean value) {
            return value ? TRUE : FALSE;
        }

        static Truth allOf(List<Truth> operands) {
            if (operands.contains(FALSE)) {
                return FALSE;
            }
            return operands.contains(UNKNOWN) ? UNKNOWN : TRUE;
        }

        static Truth anyOf(List<Truth> operands) {
            if (operands.contains(TRUE)) {
                return TRUE;
            }
            return operands.contains(UNKNOWN) ? UNKNOWN : FALSE;
        }
    }

    /**
     * What is known of the event a query is evaluated against.
     *
     * @param fullDocumentAbsent whether the event carries no {@code fullDocument}
     * @param operationType      the event's operation type, or {@code null} when any
     */
    private record EventShape(boolean fullDocumentAbsent, String operationType) {
        static final EventShape ANY = new EventShape(false, null);
        static final EventShape UPDATE_WITH_FULL_DOCUMENT = new EventShape(false, UPDATE);
        static final EventShape UPDATE_WITHOUT_FULL_DOCUMENT = new EventShape(true, UPDATE);
    }

    /**
     * What the {@code $match} stages of a pipeline tell.
     *
     * @param unsatisfiableFields             paths outside a change event whose condition can never be
     *                                        satisfied where it stands, in order
     * @param matchesNothing                  whether a {@code $match} stage is certainly false for every
     *                                        event, so the stream receives none
     * @param dropsUpdatesWithoutFullDocument whether the {@code $match} stages, taken together, certainly
     *                                        reject an update event for the sole reason that it carries
     *                                        no {@code fullDocument}
     * @param matchesOnEventId                whether a condition is on the event's {@code _id}
     */
    record Findings(Set<String> unsatisfiableFields, boolean matchesNothing,
                    boolean dropsUpdatesWithoutFullDocument, boolean matchesOnEventId) {
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

            if (!findings.unsatisfiableFields().isEmpty()) {
                String first = findings.unsatisfiableFields().iterator().next();
                log.warn("Stream '{}': the pipeline has a $match condition on {} that can never be satisfied — "
                                + "not a field of a change event, so the condition is evaluated on a missing "
                                + "field. {} The stages apply to the change event, in which the document sits "
                                + "under 'fullDocument': write '{}.{}', or declare the pipeline as an "
                                + "Aggregation with scope = PipelineScope.DOCUMENT.",
                        stream, findings.unsatisfiableFields(),
                        findings.matchesNothing()
                                ? "As written, the filter selects no event and the stream will receive none."
                                : "The part of the filter that relies on it selects no event.",
                        ChangeEventFields.FULL_DOCUMENT, first);
            }
            if (scope == PipelineScope.DOCUMENT && findings.matchesOnEventId()) {
                log.warn("Stream '{}': the pipeline has a $match on '_id' with scope = DOCUMENT. '_id' is a "
                        + "field of the change event (its resume token) and is not prefixed: the condition "
                        + "does not compare the document's id. Write 'documentKey._id' (present on every "
                        + "data event, deletes included) or 'fullDocument._id'.", stream);
            }
            if (findings.dropsUpdatesWithoutFullDocument()
                    && def.config() != null
                    && def.config().fullDocument() == FullDocumentMode.DEFAULT
                    && def.typedHandlers() != null
                    && def.typedHandlers().containsKey(OperationType.UPDATE)) {
                log.warn("Stream '{}': the pipeline has a $match on 'fullDocument' that an event without "
                        + "fullDocument cannot satisfy, and the stream uses fullDocument = DEFAULT with an "
                        + "onUpdate handler. Update events carry no fullDocument in DEFAULT mode, so the "
                        + "filter drops them server-side and the handler does not run. Set fullDocument = "
                        + "FullDocumentMode.UPDATE_LOOKUP.", stream);
            }
        } catch (RuntimeException e) {
            log.debug("Pipeline inspection skipped for stream '{}'", def != null ? def.streamName() : "?", e);
        }
    }

    static Findings analyze(List<Document> pipeline) {
        Set<String> unsatisfiable = new LinkedHashSet<>();
        boolean[] eventId = new boolean[1];
        // Successive $match stages are a conjunction: each shape is evaluated over all of them.
        List<Truth> anyEvent = new ArrayList<>();
        List<Truth> updateWithoutFullDocument = new ArrayList<>();
        List<Truth> updateWithFullDocument = new ArrayList<>();
        for (Document stage : pipeline) {
            if (stage == null || stage.size() != 1) {
                break;
            }
            String operator = stage.keySet().iterator().next();
            if (!ChangeEventFields.SHAPE_PRESERVING_STAGES.contains(operator)) {
                break; // reshaped, or a stage this check does not know: stop rather than guess
            }
            if ("$match".equals(operator) && stage.get(operator) instanceof Map<?, ?> query) {
                collect(query, false, unsatisfiable, eventId);
                anyEvent.add(evaluate(query, EventShape.ANY));
                updateWithoutFullDocument.add(evaluate(query, EventShape.UPDATE_WITHOUT_FULL_DOCUMENT));
                updateWithFullDocument.add(evaluate(query, EventShape.UPDATE_WITH_FULL_DOCUMENT));
            }
        }
        boolean matchesNothing = Truth.allOf(anyEvent) == Truth.FALSE;
        // Attributed to the missing fullDocument only if the same update event would not certainly
        // be rejected with one: {operationType: "insert"}, in this stage or another, drops updates
        // either way.
        boolean dropsUpdates = Truth.allOf(updateWithoutFullDocument) == Truth.FALSE
                && Truth.allOf(updateWithFullDocument) != Truth.FALSE;
        return new Findings(Collections.unmodifiableSet(unsatisfiable), matchesNothing, dropsUpdates, eventId[0]);
    }

    /**
     * Collects the conditions on a path outside the event that cannot be
     * satisfied where they stand: always false, or always true under a
     * {@code $nor} ({@code negated}).
     */
    private static void collect(Map<?, ?> query, boolean negated, Set<String> unsatisfiable, boolean[] eventId) {
        for (Map.Entry<?, ?> entry : query.entrySet()) {
            String key = String.valueOf(entry.getKey());
            if (ChangeEventFields.LOGICAL_OPERATORS.contains(key)) {
                if (entry.getValue() instanceof Iterable<?> operands) {
                    boolean nestedNegated = "$nor".equals(key) != negated;
                    for (Object operand : operands) {
                        if (operand instanceof Map<?, ?> nested) {
                            collect(nested, nestedNegated, unsatisfiable, eventId);
                        }
                    }
                }
            } else if (key.startsWith("$")) {
                continue; // $expr, $comment, $jsonSchema, …: not a field path
            } else if (!ChangeEventFields.isTopLevel(key)) {
                Truth truth = onMissingField(entry.getValue());
                if (truth == (negated ? Truth.TRUE : Truth.FALSE)) {
                    unsatisfiable.add(key);
                }
            } else if (ChangeEventFields.EVENT_ID.equals(ChangeEventFields.firstSegment(key))) {
                eventId[0] = true;
            }
        }
    }

    /** Evaluates a query (the implicit conjunction of its entries) against what is known of the event. */
    private static Truth evaluate(Map<?, ?> query, EventShape shape) {
        List<Truth> results = new ArrayList<>(query.size());
        for (Map.Entry<?, ?> entry : query.entrySet()) {
            String key = String.valueOf(entry.getKey());
            Object value = entry.getValue();
            if (ChangeEventFields.LOGICAL_OPERATORS.contains(key)) {
                results.add(evaluateLogical(key, value, shape));
            } else if (key.startsWith("$")) {
                results.add(NEUTRAL.contains(key) ? Truth.TRUE : Truth.UNKNOWN);
            } else if (isMissing(key, shape)) {
                results.add(onMissingField(value));
            } else if (OPERATION_TYPE.equals(key) && shape.operationType() != null) {
                results.add(onKnownString(value, shape.operationType()));
            } else {
                results.add(Truth.UNKNOWN);
            }
        }
        return Truth.allOf(results);
    }

    private static Truth evaluateLogical(String operator, Object value, EventShape shape) {
        if (!(value instanceof Iterable<?> operands)) {
            return Truth.UNKNOWN;
        }
        List<Truth> results = new ArrayList<>();
        for (Object operand : operands) {
            results.add(operand instanceof Map<?, ?> nested ? evaluate(nested, shape) : Truth.UNKNOWN);
        }
        return switch (operator) {
            case "$and" -> Truth.allOf(results);
            case "$or" -> Truth.anyOf(results);
            default -> Truth.anyOf(results).not(); // $nor
        };
    }

    private static boolean isMissing(String path, EventShape shape) {
        String first = ChangeEventFields.firstSegment(path);
        return !ChangeEventFields.TOP_LEVEL.contains(first)
                || (shape.fullDocumentAbsent() && ChangeEventFields.FULL_DOCUMENT.equals(first));
    }

    /**
     * The outcome of a field condition when the field is missing, as MongoDB
     * evaluates it: a missing field equals {@code null} and nothing else.
     */
    static Truth onMissingField(Object condition) {
        if (isNull(condition)) {
            return Truth.TRUE;
        }
        if (!(condition instanceof Map<?, ?> operators) || !isOperatorDocument(operators)) {
            return Truth.FALSE; // a non-null literal: a value, a document, an array, a regex
        }
        List<Truth> results = new ArrayList<>(operators.size());
        for (Map.Entry<?, ?> entry : operators.entrySet()) {
            results.add(onMissingField(String.valueOf(entry.getKey()), entry.getValue()));
        }
        return Truth.allOf(results);
    }

    private static Truth onMissingField(String operator, Object operand) {
        if (NEUTRAL.contains(operator)) {
            return Truth.TRUE;
        }
        if (NEVER_ON_MISSING.contains(operator)) {
            return Truth.FALSE;
        }
        return switch (operator) {
            case "$exists" -> truthiness(operand).not();
            case "$eq", "$gte", "$lte" -> Truth.of(isNull(operand));
            case "$ne" -> Truth.of(!isNull(operand));
            case "$in" -> operand instanceof Iterable<?> values ? Truth.of(containsNull(values)) : Truth.UNKNOWN;
            case "$nin" -> operand instanceof Iterable<?> values ? Truth.of(!containsNull(values)) : Truth.UNKNOWN;
            case "$not" -> {
                if (operand instanceof Pattern) {
                    yield Truth.TRUE; // a regex never matches a missing field
                }
                yield operand instanceof Map<?, ?> nested && isOperatorDocument(nested)
                        ? onMissingField(nested).not() : Truth.UNKNOWN;
            }
            default -> Truth.UNKNOWN;
        };
    }

    /** The outcome of a condition on a field whose string value is known. */
    private static Truth onKnownString(Object condition, String actual) {
        if (condition instanceof String expected) {
            return Truth.of(expected.equals(actual));
        }
        if (!(condition instanceof Map<?, ?> operators) || !isOperatorDocument(operators)) {
            return Truth.UNKNOWN;
        }
        List<Truth> results = new ArrayList<>(operators.size());
        for (Map.Entry<?, ?> entry : operators.entrySet()) {
            Object operand = entry.getValue();
            results.add(switch (String.valueOf(entry.getKey())) {
                case "$eq" -> operand instanceof String s ? Truth.of(s.equals(actual)) : Truth.UNKNOWN;
                case "$ne" -> operand instanceof String s ? Truth.of(!s.equals(actual)) : Truth.UNKNOWN;
                case "$in" -> oneOf(operand, actual);
                case "$nin" -> oneOf(operand, actual).not();
                default -> Truth.UNKNOWN;
            });
        }
        return Truth.allOf(results);
    }

    private static Truth oneOf(Object operand, String actual) {
        if (!(operand instanceof Iterable<?> values)) {
            return Truth.UNKNOWN;
        }
        boolean onlyStrings = true;
        for (Object value : values) {
            if (actual.equals(value)) {
                return Truth.TRUE;
            }
            onlyStrings &= value instanceof String;
        }
        return onlyStrings ? Truth.FALSE : Truth.UNKNOWN; // a regex in the list could still match
    }

    private static Truth truthiness(Object operand) {
        if (operand instanceof Boolean flag) {
            return Truth.of(flag);
        }
        if (operand instanceof Number number) {
            return Truth.of(number.doubleValue() != 0);
        }
        return Truth.UNKNOWN;
    }

    private static boolean isOperatorDocument(Map<?, ?> document) {
        if (document.isEmpty()) {
            return false;
        }
        for (Object key : document.keySet()) {
            if (!String.valueOf(key).startsWith("$")) {
                return false;
            }
        }
        return true;
    }

    private static boolean containsNull(Iterable<?> values) {
        for (Object value : values) {
            if (isNull(value)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isNull(Object value) {
        return value == null || value instanceof BsonNull;
    }
}
