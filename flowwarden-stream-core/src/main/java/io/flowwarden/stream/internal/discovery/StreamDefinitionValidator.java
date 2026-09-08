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

import io.flowwarden.stream.OperationType;
import io.flowwarden.stream.internal.retry.RetryPolicyConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Fail-fast validation rules for a Change Stream definition, shared between the
 * annotation-discovery path ({@link ChangeStreamBeanPostProcessor}) and the
 * programmatic contribution path ({@code StreamContributorProcessor}).
 *
 * <p>Every method here operates on plain values rather than annotation instances or
 * reflective {@link java.lang.reflect.Method} objects, so both discovery paths can run
 * the exact same rules and get the exact same class of error — an annotation with an
 * invalid {@code @RetryPolicy} and a contributed {@code StreamSpec} with the equivalent
 * invalid retry config fail for the same reason.</p>
 *
 * <p>This class is internal and not part of the public API.</p>
 */
final class StreamDefinitionValidator {

    private static final Logger log = LoggerFactory.getLogger(StreamDefinitionValidator.class);

    /** Operation types for which MongoDB does not provide a fullDocument. */
    static final Set<OperationType> NO_FULL_DOCUMENT_OPS = Set.of(
            OperationType.DELETE, OperationType.DROP, OperationType.INVALIDATE);

    private StreamDefinitionValidator() {
    }

    static void validateCheckpoint(String beanName, String subject, int saveEveryN,
                                    int saveIntervalSeconds, int idleHeartbeatIntervalSeconds) {
        if (saveIntervalSeconds < 0) {
            throw new BeanCreationException(beanName,
                    "@Checkpoint on " + subject
                            + " has negative saveIntervalSeconds: " + saveIntervalSeconds);
        }
        if (saveEveryN < 1) {
            throw new BeanCreationException(beanName,
                    "@Checkpoint on " + subject
                            + " has saveEveryN=" + saveEveryN
                            + " (must be >= 1; saveEveryN controls how often lastProcessedToken is persisted)");
        }
        if (idleHeartbeatIntervalSeconds < 0) {
            throw new BeanCreationException(beanName,
                    "@Checkpoint on " + subject
                            + " has negative idleHeartbeatIntervalSeconds: "
                            + idleHeartbeatIntervalSeconds);
        }
        if (idleHeartbeatIntervalSeconds == 0) {
            log.warn("@Checkpoint on {} has idleHeartbeatIntervalSeconds=0 — idle probing is "
                            + "disabled, so a stream that stays idle longer than the oplog window "
                            + "loses its resume point (ChangeStreamHistoryLost on restart). "
                            + "Only disable this if the collection is guaranteed to receive "
                            + "regular traffic.",
                    subject);
        }
        if (saveIntervalSeconds == 0 && saveEveryN > 1 && idleHeartbeatIntervalSeconds == 0) {
            log.warn("@Checkpoint on {} has saveIntervalSeconds=0, idleHeartbeatIntervalSeconds=0 "
                            + "and saveEveryN={} — lastSeenToken never advances, so the resume "
                            + "cascade level-2 safety net (fallback to lastSeenToken when "
                            + "lastProcessedToken ages out) will not work.",
                    subject, saveEveryN);
        }
    }

    static void validateRetryPolicy(String beanName, String subject, int maxAttempts,
                                     double multiplier, String initialDelay, String maxDelay) {
        if (maxAttempts < 1) {
            throw new BeanCreationException(beanName,
                    "@RetryPolicy on " + subject
                            + " has invalid maxAttempts: " + maxAttempts
                            + ". Must be >= 1.");
        }
        if (multiplier <= 0) {
            throw new BeanCreationException(beanName,
                    "@RetryPolicy on " + subject
                            + " has invalid multiplier: " + multiplier
                            + ". Must be > 0.");
        }
        try {
            RetryPolicyConfig.parseDuration(initialDelay);
        } catch (IllegalArgumentException e) {
            throw new BeanCreationException(beanName,
                    "@RetryPolicy on " + subject
                            + " has invalid initialDelay: " + initialDelay);
        }
        try {
            RetryPolicyConfig.parseDuration(maxDelay);
        } catch (IllegalArgumentException e) {
            throw new BeanCreationException(beanName,
                    "@RetryPolicy on " + subject
                            + " has invalid maxDelay: " + maxDelay);
        }
    }

    static void validateDeadLetterQueue(String beanName, String subject, int retentionDays) {
        if (retentionDays < 0) {
            throw new BeanCreationException(beanName,
                    "@DeadLetterQueue on " + subject
                            + " has negative retentionDays: " + retentionDays
                            + ". Must be >= 0.");
        }
    }

    static void warnMongoDlqOptionsWithoutDlq(String subject, boolean hasMongoDlqOptions, boolean hasDlq) {
        if (hasMongoDlqOptions && !hasDlq) {
            log.warn("@MongoDlqOptions on {} has no effect without @DeadLetterQueue on the same class.",
                    subject);
        }
    }

    static void validateAtLeastOneHandler(String beanName, String subject,
                                           boolean hasOnChangeHandler, boolean hasTypedHandlers) {
        if (!hasOnChangeHandler && !hasTypedHandlers) {
            throw new BeanCreationException(beanName,
                    "Change Stream " + subject
                            + " must have at least one handler (onChange, onInsert, onUpdate, onDelete, or onReplace)");
        }
    }

    /**
     * Rejects combining a filter with typed handlers that cover operations without a
     * fullDocument (DELETE, DROP, INVALIDATE) — the filter predicate cannot safely inspect
     * a document that doesn't exist for those events.
     */
    static void validateFilterCompatibility(String beanName, String subject, Set<OperationType> typedOperations) {
        for (OperationType opType : NO_FULL_DOCUMENT_OPS) {
            if (typedOperations.contains(opType)) {
                throw new BeanCreationException(beanName,
                        "Change Stream " + subject + " declares a filter and an on" + capitalize(opType.name())
                                + " handler, which is not allowed. "
                                + opType + " events have no fullDocument, so the filter predicate "
                                + "cannot safely access the document. "
                                + "Use a server-side pipeline to filter these events, "
                                + "or move the filtering logic into the handler.");
            }
        }
    }

    static void warnFullDocumentDefaultWithTypedUpdate(String subject, boolean fullDocumentIsDefault,
                                                        boolean updateHandlerExpectsDocument) {
        if (fullDocumentIsDefault && updateHandlerExpectsDocument) {
            log.warn("Change Stream {} uses fullDocument = DEFAULT with an onUpdate handler "
                    + "that expects a typed document parameter. UPDATE events with DEFAULT mode do not include "
                    + "the fullDocument — the parameter will be null. "
                    + "Set fullDocument = FullDocumentMode.UPDATE_LOOKUP, "
                    + "or change the onUpdate handler to use ChangeStreamContext.",
                    subject);
        }
    }

    static void warnFullDocumentBeforeChangeWithInsertOnly(String subject, String fullDocumentBeforeChangeMode,
                                                            boolean hasOnlyInsertHandlers) {
        if (hasOnlyInsertHandlers) {
            log.warn("Change Stream {} sets fullDocumentBeforeChange = {} "
                    + "but only has onInsert handlers. INSERT events have no pre-image — "
                    + "the fullDocumentBeforeChange value will always be null. "
                    + "Consider removing fullDocumentBeforeChange or adding onUpdate/onDelete/onReplace handlers.",
                    subject, fullDocumentBeforeChangeMode);
        }
    }

    static void validateMongoTemplateRef(String beanName, String subject, String mongoTemplateRef,
                                          ApplicationContext applicationContext) {
        if (mongoTemplateRef.isEmpty() || applicationContext == null
                || isMongoTemplateRefResolvable(applicationContext, mongoTemplateRef)) {
            return;
        }
        throw new BeanCreationException(beanName,
                "Change Stream " + subject
                        + " references mongoTemplateRef='" + mongoTemplateRef
                        + "' but no bean of type MongoTemplate or ReactiveMongoTemplate"
                        + " with this name was found.");
    }

    /**
     * Verifies that {@code mongoTemplateRef} names a bean that IS a {@code MongoTemplate} or
     * {@code ReactiveMongoTemplate} — not merely that a bean of either type exists somewhere
     * in the context and a bean of any type happens to share the name.
     */
    private static boolean isMongoTemplateRefResolvable(ApplicationContext applicationContext, String mongoTemplateRef) {
        return Arrays.asList(applicationContext.getBeanNamesForType(MongoTemplate.class)).contains(mongoTemplateRef)
                || Arrays.asList(applicationContext.getBeanNamesForType(ReactiveMongoTemplate.class))
                        .contains(mongoTemplateRef);
    }

    static void validateHandlerReturnMode(String beanName, String subject, String handlerLabel,
                                           String mode, boolean isReactiveReturn) {
        if (mode.isEmpty()) {
            return;
        }
        if ("IMPERATIVE".equals(mode) && isReactiveReturn) {
            throw new BeanCreationException(beanName,
                    "Handler " + handlerLabel + " on " + subject
                            + " returns Mono<Void> but flowwarden.default-mode is IMPERATIVE. "
                            + "Mono<Void> signatures are not allowed in IMPERATIVE mode.");
        }
        if ("REACTIVE".equals(mode) && !isReactiveReturn) {
            throw new BeanCreationException(beanName,
                    "Handler " + handlerLabel + " on " + subject
                            + " returns void but flowwarden.default-mode is REACTIVE. "
                            + "Use Mono<Void> return type in REACTIVE mode.");
        }
    }

    /**
     * Fixes issue #86: a document-typed handler parameter is accepted regardless of
     * whether it matches {@code @ChangeStream.documentType()} (or the equivalent
     * {@code StreamSpec} document type). The mismatch used to only surface at runtime, on
     * the first matching event, as a raw {@code IllegalArgumentException} from
     * {@code Method.invoke}. This runs at bean-creation time instead.
     *
     * <p>Only meaningful for the reflection-based annotation path: the builder-based
     * {@code StreamSpec} path is immune to this by construction — its handler setters are
     * generic in the stream's document type, so a mismatched handler fails to compile.</p>
     */
    static void validateTypedHandlerParameterType(String beanName, String subject, String annotationLabel,
                                                   String methodName, Class<?> parameterType,
                                                   Class<?> documentType) {
        if (!parameterType.isAssignableFrom(documentType)) {
            throw new BeanCreationException(beanName,
                    "@" + annotationLabel + " method " + subject + "#" + methodName
                            + " expects a parameter of type " + parameterType.getName()
                            + " but the Change Stream's documentType is " + documentType.getName()
                            + " (" + documentType.getName() + " is not assignable to "
                            + parameterType.getName() + ").");
        }
    }

    /**
     * Resolves a stream's collection the same way for both discovery paths: the explicit
     * value if given, otherwise inferred from {@code documentType} via
     * {@link ChangeStreamBeanPostProcessor#resolveCollectionFromDocumentType(Class)} — and,
     * matching the annotation path exactly, a raw {@code Document.class} documentType with
     * no explicit collection is a fail-fast error, not a silent {@code "document"} collection.
     */
    static String resolveCollection(String beanName, String subject, String explicitCollection,
                                     Class<?> documentType) {
        if (!explicitCollection.isEmpty()) {
            return explicitCollection;
        }
        String inferred = documentType != org.bson.Document.class
                ? ChangeStreamBeanPostProcessor.resolveCollectionFromDocumentType(documentType)
                : "";
        if (inferred.isEmpty()) {
            throw new BeanCreationException(beanName,
                    subject + " must specify a collection or a documentType with @Document");
        }
        return inferred;
    }

    static String capitalize(String name) {
        if (name == null || name.isEmpty()) {
            return name;
        }
        return name.charAt(0) + name.substring(1).toLowerCase();
    }

    static Set<OperationType> intersectWithNoFullDocumentOps(Set<OperationType> operations) {
        return operations.stream().filter(NO_FULL_DOCUMENT_OPS::contains).collect(Collectors.toSet());
    }
}
