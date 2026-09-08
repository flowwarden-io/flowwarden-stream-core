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
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.annotation.DeadLetterQueue;
import io.flowwarden.stream.annotation.MongoDlqOptions;
import io.flowwarden.stream.annotation.RetryPolicy;
import io.flowwarden.stream.registration.CheckpointSpec;
import io.flowwarden.stream.registration.DeadLetterQueueSpec;
import io.flowwarden.stream.registration.MongoDlqOptionsSpec;
import io.flowwarden.stream.registration.RetryPolicySpec;
import io.flowwarden.stream.registration.StreamSpec;
import io.flowwarden.stream.registration.TypedHandler;
import org.springframework.core.annotation.AnnotationUtils;

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Converts a public, annotation-free {@link StreamSpec} into the internal
 * {@link ChangeStreamDefinition} model the stream managers already know how to run.
 *
 * <p>{@link Checkpoint}, {@link RetryPolicy}, {@link DeadLetterQueue} and
 * {@link MongoDlqOptions} instances are synthesized via
 * {@link AnnotationUtils#synthesizeAnnotation(Map, Class, java.lang.reflect.AnnotatedElement)}
 * rather than requiring {@code ChangeStreamDefinition} to stop holding real annotation
 * instances — a synthesized annotation behaves identically to a real one for every
 * downstream consumer (managers, actuator, health indicator), so none of them need to
 * change to support contributed streams.</p>
 *
 * <p>This class is internal and not part of the public API.</p>
 */
final class StreamSpecConverter {

    private StreamSpecConverter() {
    }

    static ChangeStreamDefinition convert(StreamSpec<?> spec, Object contributorBean, String beanName) {
        String collection = StreamDefinitionValidator.resolveCollection(beanName,
                "contributed stream '" + spec.name() + "'", spec.collection(), spec.documentType());

        Map<OperationType, HandlerMethod> typedHandlers = new EnumMap<>(OperationType.class);
        spec.typedHandlers().forEach((opType, handler) -> typedHandlers.put(opType, toHandlerMethod(handler)));
        HandlerMethod onChangeHandler = spec.onChangeHandler().map(StreamSpecConverter::toHandlerMethod).orElse(null);

        StreamConfig config = new StreamConfig(spec.enabled(), spec.autoStart(), spec.documentType(),
                spec.mongoTemplateRef(), spec.fullDocument(), spec.fullDocumentBeforeChange(), spec.deploymentMode());

        Checkpoint checkpoint = spec.checkpoint().map(StreamSpecConverter::synthesizeCheckpoint).orElse(null);
        RetryPolicy retryPolicy = spec.retryPolicy().map(StreamSpecConverter::synthesizeRetryPolicy).orElse(null);
        DeadLetterQueue deadLetterQueue = spec.deadLetterQueue()
                .map(StreamSpecConverter::synthesizeDeadLetterQueue).orElse(null);
        MongoDlqOptions mongoDlqOptions = spec.mongoDlqOptions()
                .map(StreamSpecConverter::synthesizeMongoDlqOptions).orElse(null);

        return new ChangeStreamDefinition(
                spec.name(),
                collection,
                spec.database(),
                "",
                contributorBean,
                onChangeHandler,
                Collections.unmodifiableMap(typedHandlers),
                config,
                null,
                null,
                checkpoint,
                retryPolicy,
                deadLetterQueue,
                mongoDlqOptions,
                new ErrorHandlerResolver(List.of()),
                Collections.emptyMap());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static HandlerMethod toHandlerMethod(TypedHandler<?> handler) {
        if (handler instanceof TypedHandler.Context<?> h) {
            return HandlerMethod.fromContextHandler(h.handler());
        }
        if (handler instanceof TypedHandler.Document<?> h) {
            return HandlerMethod.fromDocumentHandler((io.flowwarden.stream.core.DocumentHandler) h.handler());
        }
        if (handler instanceof TypedHandler.ReactiveContext<?> h) {
            return HandlerMethod.fromReactiveContextHandler(h.handler());
        }
        if (handler instanceof TypedHandler.ReactiveDocument<?> h) {
            return HandlerMethod.fromReactiveDocumentHandler(
                    (io.flowwarden.stream.core.ReactiveDocumentHandler) h.handler());
        }
        throw new IllegalStateException("Unknown TypedHandler implementation: " + handler.getClass());
    }

    private static Checkpoint synthesizeCheckpoint(CheckpointSpec spec) {
        return AnnotationUtils.synthesizeAnnotation(Map.of(
                "saveEveryN", spec.saveEveryN(),
                "saveIntervalSeconds", spec.saveIntervalSeconds(),
                "idleHeartbeatIntervalSeconds", spec.idleHeartbeatIntervalSeconds(),
                "startPosition", spec.startPosition(),
                "onHistoryLost", spec.onHistoryLost()
        ), Checkpoint.class, null);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static RetryPolicy synthesizeRetryPolicy(RetryPolicySpec spec) {
        Class<? extends Throwable>[] retryOn = spec.retryOn().toArray(new Class[0]);
        Class<? extends Throwable>[] noRetryOn = spec.noRetryOn().toArray(new Class[0]);
        return AnnotationUtils.synthesizeAnnotation(Map.of(
                "maxAttempts", spec.maxAttempts(),
                "initialDelay", spec.initialDelay(),
                "maxDelay", spec.maxDelay(),
                "multiplier", spec.multiplier(),
                "retryOn", retryOn,
                "noRetryOn", noRetryOn,
                "jitter", spec.jitter()
        ), RetryPolicy.class, null);
    }

    private static DeadLetterQueue synthesizeDeadLetterQueue(DeadLetterQueueSpec spec) {
        return AnnotationUtils.synthesizeAnnotation(Map.of(
                "enabled", spec.enabled(),
                "retentionDays", spec.retentionDays(),
                "includeOriginalDocument", spec.includeOriginalDocument(),
                "includeStackTrace", spec.includeStackTrace()
        ), DeadLetterQueue.class, null);
    }

    private static MongoDlqOptions synthesizeMongoDlqOptions(MongoDlqOptionsSpec spec) {
        return AnnotationUtils.synthesizeAnnotation(Map.of(
                "collection", spec.collection()
        ), MongoDlqOptions.class, null);
    }
}
