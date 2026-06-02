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
import io.flowwarden.stream.annotation.RetryPolicy;

import java.util.Map;

/**
 * Holds everything needed to start a Change Stream subscription.
 *
 * <p>This class is internal and not part of the public API.</p>
 *
 * @param streamName      resolved stream name
 * @param collection      MongoDB collection to watch
 * @param database        MongoDB database (empty string = Spring default)
 * @param bean            the Spring bean instance
 * @param onChangeHandler the {@code @OnChange} handler (nullable if typed handlers exist)
 * @param typedHandlers   map of operation-type-specific handlers ({@code @OnInsert}, etc.)
 * @param config                  the stream configuration (extracted from annotation or builder)
 * @param pipelineMethod          the {@code @Pipeline} method (nullable if no pipeline defined)
 * @param filterMethod            the {@code @Filter} method (nullable if no filter defined)
 * @param checkpointAnnotation    the {@code @Checkpoint} annotation (nullable if not present)
 * @param retryPolicyAnnotation   the {@code @RetryPolicy} annotation (nullable if not present)
 * @param deadLetterQueueAnnotation the {@code @DeadLetterQueue} annotation (nullable if not present)
 * @param errorHandlerResolver    the resolver for {@code @OnError} handlers (never null)
 * @param metadata                extensible metadata map for external modules (e.g., Javers handler names)
 */
public record ChangeStreamDefinition(
        String streamName,
        String collection,
        String database,
        String zone,
        Object bean,
        HandlerMethod onChangeHandler,
        Map<OperationType, HandlerMethod> typedHandlers,
        StreamConfig config,
        PipelineMethod pipelineMethod,
        FilterMethod filterMethod,
        Checkpoint checkpointAnnotation,
        RetryPolicy retryPolicyAnnotation,
        DeadLetterQueue deadLetterQueueAnnotation,
        ErrorHandlerResolver errorHandlerResolver,
        Map<String, Object> metadata) {

    /**
     * Resolves the best handler for a given operation type.
     *
     * <p>Priority: typed handler &gt; {@code @OnChange} catch-all.</p>
     *
     * @param opType the operation type of the current event
     * @return the handler to invoke, or {@code null} if no handler matches (skip silently)
     */
    public HandlerMethod resolveHandler(OperationType opType) {
        HandlerMethod typed = typedHandlers.get(opType);
        if (typed != null) {
            return typed;
        }
        return onChangeHandler;
    }
}
