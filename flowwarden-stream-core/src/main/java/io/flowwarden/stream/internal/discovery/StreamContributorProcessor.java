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

import io.flowwarden.stream.FullDocumentBeforeChangeMode;
import io.flowwarden.stream.FullDocumentMode;
import io.flowwarden.stream.OperationType;
import io.flowwarden.stream.registration.StreamDefinitionContributor;
import io.flowwarden.stream.registration.StreamRegistration;
import io.flowwarden.stream.registration.StreamSpec;
import io.flowwarden.stream.registration.TypedHandler;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Discovers {@link StreamDefinitionContributor} beans and registers the streams they
 * declare, once every singleton bean — including {@code @ChangeStream}-annotated ones,
 * discovered by {@link ChangeStreamBeanPostProcessor} — has been created.
 *
 * <p>Runs as a {@link SmartInitializingSingleton}: strictly after all singletons are
 * initialized, strictly before the stream managers read {@link StreamRegistry#getDefinitions()}
 * on {@code ApplicationReadyEvent}. This is a bootstrap-only contribution point — a
 * contributor added or changed after the application context has started has no effect.</p>
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public class StreamContributorProcessor implements SmartInitializingSingleton {

    private final ApplicationContext applicationContext;
    private final StreamRegistry registry;

    public StreamContributorProcessor(ApplicationContext applicationContext, StreamRegistry registry) {
        this.applicationContext = applicationContext;
        this.registry = registry;
    }

    @Override
    public void afterSingletonsInstantiated() {
        Map<String, StreamDefinitionContributor> contributors =
                applicationContext.getBeansOfType(StreamDefinitionContributor.class);
        if (contributors.isEmpty()) {
            return;
        }

        String mode = applicationContext.getEnvironment()
                .getProperty("flowwarden.default-mode", "IMPERATIVE").toUpperCase();

        Set<String> streamNames = new HashSet<>();
        for (ChangeStreamDefinition existing : registry.getDefinitions()) {
            streamNames.add(existing.streamName());
        }

        contributors.forEach((beanName, contributor) -> {
            StreamRegistration registration = new StreamRegistration();
            contributor.contribute(registration);

            for (StreamSpec<?> spec : registration.streams()) {
                validate(spec, beanName, mode);

                if (!streamNames.add(spec.name())) {
                    throw new BeanCreationException(beanName,
                            "Stream name '" + spec.name()
                                    + "' is already registered — by an @ChangeStream class or another "
                                    + "StreamDefinitionContributor. Stream names must be unique regardless of origin.");
                }

                registry.register(StreamSpecConverter.convert(spec, contributor, beanName));
            }
        });
    }

    private void validate(StreamSpec<?> spec, String beanName, String mode) {
        String subject = "contributed stream '" + spec.name() + "'";

        // Fail fast on the same collection-resolution rule as the annotation path (throws when
        // documentType is the raw Document.class and no explicit collection was given).
        StreamDefinitionValidator.resolveCollection(beanName, subject, spec.collection(), spec.documentType());

        spec.checkpoint().ifPresent(cp -> StreamDefinitionValidator.validateCheckpoint(beanName, subject,
                cp.saveEveryN(), cp.saveIntervalSeconds(), cp.idleHeartbeatIntervalSeconds()));

        spec.retryPolicy().ifPresent(rp -> StreamDefinitionValidator.validateRetryPolicy(beanName, subject,
                rp.maxAttempts(), rp.multiplier(), rp.initialDelay(), rp.maxDelay()));

        spec.deadLetterQueue().ifPresent(dlq -> StreamDefinitionValidator.validateDeadLetterQueue(beanName, subject,
                dlq.retentionDays()));

        StreamDefinitionValidator.warnMongoDlqOptionsWithoutDlq(subject, spec.mongoDlqOptions().isPresent(),
                spec.deadLetterQueue().isPresent());

        StreamDefinitionValidator.validateAtLeastOneHandler(beanName, subject,
                spec.onChangeHandler().isPresent(), !spec.typedHandlers().isEmpty());

        boolean updateExpectsDocument = spec.handler(OperationType.UPDATE)
                .map(StreamContributorProcessor::expectsDocument)
                .orElse(false);
        StreamDefinitionValidator.warnFullDocumentDefaultWithTypedUpdate(subject,
                spec.fullDocument() == FullDocumentMode.DEFAULT, updateExpectsDocument);

        if (spec.fullDocumentBeforeChange() != FullDocumentBeforeChangeMode.OFF) {
            boolean hasOnlyInsertHandlers = !spec.typedHandlers().isEmpty()
                    && spec.typedHandlers().keySet().stream().allMatch(op -> op == OperationType.INSERT);
            StreamDefinitionValidator.warnFullDocumentBeforeChangeWithInsertOnly(subject,
                    spec.fullDocumentBeforeChange().toString(), hasOnlyInsertHandlers);
        }

        StreamDefinitionValidator.validateMongoTemplateRef(beanName, subject, spec.mongoTemplateRef(),
                applicationContext);

        spec.typedHandlers().forEach((opType, handler) -> StreamDefinitionValidator.validateHandlerReturnMode(
                beanName, subject, "on" + StreamDefinitionValidator.capitalize(opType.name()), mode,
                isReactive(handler)));
        spec.onChangeHandler().ifPresent(handler -> StreamDefinitionValidator.validateHandlerReturnMode(
                beanName, subject, "onChange", mode, isReactive(handler)));
    }

    private static boolean expectsDocument(TypedHandler<?> handler) {
        return handler instanceof TypedHandler.Document<?> || handler instanceof TypedHandler.ReactiveDocument<?>;
    }

    private static boolean isReactive(TypedHandler<?> handler) {
        return handler instanceof TypedHandler.ReactiveContext<?> || handler instanceof TypedHandler.ReactiveDocument<?>;
    }
}
