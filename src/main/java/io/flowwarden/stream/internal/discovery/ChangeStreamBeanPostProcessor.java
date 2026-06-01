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

import io.flowwarden.stream.ChangeStreamContext;
import io.flowwarden.stream.DeploymentMode;
import io.flowwarden.stream.FullDocumentBeforeChangeMode;
import io.flowwarden.stream.FullDocumentMode;
import io.flowwarden.stream.OperationType;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.annotation.DeadLetterQueue;
import io.flowwarden.stream.annotation.OnChange;
import io.flowwarden.stream.annotation.RetryPolicy;
import io.flowwarden.stream.internal.retry.RetryPolicyConfig;
import io.flowwarden.stream.annotation.OnDelete;
import io.flowwarden.stream.annotation.OnError;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.annotation.OnReplace;
import io.flowwarden.stream.annotation.OnUpdate;
import io.flowwarden.stream.annotation.Filter;
import io.flowwarden.stream.annotation.Pipeline;
import io.flowwarden.stream.ErrorAction;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;

import reactor.core.publisher.Mono;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;

/**
 * Discovers beans annotated with {@link ChangeStream} and validates their
 * handler methods ({@link OnChange}, {@link OnInsert}, {@link OnUpdate},
 * {@link OnDelete}, {@link OnReplace}).
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public class ChangeStreamBeanPostProcessor implements BeanPostProcessor, ApplicationContextAware, StreamRegistry, Ordered {

    private static final Logger log = LoggerFactory.getLogger(ChangeStreamBeanPostProcessor.class);

    private static final Map<Class<? extends Annotation>, OperationType> TYPED_ANNOTATIONS = Map.of(
            OnInsert.class, OperationType.INSERT,
            OnUpdate.class, OperationType.UPDATE,
            OnDelete.class, OperationType.DELETE,
            OnReplace.class, OperationType.REPLACE
    );

    /** Operation types for which MongoDB does not provide a fullDocument. */
    private static final Set<OperationType> NO_FULL_DOCUMENT_OPS = Set.of(
            OperationType.DELETE, OperationType.DROP, OperationType.INVALIDATE
    );

    private final List<ChangeStreamDefinition> definitions = new CopyOnWriteArrayList<>();
    private ApplicationContext applicationContext;

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Class<?> targetClass = AopUtils.getTargetClass(bean);
        ChangeStream annotation = AnnotationUtils.findAnnotation(targetClass, ChangeStream.class);
        if (annotation == null) {
            return bean;
        }

        Checkpoint cpAnnotation = AnnotationUtils.findAnnotation(targetClass, Checkpoint.class);
        if (cpAnnotation != null) {
            if (cpAnnotation.saveIntervalSeconds() < 0) {
                throw new BeanCreationException(beanName,
                        "@Checkpoint on " + targetClass.getName()
                                + " has negative saveIntervalSeconds: " + cpAnnotation.saveIntervalSeconds());
            }
            if (cpAnnotation.saveEveryN() < 1) {
                throw new BeanCreationException(beanName,
                        "@Checkpoint on " + targetClass.getName()
                                + " has saveEveryN=" + cpAnnotation.saveEveryN()
                                + " (must be >= 1; saveEveryN controls how often lastProcessedToken is persisted)");
            }
            if (cpAnnotation.saveIntervalSeconds() == 0 && cpAnnotation.saveEveryN() > 1) {
                log.warn("@Checkpoint on {} has saveIntervalSeconds=0 and saveEveryN={} — "
                                + "the heartbeat timer is disabled, so the resume cascade level-2 safety net "
                                + "(fallback to lastSeenToken when lastProcessedToken ages out) will not work. "
                                + "Consider setting saveIntervalSeconds > 0 or saveEveryN = 1.",
                        targetClass.getName(), cpAnnotation.saveEveryN());
            }
        }

        RetryPolicy retryAnnotation = AnnotationUtils.findAnnotation(targetClass, RetryPolicy.class);
        if (retryAnnotation != null) {
            validateRetryPolicy(retryAnnotation, targetClass, beanName);
        }

        DeadLetterQueue dlqAnnotation = AnnotationUtils.findAnnotation(targetClass, DeadLetterQueue.class);
        if (dlqAnnotation != null) {
            validateDeadLetterQueue(dlqAnnotation, targetClass, beanName);
        }

        HandlerMethod onChangeHandler = findOnChangeHandler(targetClass, beanName, annotation);
        Map<OperationType, HandlerMethod> typedHandlers = findTypedHandlers(targetClass, beanName, annotation);
        PipelineMethod pipelineMethod = findPipelineMethod(targetClass, beanName);
        FilterMethod filterMethod = findFilterMethod(targetClass, beanName);
        ErrorHandlerResolver errorHandlerResolver = findErrorHandlers(targetClass, beanName);

        // Warn when a method combines annotations with and without fullDocument
        warnMixedFullDocumentAnnotations(typedHandlers, targetClass);

        // Validate: at least one handler required
        if (onChangeHandler == null && typedHandlers.isEmpty()) {
            throw new BeanCreationException(beanName,
                    "@ChangeStream class " + targetClass.getName()
                            + " must have at least one handler method (@OnChange, @OnInsert, @OnUpdate, @OnDelete, or @OnReplace)");
        }

        // Validate: @Filter is incompatible with handlers that cover operations without fullDocument
        if (filterMethod != null) {
            validateFilterCompatibility(filterMethod, onChangeHandler, typedHandlers,
                    targetClass, beanName);
        }

        // Validate: fullDocument = DEFAULT with typed @OnUpdate handler
        validateFullDocumentCompatibility(annotation, typedHandlers, targetClass, beanName);

        // Validate: fullDocumentBeforeChange with only INSERT handlers (no pre-image)
        validateFullDocumentBeforeChangeCompatibility(annotation, typedHandlers, targetClass, beanName);

        // Validate deployment mode
        if (annotation.deploymentMode() == DeploymentMode.PARTITIONED) {
            throw new BeanCreationException(beanName,
                    "@ChangeStream on " + targetClass.getName()
                            + " uses deploymentMode = PARTITIONED which is not yet implemented.");
        }

        String streamName = resolveStreamName(annotation, targetClass);
        String collection = resolveCollection(annotation, targetClass, beanName);
        validateMongoTemplateRef(annotation, targetClass, beanName);

        // Make all handler methods accessible
        if (onChangeHandler != null) {
            onChangeHandler.method().setAccessible(true);
        }
        for (HandlerMethod hm : typedHandlers.values()) {
            hm.method().setAccessible(true);
        }
        if (pipelineMethod != null) {
            pipelineMethod.method().setAccessible(true);
        }
        if (filterMethod != null) {
            filterMethod.method().setAccessible(true);
        }

        // Validate handler return types against configured mode
        if (applicationContext != null) {
            String mode = applicationContext.getEnvironment()
                    .getProperty("flowwarden.default-mode", "IMPERATIVE").toUpperCase();
            validateHandlerReturnTypes(mode, beanName, targetClass, onChangeHandler, typedHandlers);
        }

        StreamConfig config = StreamConfig.fromAnnotation(annotation);

        ChangeStreamDefinition definition = new ChangeStreamDefinition(
                streamName,
                collection,
                annotation.database(),
                annotation.zone(),
                bean,
                onChangeHandler,
                Collections.unmodifiableMap(typedHandlers),
                config,
                pipelineMethod,
                filterMethod,
                cpAnnotation,
                retryAnnotation,
                dlqAnnotation,
                errorHandlerResolver,
                Collections.emptyMap());

        definitions.add(definition);

        // Build log message
        StringBuilder handlers = new StringBuilder();
        if (onChangeHandler != null) {
            handlers.append("@OnChange=").append(onChangeHandler.method().getName());
        }
        for (var entry : typedHandlers.entrySet()) {
            if (!handlers.isEmpty()) handlers.append(", ");
            handlers.append("@On").append(capitalize(entry.getKey().name()))
                    .append("=").append(entry.getValue().method().getName());
        }

        if (pipelineMethod != null) {
            if (!handlers.isEmpty()) handlers.append(", ");
            handlers.append("@Pipeline=").append(pipelineMethod.method().getName());
        }
        if (filterMethod != null) {
            if (!handlers.isEmpty()) handlers.append(", ");
            handlers.append("@Filter=").append(filterMethod.method().getName());
        }

        log.info("Discovered Change Stream '{}' on collection '{}' (handlers: {}, checkpoint: {}, retryPolicy: {}, dlq: {}, onError: {})",
                streamName, collection, handlers, cpAnnotation != null, retryAnnotation != null, dlqAnnotation != null,
                !errorHandlerResolver.isEmpty());

        return bean;
    }

    /**
     * Returns all discovered stream definitions (unmodifiable).
     */
    @Override
    public List<ChangeStreamDefinition> getDefinitions() {
        return Collections.unmodifiableList(definitions);
    }

    @Override
    public void register(ChangeStreamDefinition definition) {
        definitions.add(definition);
    }

    @Override
    public Optional<ChangeStreamDefinition> findByName(String streamName) {
        return definitions.stream()
                .filter(d -> d.streamName().equals(streamName))
                .findFirst();
    }

    // --- Handler discovery ---

    private HandlerMethod findOnChangeHandler(Class<?> targetClass, String beanName, ChangeStream csAnnotation) {
        List<Method> onChangeMethods = new ArrayList<>();
        for (Method method : targetClass.getDeclaredMethods()) {
            if (AnnotationUtils.findAnnotation(method, OnChange.class) != null) {
                onChangeMethods.add(method);
            }
        }

        if (onChangeMethods.isEmpty()) {
            return null; // typed handlers may suffice
        }
        if (onChangeMethods.size() > 1) {
            throw new BeanCreationException(beanName,
                    "@ChangeStream class " + targetClass.getName()
                            + " must have exactly one @OnChange method, found " + onChangeMethods.size());
        }

        Method method = onChangeMethods.get(0);
        validateOnChangeSignature(method, targetClass, beanName);
        boolean reactive = detectReactiveReturn(method, targetClass, beanName);
        return new HandlerMethod(method, HandlerMethod.SignatureStyle.CONTEXT_ONLY, reactive);
    }

    private Map<OperationType, HandlerMethod> findTypedHandlers(Class<?> targetClass, String beanName,
                                                                 ChangeStream csAnnotation) {
        Map<OperationType, HandlerMethod> result = new EnumMap<>(OperationType.class);

        for (var entry : TYPED_ANNOTATIONS.entrySet()) {
            Class<? extends Annotation> annotationType = entry.getKey();
            OperationType opType = entry.getValue();

            List<Method> methods = new ArrayList<>();
            for (Method method : targetClass.getDeclaredMethods()) {
                if (AnnotationUtils.findAnnotation(method, annotationType) != null) {
                    methods.add(method);
                }
            }

            if (methods.isEmpty()) {
                continue;
            }
            if (methods.size() > 1) {
                throw new BeanCreationException(beanName,
                        "@ChangeStream class " + targetClass.getName()
                                + " must have at most one @" + annotationType.getSimpleName()
                                + " method, found " + methods.size());
            }

            Method method = methods.get(0);
            HandlerMethod.SignatureStyle style = resolveTypedSignatureStyle(method, targetClass, beanName,
                    annotationType);

            // Validate documentType for document-typed signatures
            if (style != HandlerMethod.SignatureStyle.CONTEXT_ONLY
                    && csAnnotation.documentType() == Document.class) {
                throw new BeanCreationException(beanName,
                        "@" + annotationType.getSimpleName() + " method "
                                + targetClass.getName() + "#" + method.getName()
                                + " uses a document-typed signature but @ChangeStream.documentType is Document.class. "
                                + "Specify a concrete documentType.");
            }

            boolean reactive = detectReactiveReturn(method, targetClass, beanName);
            result.put(opType, new HandlerMethod(method, style, reactive));
        }

        return result;
    }

    /**
     * Warns when the same method carries annotations that cover both "with fullDocument"
     * operations (INSERT, UPDATE, REPLACE) and "without fullDocument" operations (DELETE, DROP, INVALIDATE).
     */
    private void warnMixedFullDocumentAnnotations(Map<OperationType, HandlerMethod> typedHandlers,
                                                  Class<?> targetClass) {
        // Invert the map: Method → Set<OperationType>
        Map<Method, Set<OperationType>> methodToOps = new HashMap<>();
        for (var entry : typedHandlers.entrySet()) {
            methodToOps.computeIfAbsent(entry.getValue().method(), k -> new HashSet<>())
                    .add(entry.getKey());
        }

        for (var entry : methodToOps.entrySet()) {
            Set<OperationType> ops = entry.getValue();
            if (ops.size() < 2) {
                continue;
            }

            Set<OperationType> withDoc = new HashSet<>(ops);
            withDoc.removeAll(NO_FULL_DOCUMENT_OPS);
            Set<OperationType> withoutDoc = new HashSet<>(ops);
            withoutDoc.retainAll(NO_FULL_DOCUMENT_OPS);

            if (!withDoc.isEmpty() && !withoutDoc.isEmpty()) {
                String withDocNames = withDoc.stream()
                        .map(op -> "@On" + capitalize(op.name()))
                        .sorted()
                        .collect(java.util.stream.Collectors.joining(", "));
                String withoutDocNames = withoutDoc.stream()
                        .map(op -> "@On" + capitalize(op.name()))
                        .sorted()
                        .collect(java.util.stream.Collectors.joining(", "));
                log.warn("Method '{}#{}' combines {} and {} on the same method. "
                                + "{} events have no fullDocument — the document parameter will be null for those events. "
                                + "Consider using separate methods or handle the null case explicitly.",
                        targetClass.getSimpleName(), entry.getKey().getName(),
                        withDocNames, withoutDocNames, withoutDocNames);
            }
        }
    }

    // --- Pipeline discovery ---

    private PipelineMethod findPipelineMethod(Class<?> targetClass, String beanName) {
        List<Method> pipelineMethods = new ArrayList<>();
        for (Method method : targetClass.getDeclaredMethods()) {
            if (AnnotationUtils.findAnnotation(method, Pipeline.class) != null) {
                pipelineMethods.add(method);
            }
        }

        if (pipelineMethods.isEmpty()) {
            return null;
        }
        if (pipelineMethods.size() > 1) {
            throw new BeanCreationException(beanName,
                    "@ChangeStream class " + targetClass.getName()
                            + " must have at most one @Pipeline method, found " + pipelineMethods.size());
        }

        Method method = pipelineMethods.get(0);

        if (method.getParameterCount() != 0) {
            throw new BeanCreationException(beanName,
                    "@Pipeline method " + targetClass.getName() + "#" + method.getName()
                            + " must have no parameters");
        }

        PipelineMethod.ReturnStyle returnStyle = resolvePipelineReturnStyle(method, targetClass, beanName);
        return new PipelineMethod(method, returnStyle);
    }

    private PipelineMethod.ReturnStyle resolvePipelineReturnStyle(Method method, Class<?> targetClass,
                                                                   String beanName) {
        Class<?> returnType = method.getReturnType();

        // Check for Aggregation first (exact type)
        if (Aggregation.class.isAssignableFrom(returnType)) {
            return PipelineMethod.ReturnStyle.AGGREGATION;
        }

        // Check for List return type
        if (java.util.List.class.isAssignableFrom(returnType)) {
            // Inspect generic type argument to distinguish List<Bson> from List<AggregationOperation>
            Type genericReturnType = method.getGenericReturnType();
            if (genericReturnType instanceof ParameterizedType paramType) {
                Type[] typeArgs = paramType.getActualTypeArguments();
                if (typeArgs.length == 1) {
                    Type elementType = typeArgs[0];
                    Class<?> elementClass = elementType instanceof Class<?> cls ? cls : null;
                    if (elementClass != null) {
                        if (AggregationOperation.class.isAssignableFrom(elementClass)) {
                            return PipelineMethod.ReturnStyle.AGGREGATION_OPERATION_LIST;
                        }
                        if (Bson.class.isAssignableFrom(elementClass)) {
                            return PipelineMethod.ReturnStyle.BSON_LIST;
                        }
                    }
                }
            }
            // Raw List — assume Bson
            return PipelineMethod.ReturnStyle.BSON_LIST;
        }

        throw new BeanCreationException(beanName,
                "@Pipeline method " + targetClass.getName() + "#" + method.getName()
                        + " has unsupported return type: " + returnType.getName()
                        + ". Supported: List<Bson>, List<AggregationOperation>, or Aggregation.");
    }

    // --- Filter discovery ---

    private FilterMethod findFilterMethod(Class<?> targetClass, String beanName) {
        List<Method> filterMethods = new ArrayList<>();
        for (Method method : targetClass.getDeclaredMethods()) {
            if (AnnotationUtils.findAnnotation(method, Filter.class) != null) {
                filterMethods.add(method);
            }
        }

        if (filterMethods.isEmpty()) {
            return null;
        }
        if (filterMethods.size() > 1) {
            throw new BeanCreationException(beanName,
                    "@ChangeStream class " + targetClass.getName()
                            + " must have at most one @Filter method, found " + filterMethods.size());
        }

        Method method = filterMethods.get(0);
        FilterMethod.ReturnStyle returnStyle = resolveFilterReturnStyle(method, targetClass, beanName);
        return new FilterMethod(method, returnStyle);
    }

    private FilterMethod.ReturnStyle resolveFilterReturnStyle(Method method, Class<?> targetClass,
                                                               String beanName) {
        Class<?> returnType = method.getReturnType();
        Class<?>[] params = method.getParameterTypes();

        // Style 1: Predicate<ChangeStreamContext<?>> with no parameters
        if (Predicate.class.isAssignableFrom(returnType)) {
            if (params.length != 0) {
                throw new BeanCreationException(beanName,
                        "@Filter method " + targetClass.getName() + "#" + method.getName()
                                + " returns Predicate but must have no parameters");
            }
            return FilterMethod.ReturnStyle.PREDICATE;
        }

        // Style 2: boolean with one ChangeStreamContext<?> parameter
        if (returnType == boolean.class || returnType == Boolean.class) {
            if (params.length != 1 || !ChangeStreamContext.class.isAssignableFrom(params[0])) {
                throw new BeanCreationException(beanName,
                        "@Filter method " + targetClass.getName() + "#" + method.getName()
                                + " returns boolean but must have exactly one ChangeStreamContext parameter");
            }
            return FilterMethod.ReturnStyle.BOOLEAN_METHOD;
        }

        throw new BeanCreationException(beanName,
                "@Filter method " + targetClass.getName() + "#" + method.getName()
                        + " has unsupported return type: " + returnType.getName()
                        + ". Supported: Predicate<ChangeStreamContext<?>> (no params) or boolean (with ChangeStreamContext param).");
    }

    // --- @OnError discovery ---

    private ErrorHandlerResolver findErrorHandlers(Class<?> targetClass, String beanName) {
        List<ErrorHandlerMethod> handlers = new ArrayList<>();
        Set<Class<? extends Throwable>> seenExactTypes = new HashSet<>();
        boolean hasCatchAll = false;

        for (Method method : targetClass.getDeclaredMethods()) {
            OnError onError = AnnotationUtils.findAnnotation(method, OnError.class);
            if (onError == null) {
                continue;
            }

            // Validate signature: ErrorAction method(Throwable, ChangeStreamContext)
            validateOnErrorSignature(method, targetClass, beanName);

            Set<Class<? extends Throwable>> exTypes = new LinkedHashSet<>(List.of(onError.value()));
            boolean isCatchAll = exTypes.isEmpty();

            if (isCatchAll) {
                if (hasCatchAll) {
                    throw new BeanCreationException(beanName,
                            "@ChangeStream class " + targetClass.getName()
                                    + " has multiple catch-all @OnError methods. At most one is allowed.");
                }
                hasCatchAll = true;
            } else {
                for (Class<? extends Throwable> exType : exTypes) {
                    if (!seenExactTypes.add(exType)) {
                        throw new BeanCreationException(beanName,
                                "@ChangeStream class " + targetClass.getName()
                                        + " has duplicate @OnError for exception type: " + exType.getName());
                    }
                }
            }

            method.setAccessible(true);
            handlers.add(new ErrorHandlerMethod(method, exTypes));
        }

        return new ErrorHandlerResolver(handlers);
    }

    private void validateOnErrorSignature(Method method, Class<?> targetClass, String beanName) {
        Class<?>[] params = method.getParameterTypes();
        Class<?> returnType = method.getReturnType();

        if (returnType != ErrorAction.class) {
            throw new BeanCreationException(beanName,
                    "@OnError method " + targetClass.getName() + "#" + method.getName()
                            + " must return ErrorAction, found: " + returnType.getName());
        }

        if (params.length != 2
                || !Throwable.class.isAssignableFrom(params[0])
                || !ChangeStreamContext.class.isAssignableFrom(params[1])) {
            throw new BeanCreationException(beanName,
                    "@OnError method " + targetClass.getName() + "#" + method.getName()
                            + " must have signature: ErrorAction method(Throwable ex, ChangeStreamContext<?> ctx)");
        }
    }

    // --- Signature validation ---

    private void validateOnChangeSignature(Method method, Class<?> targetClass, String beanName) {
        Class<?>[] params = method.getParameterTypes();
        if (params.length != 1 || !ChangeStreamContext.class.isAssignableFrom(params[0])) {
            throw new BeanCreationException(beanName,
                    "@OnChange method " + targetClass.getName() + "#" + method.getName()
                            + " must have signature: void method(ChangeStreamContext ctx)");
        }
    }

    private HandlerMethod.SignatureStyle resolveTypedSignatureStyle(Method method, Class<?> targetClass,
                                                                    String beanName,
                                                                    Class<? extends Annotation> annotationType) {
        Class<?>[] params = method.getParameterTypes();
        String annName = "@" + annotationType.getSimpleName();

        if (params.length == 1) {
            if (ChangeStreamContext.class.isAssignableFrom(params[0])) {
                return HandlerMethod.SignatureStyle.CONTEXT_ONLY;
            }
            // Assume it's a document type parameter
            return HandlerMethod.SignatureStyle.DOCUMENT_ONLY;
        }
        if (params.length == 2) {
            if (!ChangeStreamContext.class.isAssignableFrom(params[1])) {
                throw new BeanCreationException(beanName,
                        annName + " method " + targetClass.getName() + "#" + method.getName()
                                + " has invalid signature. Expected: void method(T doc, ChangeStreamContext ctx)");
            }
            return HandlerMethod.SignatureStyle.DOCUMENT_AND_CONTEXT;
        }

        throw new BeanCreationException(beanName,
                annName + " method " + targetClass.getName() + "#" + method.getName()
                        + " has invalid signature. Supported: (ChangeStreamContext), (T doc), or (T doc, ChangeStreamContext)");
    }

    // --- Return type detection and mode validation ---

    private boolean detectReactiveReturn(Method method, Class<?> targetClass, String beanName) {
        Class<?> returnType = method.getReturnType();
        if (returnType == void.class) {
            return false;
        }
        if (Mono.class.isAssignableFrom(returnType)) {
            return true;
        }
        throw new BeanCreationException(beanName,
                "Handler method " + targetClass.getName() + "#" + method.getName()
                        + " has unsupported return type: " + returnType.getName()
                        + ". Supported return types are void and Mono<Void>.");
    }

    private void validateHandlerReturnTypes(String mode, String beanName, Class<?> targetClass,
                                             HandlerMethod onChangeHandler,
                                             Map<OperationType, HandlerMethod> typedHandlers) {
        if (mode.isEmpty()) {
            return;
        }
        List<HandlerMethod> allHandlers = new ArrayList<>();
        if (onChangeHandler != null) {
            allHandlers.add(onChangeHandler);
        }
        allHandlers.addAll(typedHandlers.values());

        for (HandlerMethod handler : allHandlers) {
            if ("IMPERATIVE".equals(mode) && handler.isReactiveReturn()) {
                throw new BeanCreationException(beanName,
                        "Handler method " + targetClass.getName() + "#" + handler.method().getName()
                                + " returns Mono<Void> but flowwarden.default-mode is IMPERATIVE. "
                                + "Mono<Void> signatures are not allowed in IMPERATIVE mode.");
            }
            if ("REACTIVE".equals(mode) && !handler.isReactiveReturn()) {
                throw new BeanCreationException(beanName,
                        "Handler method " + targetClass.getName() + "#" + handler.method().getName()
                                + " returns void but flowwarden.default-mode is REACTIVE. "
                                + "Use Mono<Void> return type in REACTIVE mode.");
            }
        }
    }

    // --- Validation helpers ---

    /**
     * Validates that {@code @Filter} is not used on streams that handle operations
     * without {@code fullDocument} (DELETE, DROP, INVALIDATE).
     *
     * <p>The filter predicate receives a {@link ChangeStreamContext} and typically
     * accesses {@code getFullDocument()}, which returns {@code Optional.empty()} for
     * these operations. Running the filter would be error-prone at runtime.</p>
     */
    private void validateFilterCompatibility(FilterMethod filterMethod,
                                              HandlerMethod onChangeHandler,
                                              Map<OperationType, HandlerMethod> typedHandlers,
                                              Class<?> targetClass, String beanName) {
        // Check typed handlers for no-fullDocument operations
        for (OperationType opType : NO_FULL_DOCUMENT_OPS) {
            if (typedHandlers.containsKey(opType)) {
                throw new BeanCreationException(beanName,
                        "@ChangeStream class " + targetClass.getName()
                                + " declares @Filter and @On" + capitalize(opType.name())
                                + ", which is not allowed. "
                                + opType + " events have no fullDocument, so the @Filter predicate "
                                + "cannot safely access the document. "
                                + "Use a server-side @Pipeline to filter these events, "
                                + "or move the filtering logic into the handler method.");
            }
        }

        // Check @OnChange if it covers no-fullDocument operations
        if (onChangeHandler != null) {
            OnChange onChangeAnn = AnnotationUtils.findAnnotation(
                    onChangeHandler.method(), OnChange.class);
            OperationType[] opTypes = (onChangeAnn != null) ? onChangeAnn.operationTypes()
                    : new OperationType[0];
            if (opTypes.length == 0) {
                // Empty = all types, which includes DELETE/DROP/INVALIDATE
                throw new BeanCreationException(beanName,
                        "@ChangeStream class " + targetClass.getName()
                                + " declares @Filter and @OnChange without operationTypes restriction. "
                                + "@OnChange handles all operation types including DELETE/DROP/INVALIDATE "
                                + "which have no fullDocument. Either restrict @OnChange(operationTypes = {...}) "
                                + "to operations with fullDocument (INSERT, UPDATE, REPLACE), "
                                + "use a server-side @Pipeline, or move the filtering logic into the handler.");
            }
            for (OperationType opType : opTypes) {
                if (NO_FULL_DOCUMENT_OPS.contains(opType)) {
                    throw new BeanCreationException(beanName,
                            "@ChangeStream class " + targetClass.getName()
                                    + " declares @Filter and @OnChange covering " + opType
                                    + ", which is not allowed. "
                                    + opType + " events have no fullDocument, so the @Filter predicate "
                                    + "cannot safely access the document. "
                                    + "Remove " + opType + " from @OnChange(operationTypes), "
                                    + "use a server-side @Pipeline, "
                                    + "or move the filtering logic into the handler method.");
                }
            }
        }
    }

    private void validateFullDocumentCompatibility(ChangeStream annotation,
                                                   Map<OperationType, HandlerMethod> typedHandlers,
                                                   Class<?> targetClass, String beanName) {
        if (annotation.fullDocument() != FullDocumentMode.DEFAULT) {
            return; // UPDATE_LOOKUP, WHEN_AVAILABLE, REQUIRED all provide fullDocument
        }

        HandlerMethod updateHandler = typedHandlers.get(OperationType.UPDATE);
        if (updateHandler != null
                && updateHandler.signatureStyle() != HandlerMethod.SignatureStyle.CONTEXT_ONLY) {
            log.warn("@ChangeStream '{}' ({}) uses fullDocument = DEFAULT with an @OnUpdate handler "
                    + "that expects a typed document parameter. UPDATE events with DEFAULT mode do not include "
                    + "the fullDocument — the parameter will be null. "
                    + "Set fullDocument = FullDocumentMode.UPDATE_LOOKUP on @ChangeStream, "
                    + "or change the @OnUpdate handler to use ChangeStreamContext.",
                    beanName, targetClass.getName());
        }
    }

    private void validateFullDocumentBeforeChangeCompatibility(ChangeStream annotation,
                                                              Map<OperationType, HandlerMethod> typedHandlers,
                                                              Class<?> targetClass, String beanName) {
        if (annotation.fullDocumentBeforeChange() == FullDocumentBeforeChangeMode.OFF) {
            return;
        }

        boolean hasOnlyInsertHandlers = !typedHandlers.isEmpty()
                && typedHandlers.keySet().stream().allMatch(op -> op == OperationType.INSERT);

        if (hasOnlyInsertHandlers) {
            log.warn("@ChangeStream '{}' ({}) sets fullDocumentBeforeChange = {} "
                    + "but only has @OnInsert handlers. INSERT events have no pre-image — "
                    + "the fullDocumentBeforeChange value will always be null. "
                    + "Consider removing fullDocumentBeforeChange or adding @OnUpdate/@OnDelete/@OnReplace handlers.",
                    beanName, targetClass.getName(), annotation.fullDocumentBeforeChange());
        }
    }

    private void validateMongoTemplateRef(ChangeStream annotation, Class<?> targetClass, String beanName) {
        String ref = annotation.mongoTemplateRef();
        if (ref.isEmpty() || applicationContext == null) {
            return;
        }
        boolean existsAsBlocking = applicationContext.getBeanNamesForType(MongoTemplate.class).length > 0
                && applicationContext.containsBean(ref);
        boolean existsAsReactive = applicationContext.getBeanNamesForType(ReactiveMongoTemplate.class).length > 0
                && applicationContext.containsBean(ref);
        if (!existsAsBlocking && !existsAsReactive) {
            throw new BeanCreationException(beanName,
                    "@ChangeStream on '" + targetClass.getSimpleName()
                            + "' references mongoTemplateRef='" + ref
                            + "' but no bean of type MongoTemplate or ReactiveMongoTemplate"
                            + " with this name was found.");
        }
    }

    // --- Name/collection resolution ---

    static String resolveCollection(ChangeStream annotation, Class<?> targetClass, String beanName) {
        String collection = annotation.collection();
        if (!collection.isEmpty()) {
            return collection;
        }

        // Infer from documentType
        Class<?> docType = annotation.documentType();
        if (docType != Document.class) {
            collection = resolveCollectionFromDocumentType(docType);
        }

        if (collection.isEmpty()) {
            throw new BeanCreationException(beanName,
                    "@ChangeStream on " + targetClass.getName()
                            + " must specify a collection or a documentType with @Document");
        }
        return collection;
    }

    static String resolveCollectionFromDocumentType(Class<?> docType) {
        org.springframework.data.mongodb.core.mapping.Document docAnnotation =
                AnnotationUtils.findAnnotation(docType, org.springframework.data.mongodb.core.mapping.Document.class);
        if (docAnnotation != null && !docAnnotation.collection().isEmpty()) {
            return docAnnotation.collection();
        }
        // Fall back to Spring Data convention: decapitalized simple class name
        String simpleName = docType.getSimpleName();
        return Character.toLowerCase(simpleName.charAt(0)) + simpleName.substring(1);
    }

    static String resolveStreamName(ChangeStream annotation, Class<?> targetClass) {
        String name = annotation.name();
        if (name.isEmpty()) {
            name = annotation.value();
        }
        if (name.isEmpty()) {
            name = toKebabCase(targetClass.getSimpleName());
        }
        return name;
    }

    static String toKebabCase(String className) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < className.length(); i++) {
            char c = className.charAt(i);
            if (Character.isUpperCase(c)) {
                if (i > 0) {
                    sb.append('-');
                }
                sb.append(Character.toLowerCase(c));
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private void validateDeadLetterQueue(DeadLetterQueue dlqAnnotation, Class<?> targetClass, String beanName) {
        if (dlqAnnotation.ttlDays() < 0) {
            throw new BeanCreationException(beanName,
                    "@DeadLetterQueue on " + targetClass.getName()
                            + " has negative ttlDays: " + dlqAnnotation.ttlDays()
                            + ". Must be >= 0.");
        }
    }

    private void validateRetryPolicy(RetryPolicy retryAnnotation, Class<?> targetClass, String beanName) {
        if (retryAnnotation.maxAttempts() < 1) {
            throw new BeanCreationException(beanName,
                    "@RetryPolicy on " + targetClass.getName()
                            + " has invalid maxAttempts: " + retryAnnotation.maxAttempts()
                            + ". Must be >= 1.");
        }
        if (retryAnnotation.multiplier() <= 0) {
            throw new BeanCreationException(beanName,
                    "@RetryPolicy on " + targetClass.getName()
                            + " has invalid multiplier: " + retryAnnotation.multiplier()
                            + ". Must be > 0.");
        }
        try {
            RetryPolicyConfig.parseDuration(retryAnnotation.initialDelay());
        } catch (IllegalArgumentException e) {
            throw new BeanCreationException(beanName,
                    "@RetryPolicy on " + targetClass.getName()
                            + " has invalid initialDelay: " + retryAnnotation.initialDelay());
        }
        try {
            RetryPolicyConfig.parseDuration(retryAnnotation.maxDelay());
        } catch (IllegalArgumentException e) {
            throw new BeanCreationException(beanName,
                    "@RetryPolicy on " + targetClass.getName()
                            + " has invalid maxDelay: " + retryAnnotation.maxDelay());
        }
    }

    private static String capitalize(String name) {
        if (name == null || name.isEmpty()) return name;
        return name.charAt(0) + name.substring(1).toLowerCase();
    }
}
