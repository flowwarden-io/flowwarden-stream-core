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
import io.flowwarden.stream.core.ContextHandler;
import io.flowwarden.stream.core.DocumentHandler;
import io.flowwarden.stream.core.ReactiveContextHandler;
import io.flowwarden.stream.core.ReactiveDocumentHandler;
import org.bson.Document;
import org.springframework.data.mongodb.core.convert.MongoConverter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;

/**
 * Encapsulates a handler {@link Method} (or functional handler) together with its invocation strategy.
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public final class HandlerMethod {

    /**
     * Describes how the handler method should be invoked.
     */
    public enum SignatureStyle {
        /** {@code void handle(ChangeStreamContext ctx)} */
        CONTEXT_ONLY,
        /** {@code void handle(T doc)} */
        DOCUMENT_ONLY,
        /** {@code void handle(T doc, ChangeStreamContext ctx)} */
        DOCUMENT_AND_CONTEXT
    }

    private final Method method;
    private final SignatureStyle signatureStyle;
    private final boolean reactiveReturn;
    private final Object handlerFunction;

    public HandlerMethod(Method method, SignatureStyle signatureStyle, boolean reactiveReturn) {
        this.method = Objects.requireNonNull(method, "method must not be null");
        this.signatureStyle = Objects.requireNonNull(signatureStyle, "signatureStyle must not be null");
        this.reactiveReturn = reactiveReturn;
        this.handlerFunction = null;
    }

    private HandlerMethod(SignatureStyle signatureStyle, boolean reactiveReturn, Object handlerFunction) {
        this.method = null;
        this.signatureStyle = Objects.requireNonNull(signatureStyle, "signatureStyle must not be null");
        this.reactiveReturn = reactiveReturn;
        this.handlerFunction = Objects.requireNonNull(handlerFunction, "handlerFunction must not be null");
    }

    // --- Factory methods for reflection-based handlers ---

    /**
     * Creates a {@link HandlerMethod} backed by a reflective {@link Method}.
     */
    public static HandlerMethod fromMethod(Method method, SignatureStyle signatureStyle, boolean reactiveReturn) {
        return new HandlerMethod(method, signatureStyle, reactiveReturn);
    }

    // --- Factory methods for functional handlers ---

    /**
     * Creates a {@link HandlerMethod} backed by a {@link DocumentHandler} lambda.
     */
    public static HandlerMethod fromDocumentHandler(DocumentHandler<?> handler) {
        return new HandlerMethod(SignatureStyle.DOCUMENT_AND_CONTEXT, false, handler);
    }

    /**
     * Creates a {@link HandlerMethod} backed by a {@link ContextHandler} lambda.
     */
    public static HandlerMethod fromContextHandler(ContextHandler<?> handler) {
        return new HandlerMethod(SignatureStyle.CONTEXT_ONLY, false, handler);
    }

    /**
     * Creates a {@link HandlerMethod} backed by a {@link ReactiveDocumentHandler} lambda.
     */
    public static HandlerMethod fromReactiveDocumentHandler(ReactiveDocumentHandler<?> handler) {
        return new HandlerMethod(SignatureStyle.DOCUMENT_AND_CONTEXT, true, handler);
    }

    /**
     * Creates a {@link HandlerMethod} backed by a {@link ReactiveContextHandler} lambda.
     */
    public static HandlerMethod fromReactiveContextHandler(ReactiveContextHandler<?> handler) {
        return new HandlerMethod(SignatureStyle.CONTEXT_ONLY, true, handler);
    }

    public Method method() {
        return method;
    }

    public SignatureStyle signatureStyle() {
        return signatureStyle;
    }

    public boolean isReactiveReturn() {
        return reactiveReturn;
    }

    /**
     * Returns the functional handler, or {@code null} if this is a reflection-based handler.
     */
    public Object handlerFunction() {
        return handlerFunction;
    }

    /**
     * Invokes the handler on the given bean (reflection) or via the functional handler.
     *
     * @param bean      the target bean instance (ignored for functional handlers)
     * @param ctx       the change stream context
     * @param rawDoc    the raw full document (may be null, e.g. for DELETE)
     * @param converter the MongoDB converter for document-to-type mapping
     * @param docType   the target document type from {@code @ChangeStream.documentType()}
     * @return the result of the handler invocation (a {@code Mono<Void>} for reactive handlers, null for void)
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Object invoke(Object bean, ChangeStreamContext<?> ctx, Document rawDoc,
                         MongoConverter converter, Class<?> docType)
            throws InvocationTargetException, IllegalAccessException {

        if (handlerFunction != null) {
            return invokeFunctional(ctx, rawDoc, converter, docType);
        }

        return switch (signatureStyle) {
            case CONTEXT_ONLY -> method.invoke(bean, ctx);
            case DOCUMENT_ONLY -> {
                Object converted = convertDocument(rawDoc, converter, docType);
                yield method.invoke(bean, converted);
            }
            case DOCUMENT_AND_CONTEXT -> {
                Object converted = convertDocument(rawDoc, converter, docType);
                yield method.invoke(bean, converted, ctx);
            }
        };
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Object invokeFunctional(ChangeStreamContext<?> ctx, Document rawDoc,
                                     MongoConverter converter, Class<?> docType) {
        if (handlerFunction instanceof DocumentHandler handler) {
            Object converted = convertDocument(rawDoc, converter, docType);
            handler.handle(converted, ctx);
            return null;
        }
        if (handlerFunction instanceof ContextHandler handler) {
            handler.handle(ctx);
            return null;
        }
        if (handlerFunction instanceof ReactiveDocumentHandler handler) {
            Object converted = convertDocument(rawDoc, converter, docType);
            return handler.handle(converted, ctx);
        }
        if (handlerFunction instanceof ReactiveContextHandler handler) {
            return handler.handle(ctx);
        }
        throw new IllegalStateException("Unknown handler function type: " + handlerFunction.getClass());
    }

    private static Object convertDocument(Document rawDoc, MongoConverter converter, Class<?> docType) {
        if (rawDoc == null) {
            return null;
        }
        if (docType == Document.class) {
            return rawDoc;
        }
        return converter.read(docType, rawDoc);
    }

    @Override
    public String toString() {
        if (handlerFunction != null) {
            return "functional(" + signatureStyle + ", " + handlerFunction.getClass().getSimpleName() + ")";
        }
        return method.getDeclaringClass().getSimpleName() + "#" + method.getName()
                + "(" + signatureStyle + ")";
    }
}
