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
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class HandlerMethodTest {

    // --- Test target classes ---

    static class Target {
        final AtomicReference<ChangeStreamContext<?>> receivedCtx = new AtomicReference<>();
        final AtomicReference<Object> receivedDoc = new AtomicReference<>();

        void contextOnly(ChangeStreamContext<?> ctx) {
            receivedCtx.set(ctx);
        }

        void documentOnly(String doc) {
            receivedDoc.set(doc);
        }

        void documentAndContext(String doc, ChangeStreamContext<?> ctx) {
            receivedDoc.set(doc);
            receivedCtx.set(ctx);
        }
    }

    static class ReactiveTarget {
        final AtomicReference<ChangeStreamContext<?>> receivedCtx = new AtomicReference<>();
        final AtomicReference<Object> receivedDoc = new AtomicReference<>();

        Mono<Void> contextOnly(ChangeStreamContext<?> ctx) {
            receivedCtx.set(ctx);
            return Mono.empty();
        }

        Mono<Void> documentOnly(String doc) {
            receivedDoc.set(doc);
            return Mono.empty();
        }

        Mono<Void> documentAndContext(String doc, ChangeStreamContext<?> ctx) {
            receivedDoc.set(doc);
            receivedCtx.set(ctx);
            return Mono.empty();
        }
    }

    static class DocTarget {
        Document receivedDoc;

        void handle(Document doc) {
            receivedDoc = doc;
        }
    }

    @Test
    void invokeContextOnly() throws Exception {
        Target target = new Target();
        Method method = Target.class.getDeclaredMethod("contextOnly", ChangeStreamContext.class);
        HandlerMethod hm = new HandlerMethod(method, HandlerMethod.SignatureStyle.CONTEXT_ONLY, false);

        ChangeStreamContext<?> ctx = mock(ChangeStreamContext.class);
        MongoConverter converter = mock(MongoConverter.class);

        hm.invoke(target, ctx, new Document("key", "value"), converter, Document.class);

        assertSame(ctx, target.receivedCtx.get());
        assertNull(target.receivedDoc.get());
    }

    @Test
    void invokeDocumentOnly() throws Exception {
        Target target = new Target();
        Method method = Target.class.getDeclaredMethod("documentOnly", String.class);
        HandlerMethod hm = new HandlerMethod(method, HandlerMethod.SignatureStyle.DOCUMENT_ONLY, false);

        ChangeStreamContext<?> ctx = mock(ChangeStreamContext.class);
        MongoConverter converter = mock(MongoConverter.class);
        Document rawDoc = new Document("name", "test");
        when(converter.read(eq(String.class), any(Document.class))).thenReturn("converted");

        hm.invoke(target, ctx, rawDoc, converter, String.class);

        assertEquals("converted", target.receivedDoc.get());
        assertNull(target.receivedCtx.get());
    }

    @Test
    void invokeDocumentAndContext() throws Exception {
        Target target = new Target();
        Method method = Target.class.getDeclaredMethod("documentAndContext", String.class, ChangeStreamContext.class);
        HandlerMethod hm = new HandlerMethod(method, HandlerMethod.SignatureStyle.DOCUMENT_AND_CONTEXT, false);

        ChangeStreamContext<?> ctx = mock(ChangeStreamContext.class);
        MongoConverter converter = mock(MongoConverter.class);
        Document rawDoc = new Document("name", "test");
        when(converter.read(eq(String.class), any(Document.class))).thenReturn("converted");

        hm.invoke(target, ctx, rawDoc, converter, String.class);

        assertEquals("converted", target.receivedDoc.get());
        assertSame(ctx, target.receivedCtx.get());
    }

    @Test
    void invokeDocumentOnlyWithNullDoc() throws Exception {
        Target target = new Target();
        Method method = Target.class.getDeclaredMethod("documentOnly", String.class);
        HandlerMethod hm = new HandlerMethod(method, HandlerMethod.SignatureStyle.DOCUMENT_ONLY, false);

        ChangeStreamContext<?> ctx = mock(ChangeStreamContext.class);
        MongoConverter converter = mock(MongoConverter.class);

        // null rawDoc (e.g. DELETE) → handler receives null
        hm.invoke(target, ctx, null, converter, String.class);

        assertNull(target.receivedDoc.get());
        verifyNoInteractions(converter);
    }

    @Test
    void invokeDocumentOnlyWithRawDocTypeDocument() throws Exception {
        DocTarget target = new DocTarget();
        Method method = DocTarget.class.getDeclaredMethod("handle", Document.class);
        HandlerMethod hm = new HandlerMethod(method, HandlerMethod.SignatureStyle.DOCUMENT_ONLY, false);

        ChangeStreamContext<?> ctx = mock(ChangeStreamContext.class);
        MongoConverter converter = mock(MongoConverter.class);
        Document rawDoc = new Document("key", "value");

        hm.invoke(target, ctx, rawDoc, converter, Document.class);

        assertSame(rawDoc, target.receivedDoc);
        verifyNoInteractions(converter); // no conversion for Document.class
    }

    @Test
    void toStringContainsMethodInfo() throws Exception {
        Method method = Target.class.getDeclaredMethod("contextOnly", ChangeStreamContext.class);
        HandlerMethod hm = new HandlerMethod(method, HandlerMethod.SignatureStyle.CONTEXT_ONLY, false);

        String str = hm.toString();
        assertTrue(str.contains("Target"));
        assertTrue(str.contains("contextOnly"));
        assertTrue(str.contains("CONTEXT_ONLY"));
    }

    // --- Reactive return type tests ---

    @Test
    void reactiveReturnFlagIsSetCorrectly() throws Exception {
        Method voidMethod = Target.class.getDeclaredMethod("contextOnly", ChangeStreamContext.class);
        HandlerMethod imperative = new HandlerMethod(voidMethod, HandlerMethod.SignatureStyle.CONTEXT_ONLY, false);
        assertFalse(imperative.isReactiveReturn());

        Method monoMethod = ReactiveTarget.class.getDeclaredMethod("contextOnly", ChangeStreamContext.class);
        HandlerMethod reactive = new HandlerMethod(monoMethod, HandlerMethod.SignatureStyle.CONTEXT_ONLY, true);
        assertTrue(reactive.isReactiveReturn());
    }

    @Test
    void invokeReactiveContextOnly() throws Exception {
        ReactiveTarget target = new ReactiveTarget();
        Method method = ReactiveTarget.class.getDeclaredMethod("contextOnly", ChangeStreamContext.class);
        HandlerMethod hm = new HandlerMethod(method, HandlerMethod.SignatureStyle.CONTEXT_ONLY, true);

        ChangeStreamContext<?> ctx = mock(ChangeStreamContext.class);
        MongoConverter converter = mock(MongoConverter.class);

        Object result = hm.invoke(target, ctx, new Document("key", "value"), converter, Document.class);

        assertInstanceOf(Mono.class, result);
        assertSame(ctx, target.receivedCtx.get());
    }

    @Test
    void invokeReactiveDocumentOnly() throws Exception {
        ReactiveTarget target = new ReactiveTarget();
        Method method = ReactiveTarget.class.getDeclaredMethod("documentOnly", String.class);
        HandlerMethod hm = new HandlerMethod(method, HandlerMethod.SignatureStyle.DOCUMENT_ONLY, true);

        ChangeStreamContext<?> ctx = mock(ChangeStreamContext.class);
        MongoConverter converter = mock(MongoConverter.class);
        when(converter.read(eq(String.class), any(Document.class))).thenReturn("converted");

        Object result = hm.invoke(target, ctx, new Document("name", "test"), converter, String.class);

        assertInstanceOf(Mono.class, result);
        assertEquals("converted", target.receivedDoc.get());
    }

    @Test
    void invokeReactiveDocumentAndContext() throws Exception {
        ReactiveTarget target = new ReactiveTarget();
        Method method = ReactiveTarget.class.getDeclaredMethod("documentAndContext", String.class, ChangeStreamContext.class);
        HandlerMethod hm = new HandlerMethod(method, HandlerMethod.SignatureStyle.DOCUMENT_AND_CONTEXT, true);

        ChangeStreamContext<?> ctx = mock(ChangeStreamContext.class);
        MongoConverter converter = mock(MongoConverter.class);
        when(converter.read(eq(String.class), any(Document.class))).thenReturn("converted");

        Object result = hm.invoke(target, ctx, new Document("name", "test"), converter, String.class);

        assertInstanceOf(Mono.class, result);
        assertEquals("converted", target.receivedDoc.get());
        assertSame(ctx, target.receivedCtx.get());
    }

    // --- Functional handler tests ---

    @Test
    void invokeFromDocumentHandler() throws Exception {
        AtomicReference<Object> receivedDoc = new AtomicReference<>();
        AtomicReference<ChangeStreamContext<?>> receivedCtx = new AtomicReference<>();

        DocumentHandler<String> handler = (doc, ctx) -> {
            receivedDoc.set(doc);
            receivedCtx.set(ctx);
        };

        HandlerMethod hm = HandlerMethod.fromDocumentHandler(handler);

        ChangeStreamContext<?> ctx = mock(ChangeStreamContext.class);
        MongoConverter converter = mock(MongoConverter.class);
        when(converter.read(eq(String.class), any(Document.class))).thenReturn("converted");

        Object result = hm.invoke(null, ctx, new Document("name", "test"), converter, String.class);

        assertNull(result);
        assertEquals("converted", receivedDoc.get());
        assertSame(ctx, receivedCtx.get());
        assertFalse(hm.isReactiveReturn());
        assertEquals(HandlerMethod.SignatureStyle.DOCUMENT_AND_CONTEXT, hm.signatureStyle());
        assertNull(hm.method());
        assertNotNull(hm.handlerFunction());
    }

    @Test
    void invokeFromContextHandler() throws Exception {
        AtomicReference<ChangeStreamContext<?>> receivedCtx = new AtomicReference<>();

        ContextHandler<String> handler = ctx -> receivedCtx.set(ctx);

        HandlerMethod hm = HandlerMethod.fromContextHandler(handler);

        ChangeStreamContext<?> ctx = mock(ChangeStreamContext.class);
        MongoConverter converter = mock(MongoConverter.class);

        Object result = hm.invoke(null, ctx, new Document("key", "value"), converter, Document.class);

        assertNull(result);
        assertSame(ctx, receivedCtx.get());
        assertFalse(hm.isReactiveReturn());
        assertEquals(HandlerMethod.SignatureStyle.CONTEXT_ONLY, hm.signatureStyle());
    }

    @Test
    void invokeFromReactiveDocumentHandler() throws Exception {
        AtomicReference<Object> receivedDoc = new AtomicReference<>();
        AtomicReference<ChangeStreamContext<?>> receivedCtx = new AtomicReference<>();

        ReactiveDocumentHandler<String> handler = (doc, ctx) -> {
            receivedDoc.set(doc);
            receivedCtx.set(ctx);
            return Mono.empty();
        };

        HandlerMethod hm = HandlerMethod.fromReactiveDocumentHandler(handler);

        ChangeStreamContext<?> ctx = mock(ChangeStreamContext.class);
        MongoConverter converter = mock(MongoConverter.class);
        when(converter.read(eq(String.class), any(Document.class))).thenReturn("converted");

        Object result = hm.invoke(null, ctx, new Document("name", "test"), converter, String.class);

        assertInstanceOf(Mono.class, result);
        assertEquals("converted", receivedDoc.get());
        assertSame(ctx, receivedCtx.get());
        assertTrue(hm.isReactiveReturn());
    }

    @Test
    void invokeFromReactiveContextHandler() throws Exception {
        AtomicReference<ChangeStreamContext<?>> receivedCtx = new AtomicReference<>();

        ReactiveContextHandler<String> handler = ctx -> {
            receivedCtx.set(ctx);
            return Mono.empty();
        };

        HandlerMethod hm = HandlerMethod.fromReactiveContextHandler(handler);

        ChangeStreamContext<?> ctx = mock(ChangeStreamContext.class);
        MongoConverter converter = mock(MongoConverter.class);

        Object result = hm.invoke(null, ctx, null, converter, Document.class);

        assertInstanceOf(Mono.class, result);
        assertSame(ctx, receivedCtx.get());
        assertTrue(hm.isReactiveReturn());
        assertEquals(HandlerMethod.SignatureStyle.CONTEXT_ONLY, hm.signatureStyle());
    }

    @Test
    void functionalHandlerToStringIsMeaningful() {
        DocumentHandler<String> handler = (doc, ctx) -> {};
        HandlerMethod hm = HandlerMethod.fromDocumentHandler(handler);

        String str = hm.toString();
        assertTrue(str.contains("functional"));
        assertTrue(str.contains("DOCUMENT_AND_CONTEXT"));
    }
}
