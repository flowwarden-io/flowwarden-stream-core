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
package io.flowwarden.stream.annotation;

import io.flowwarden.stream.FullDocumentBeforeChangeMode;
import io.flowwarden.stream.FullDocumentMode;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.lang.annotation.ElementType;
import java.lang.annotation.RetentionPolicy;

import static org.junit.jupiter.api.Assertions.*;

class ChangeStreamTest {

    @ChangeStream(collection = "orders")
    static class MinimalHandler {}

    @ChangeStream(
            name = "my-stream",
            collection = "products",
            database = "catalog",
            fullDocument = FullDocumentMode.UPDATE_LOOKUP,
            enabled = false,
            autoStart = false
    )
    static class CustomHandler {}

    @Test
    void isRuntimeRetention() {
        assertEquals(RetentionPolicy.RUNTIME,
                ChangeStream.class.getAnnotation(java.lang.annotation.Retention.class).value());
    }

    @Test
    void targetsType() {
        var target = ChangeStream.class.getAnnotation(java.lang.annotation.Target.class);
        assertArrayEquals(new ElementType[]{ElementType.TYPE}, target.value());
    }

    @Test
    void defaultValues() {
        ChangeStream ann = MinimalHandler.class.getAnnotation(ChangeStream.class);
        assertNotNull(ann);

        // Identification
        assertEquals("", ann.value());
        assertEquals("", ann.name());
        assertEquals("", ann.description());
        assertEquals("", ann.zone());

        // MongoDB target
        assertEquals("orders", ann.collection());
        assertEquals("", ann.database());
        assertEquals(Document.class, ann.documentType());

        // Operations
        assertEquals(FullDocumentMode.DEFAULT, ann.fullDocument());
        assertEquals(FullDocumentBeforeChangeMode.OFF, ann.fullDocumentBeforeChange());

        // Behaviour
        assertTrue(ann.enabled());
        assertTrue(ann.autoStart());
        assertEquals("", ann.mongoTemplateRef());
    }

    @Test
    void customValues() {
        ChangeStream ann = CustomHandler.class.getAnnotation(ChangeStream.class);
        assertNotNull(ann);
        assertEquals("my-stream", ann.name());
        assertEquals("products", ann.collection());
        assertEquals("catalog", ann.database());
        assertEquals(FullDocumentMode.UPDATE_LOOKUP, ann.fullDocument());
        assertFalse(ann.enabled());
        assertFalse(ann.autoStart());
    }
}
