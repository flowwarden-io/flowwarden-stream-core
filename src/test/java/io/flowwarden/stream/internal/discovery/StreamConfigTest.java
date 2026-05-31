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
import io.flowwarden.stream.annotation.ChangeStream;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.core.annotation.AnnotationUtils;

import static org.junit.jupiter.api.Assertions.*;

class StreamConfigTest {

    @Test
    void fromAnnotationMapsAllFields() {
        ChangeStream ann = AnnotationUtils.findAnnotation(SampleStream.class, ChangeStream.class);
        assertNotNull(ann);

        StreamConfig config = StreamConfig.fromAnnotation(ann);

        assertTrue(config.enabled());
        assertFalse(config.autoStart());
        assertEquals(Document.class, config.documentType());
        assertEquals("customTemplate", config.mongoTemplateRef());
        assertEquals(FullDocumentMode.UPDATE_LOOKUP, config.fullDocument());
        assertEquals(FullDocumentBeforeChangeMode.WHEN_AVAILABLE, config.fullDocumentBeforeChange());
    }

    @Test
    void fromAnnotationWithDefaults() {
        ChangeStream ann = AnnotationUtils.findAnnotation(DefaultStream.class, ChangeStream.class);
        assertNotNull(ann);

        StreamConfig config = StreamConfig.fromAnnotation(ann);

        assertTrue(config.enabled());
        assertTrue(config.autoStart());
        assertEquals(Document.class, config.documentType());
        assertEquals("", config.mongoTemplateRef());
        assertEquals(FullDocumentMode.DEFAULT, config.fullDocument());
        assertEquals(FullDocumentBeforeChangeMode.OFF, config.fullDocumentBeforeChange());
    }

    @ChangeStream(collection = "orders", autoStart = false, mongoTemplateRef = "customTemplate",
            fullDocument = FullDocumentMode.UPDATE_LOOKUP,
            fullDocumentBeforeChange = FullDocumentBeforeChangeMode.WHEN_AVAILABLE)
    static class SampleStream {}

    @ChangeStream(collection = "orders")
    static class DefaultStream {}
}
