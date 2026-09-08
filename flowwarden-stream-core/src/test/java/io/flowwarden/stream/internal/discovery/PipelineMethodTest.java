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

import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PipelineMethodTest {

    @Test
    void fromSupplierResolvesToDocumentList() {
        PipelineMethod pm = PipelineMethod.fromSupplier(
                () -> List.of(new Document("$match", new Document("operationType", "insert"))));

        List<Document> resolved = pm.resolve(new Object());

        assertEquals(1, resolved.size());
    }

    @Test
    void fromSupplierThrowsWhenSupplierReturnsNull() {
        PipelineMethod pm = PipelineMethod.fromSupplier(() -> null);

        assertThrows(IllegalStateException.class, () -> pm.resolve(new Object()));
    }

    @Test
    void fromSupplierWrapsSupplierExceptionInIllegalStateException() {
        // Regression: the raw supplier call used to sit outside the try block, so a
        // RuntimeException from the supplier escaped uncontextualized instead of matching
        // the documented IllegalStateException contract (and the reflective @Pipeline path's
        // behavior of wrapping the cause with a clear message).
        RuntimeException cause = new IllegalArgumentException("bad pipeline config");
        PipelineMethod pm = PipelineMethod.fromSupplier(() -> {
            throw cause;
        });

        IllegalStateException thrown = assertThrows(IllegalStateException.class, () -> pm.resolve(new Object()));
        assertSame(cause, thrown.getCause());
        assertInstanceOf(String.class, thrown.getMessage());
    }
}
