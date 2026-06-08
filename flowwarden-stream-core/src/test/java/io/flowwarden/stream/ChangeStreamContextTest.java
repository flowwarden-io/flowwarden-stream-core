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
package io.flowwarden.stream;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class ChangeStreamContextTest {

    @Test
    void isPublicInterface() {
        assertTrue(ChangeStreamContext.class.isInterface());
        assertTrue(Modifier.isPublic(ChangeStreamContext.class.getModifiers()));
    }

    @Test
    void hasExpectedMethods() {
        Set<String> methodNames = Arrays.stream(ChangeStreamContext.class.getDeclaredMethods())
                .map(Method::getName)
                .collect(Collectors.toSet());

        // Event metadata
        assertTrue(methodNames.contains("getEventId"));
        assertTrue(methodNames.contains("getOperationType"));
        assertTrue(methodNames.contains("getStreamName"));
        assertTrue(methodNames.contains("getCollectionName"));
        assertTrue(methodNames.contains("getDatabaseName"));
        assertTrue(methodNames.contains("getClusterTime"));
        assertTrue(methodNames.contains("getResumeToken"));
        assertTrue(methodNames.contains("getAttemptNumber"));

        // Document access
        assertTrue(methodNames.contains("getFullDocument"));
        assertTrue(methodNames.contains("getFullDocumentBeforeChange"));
        assertTrue(methodNames.contains("getUpdateDescription"));
        assertTrue(methodNames.contains("getDocumentKey"));

        // Actions
        assertTrue(methodNames.contains("sendToDlq"));
        assertTrue(methodNames.contains("saveCheckpointNow"));
        assertTrue(methodNames.contains("addMetadata"));
        assertTrue(methodNames.contains("getMetadata"));
        assertTrue(methodNames.contains("getAllMetadata"));
    }

    @Test
    void hasOneTypeParameter() {
        assertEquals(1, ChangeStreamContext.class.getTypeParameters().length);
        assertEquals("T", ChangeStreamContext.class.getTypeParameters()[0].getName());
    }
}
