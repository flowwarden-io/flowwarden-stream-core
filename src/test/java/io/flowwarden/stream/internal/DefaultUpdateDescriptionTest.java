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
package io.flowwarden.stream.internal;

import io.flowwarden.stream.TruncatedArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DefaultUpdateDescriptionTest {

    @Test
    void mapsUpdatedFields() {
        var driverDesc = mock(com.mongodb.client.model.changestream.UpdateDescription.class);
        when(driverDesc.getUpdatedFields()).thenReturn(
                new BsonDocument("status", new BsonString("PAID"))
                        .append("amount", new BsonInt32(100)));
        when(driverDesc.getRemovedFields()).thenReturn(null);
        when(driverDesc.getTruncatedArrays()).thenReturn(null);

        var desc = new DefaultUpdateDescription(driverDesc);

        assertEquals(2, desc.getUpdatedFields().size());
        assertEquals("PAID", desc.getUpdatedFields().get("status"));
        assertEquals(100, desc.getUpdatedFields().get("amount"));
    }

    @Test
    void updatedFieldsUnmodifiable() {
        var driverDesc = mock(com.mongodb.client.model.changestream.UpdateDescription.class);
        when(driverDesc.getUpdatedFields()).thenReturn(
                new BsonDocument("a", new BsonString("b")));
        when(driverDesc.getRemovedFields()).thenReturn(null);
        when(driverDesc.getTruncatedArrays()).thenReturn(null);

        var desc = new DefaultUpdateDescription(driverDesc);
        assertThrows(UnsupportedOperationException.class,
                () -> desc.getUpdatedFields().put("new", "value"));
    }

    @Test
    void removedFieldsUnmodifiable() {
        var driverDesc = mock(com.mongodb.client.model.changestream.UpdateDescription.class);
        when(driverDesc.getUpdatedFields()).thenReturn(null);
        when(driverDesc.getRemovedFields()).thenReturn(List.of("oldField"));
        when(driverDesc.getTruncatedArrays()).thenReturn(null);

        var desc = new DefaultUpdateDescription(driverDesc);
        assertThrows(UnsupportedOperationException.class,
                () -> desc.getRemovedFields().add("injected"));
    }

    @Test
    void truncatedArraysUnmodifiable() {
        var driverTa = mock(com.mongodb.client.model.changestream.TruncatedArray.class);
        when(driverTa.getField()).thenReturn("items");
        when(driverTa.getNewSize()).thenReturn(5);

        var driverDesc = mock(com.mongodb.client.model.changestream.UpdateDescription.class);
        when(driverDesc.getUpdatedFields()).thenReturn(null);
        when(driverDesc.getRemovedFields()).thenReturn(null);
        when(driverDesc.getTruncatedArrays()).thenReturn(List.of(driverTa));

        var desc = new DefaultUpdateDescription(driverDesc);
        assertThrows(UnsupportedOperationException.class,
                () -> desc.getTruncatedArrays().add(new io.flowwarden.stream.TruncatedArray("hack", 0)));
    }

    @Test
    void mapsRemovedFields() {
        var driverDesc = mock(com.mongodb.client.model.changestream.UpdateDescription.class);
        when(driverDesc.getUpdatedFields()).thenReturn(null);
        when(driverDesc.getRemovedFields()).thenReturn(List.of("oldField", "deprecated"));
        when(driverDesc.getTruncatedArrays()).thenReturn(null);

        var desc = new DefaultUpdateDescription(driverDesc);

        assertEquals(List.of("oldField", "deprecated"), desc.getRemovedFields());
    }

    @Test
    void emptyWhenFieldsNull() {
        var driverDesc = mock(com.mongodb.client.model.changestream.UpdateDescription.class);
        when(driverDesc.getUpdatedFields()).thenReturn(null);
        when(driverDesc.getRemovedFields()).thenReturn(null);
        when(driverDesc.getTruncatedArrays()).thenReturn(null);

        var desc = new DefaultUpdateDescription(driverDesc);

        assertTrue(desc.getUpdatedFields().isEmpty());
        assertTrue(desc.getRemovedFields().isEmpty());
        assertTrue(desc.getTruncatedArrays().isEmpty());
    }

    @Test
    void mapsTruncatedArrays() {
        var driverTa = mock(com.mongodb.client.model.changestream.TruncatedArray.class);
        when(driverTa.getField()).thenReturn("items");
        when(driverTa.getNewSize()).thenReturn(5);

        var driverDesc = mock(com.mongodb.client.model.changestream.UpdateDescription.class);
        when(driverDesc.getUpdatedFields()).thenReturn(null);
        when(driverDesc.getRemovedFields()).thenReturn(null);
        when(driverDesc.getTruncatedArrays()).thenReturn(List.of(driverTa));

        var desc = new DefaultUpdateDescription(driverDesc);

        assertEquals(1, desc.getTruncatedArrays().size());
        TruncatedArray ta = desc.getTruncatedArrays().get(0);
        assertEquals("items", ta.field());
        assertEquals(5, ta.newSize());
    }

    @Test
    void hasFieldChangedForUpdatedField() {
        var driverDesc = mock(com.mongodb.client.model.changestream.UpdateDescription.class);
        when(driverDesc.getUpdatedFields()).thenReturn(
                new BsonDocument("status", new BsonString("PAID")));
        when(driverDesc.getRemovedFields()).thenReturn(null);
        when(driverDesc.getTruncatedArrays()).thenReturn(null);

        var desc = new DefaultUpdateDescription(driverDesc);

        assertTrue(desc.hasFieldChanged("status"));
        assertFalse(desc.hasFieldChanged("other"));
    }

    @Test
    void hasFieldChangedForRemovedField() {
        var driverDesc = mock(com.mongodb.client.model.changestream.UpdateDescription.class);
        when(driverDesc.getUpdatedFields()).thenReturn(null);
        when(driverDesc.getRemovedFields()).thenReturn(List.of("removed"));
        when(driverDesc.getTruncatedArrays()).thenReturn(null);

        var desc = new DefaultUpdateDescription(driverDesc);

        assertTrue(desc.hasFieldChanged("removed"));
    }

    @Test
    void getUpdatedFieldValuePresent() {
        var driverDesc = mock(com.mongodb.client.model.changestream.UpdateDescription.class);
        when(driverDesc.getUpdatedFields()).thenReturn(
                new BsonDocument("status", new BsonString("PAID")));
        when(driverDesc.getRemovedFields()).thenReturn(null);
        when(driverDesc.getTruncatedArrays()).thenReturn(null);

        var desc = new DefaultUpdateDescription(driverDesc);

        var result = desc.getUpdatedFieldValue("status", String.class);
        assertTrue(result.isPresent());
        assertEquals("PAID", result.get());
    }

    @Test
    void getUpdatedFieldValueMissing() {
        var driverDesc = mock(com.mongodb.client.model.changestream.UpdateDescription.class);
        when(driverDesc.getUpdatedFields()).thenReturn(new BsonDocument());
        when(driverDesc.getRemovedFields()).thenReturn(null);
        when(driverDesc.getTruncatedArrays()).thenReturn(null);

        var desc = new DefaultUpdateDescription(driverDesc);

        assertTrue(desc.getUpdatedFieldValue("missing", String.class).isEmpty());
    }

    @Test
    void rejectsNullDriverDescription() {
        assertThrows(NullPointerException.class, () -> new DefaultUpdateDescription(null));
    }
}
