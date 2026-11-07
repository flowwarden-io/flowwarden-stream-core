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
package io.flowwarden.stream.internal.checkpoint;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;

import static org.assertj.core.api.Assertions.assertThat;

class HistoryLostErrorsTest {

    private static MongoCommandException command(int code, String codeName) {
        return new MongoCommandException(
                BsonDocument.parse("{\"ok\": 0.0, \"code\": " + code + ", \"codeName\": \"" + codeName
                        + "\", \"errmsg\": \"boom\"}"),
                new ServerAddress("localhost", 27017));
    }

    @Test
    void nonResumableServerCodes_areHistoryLost() {
        assertThat(HistoryLostErrors.isHistoryLost(command(286, "ChangeStreamHistoryLost"))).isTrue();
        assertThat(HistoryLostErrors.isHistoryLost(command(136, "CappedPositionLost"))).isTrue();
        assertThat(HistoryLostErrors.isHistoryLost(command(280, "ChangeStreamFatalError"))).isTrue();
    }

    @Test
    void nonResumableLabel_isHistoryLost_whateverTheCode() {
        MongoException labelled = new MongoException(9999, "custom");
        labelled.addLabel("NonResumableChangeStreamError");

        assertThat(HistoryLostErrors.isHistoryLost(labelled)).isTrue();
    }

    @Test
    void wrappedCause_isFound_throughTranslationAndReactorLayers() {
        // Spring Data translates the driver exception; Reactor and the
        // reading task wrap again. The classification walks the chain.
        Throwable wrapped = new IllegalStateException("terminated",
                new DataAccessResourceFailureException("translated",
                        command(286, "ChangeStreamHistoryLost")));

        assertThat(HistoryLostErrors.isHistoryLost(wrapped)).isTrue();
    }

    @Test
    void otherErrors_areNotHistoryLost() {
        assertThat(HistoryLostErrors.isHistoryLost(command(11600, "InterruptedAtShutdown"))).isFalse();
        assertThat(HistoryLostErrors.isHistoryLost(new RuntimeException("socket timeout"))).isFalse();
        assertThat(HistoryLostErrors.isHistoryLost(null)).isFalse();
    }

    @Test
    void signature_isTheDriverExceptionClassAndCode_orTheThrowableClass() {
        assertThat(HistoryLostErrors.signatureOf(
                new RuntimeException("x", command(286, "ChangeStreamHistoryLost"))))
                .isEqualTo(MongoCommandException.class.getName() + ":286");
        assertThat(HistoryLostErrors.signatureOf(
                new RuntimeException("y", command(286, "ChangeStreamHistoryLost"))))
                .as("same failure again, whatever the message")
                .isEqualTo(HistoryLostErrors.signatureOf(
                        new RuntimeException("x", command(286, "ChangeStreamHistoryLost"))));
        assertThat(HistoryLostErrors.signatureOf(new IllegalStateException("plain")))
                .isEqualTo(IllegalStateException.class.getName());
    }
}
