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
package io.flowwarden.stream.spi.testkit;

import io.flowwarden.stream.spi.DlqPolicy;
import io.flowwarden.stream.spi.DlqStore;
import io.flowwarden.stream.spi.FailedEvent;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Behavior contract that every {@link DlqStore} implementation must satisfy.
 *
 * <p>Subclass for each backend (Mongo, Redis, Kafka, …) and provide a fresh
 * {@link DlqStore} via {@link #createDlqStore()} plus a {@link #cleanState()}
 * hook that resets the backing storage between tests.</p>
 *
 * <p>The contract mirrors the SPI's hot/cold split: the write tests always
 * run, the read tests only run when the subclass declares the replay
 * capability. Like the SPI itself, replay is opt-in: a publish-only
 * backend (queue, log sink) keeps the inherited empty defaults and is
 * green out of the box; a stateful backend that overrides the cold-path
 * {@code find*} methods overrides {@link #supportsReplay()} to return
 * {@code true} — until then its read tests are reported as skipped
 * {@linkplain Assumptions assumptions}, not failures. The backlog count is
 * a separate capability with its own {@link #supportsCount()} hook, so all
 * four combinations stay expressible: publish-only, replay-only,
 * count-only, replay+count.</p>
 */
public abstract class DlqStoreContractTest {

    protected static final DlqPolicy DEFAULT_POLICY = new DlqPolicy(30, true, true);

    protected DlqStore store;

    /**
     * Returns a fresh {@link DlqStore} bound to a clean backend.
     */
    protected abstract DlqStore createDlqStore();

    /**
     * Clears all state in the backing storage so tests are isolated.
     */
    protected abstract void cleanState();

    /**
     * Whether the implementation under test overrides the cold-path read
     * methods ({@code findById} / {@code findByStreamName}). Defaults to
     * {@code false}, mirroring the SPI where replay is absent until a
     * backend implements it: stateful backends override this to return
     * {@code true} to activate the read-contract tests.
     */
    protected boolean supportsReplay() {
        return false;
    }

    /**
     * Whether the implementation under test overrides
     * {@code DlqStore.count}. Independent of {@link #supportsReplay()} —
     * the SPI models them as separate capabilities (a backend may replay
     * without counting, or count without replaying), and the SPI default
     * {@code -1} is a supported value, not a contract failure. Defaults to
     * {@code false}; counting backends override with {@code true} to
     * activate the backlog-count tests.
     */
    protected boolean supportsCount() {
        return false;
    }

    private void assumeReplay() {
        Assumptions.assumeTrue(supportsReplay(),
                "read contract skipped: override supportsReplay() to return true "
                        + "if this backend overrides the cold-path find* methods");
    }

    private void assumeCount() {
        Assumptions.assumeTrue(supportsCount(),
                "count contract skipped: override supportsCount() to return true "
                        + "if this backend overrides DlqStore.count");
    }

    @BeforeEach
    void setUpContract() {
        cleanState();
        store = createDlqStore();
    }

    // --- Write contract: always runs, read-free -------------------------

    @Test
    void saveAcceptsFullyPopulatedEvent() {
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        var event = new FailedEvent(
                "evt-w1", "my-stream", "INSERT",
                new BsonDocument("_id", new BsonString("doc-w1")),
                new Document("status", "NEW").append("amount", 42),
                BsonDocument.parse("{\"_data\": \"resume-w1\"}"),
                new FailedEvent.ErrorInfo("RuntimeException", "boom", "stack trace here"),
                3, FailedEvent.STATUS_PENDING,
                now, now, now, null, Map.of("env", "test")
        );

        store.save(event, DEFAULT_POLICY);
    }

    @Test
    void saveAcceptsNullableFields() {
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        var event = new FailedEvent(
                "evt-w-null", "stream-x", "DELETE",
                null, null, null,
                new FailedEvent.ErrorInfo("Ex", "msg", null),
                1, FailedEvent.STATUS_PENDING,
                now, now, now, null, Collections.emptyMap()
        );

        store.save(event, DEFAULT_POLICY);
    }

    // --- Read contract: requires overridden cold-path reads -------------

    @Test
    void saveAndFindById() {
        assumeReplay();
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        var docKey = new BsonDocument("_id", new BsonString("doc-1"));
        var fullDoc = new Document("status", "NEW").append("amount", 42);
        var resumeToken = BsonDocument.parse("{\"_data\": \"resume-1\"}");
        var error = new FailedEvent.ErrorInfo("RuntimeException", "boom", "stack trace here");
        var metadata = Map.<String, Object>of("env", "test");

        var event = new FailedEvent(
                "evt-1", "my-stream", "INSERT",
                docKey, fullDoc, resumeToken,
                error, 3, FailedEvent.STATUS_PENDING,
                now, now, now, null, metadata
        );

        store.save(event, DEFAULT_POLICY);

        var found = store.findById("evt-1");
        assertTrue(found.isPresent());

        var result = found.get();
        assertEquals("evt-1", result.id());
        assertEquals("my-stream", result.streamName());
        assertEquals("INSERT", result.operationType());
        assertNotNull(result.documentKey());
        assertEquals("NEW", result.fullDocument().getString("status"));
        assertEquals(42, result.fullDocument().getInteger("amount"));
        assertEquals(resumeToken, result.resumeToken());
        assertEquals("RuntimeException", result.error().type());
        assertEquals("boom", result.error().message());
        assertEquals("stack trace here", result.error().stackTrace());
        assertEquals(3, result.attempts());
        assertEquals("PENDING", result.status());
        assertEquals(now, result.firstAttemptAt());
        assertEquals(now, result.lastAttemptAt());
        assertEquals(now, result.createdAt());
        assertNull(result.expiresAt());
        assertEquals("test", result.metadata().get("env"));
    }

    @Test
    void findByIdReturnsEmptyWhenNotFound() {
        assumeReplay();
        assertTrue(store.findById("nonexistent").isEmpty());
    }

    @Test
    void findByStreamName() {
        assumeReplay();
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        store.save(makeEvent("e1", "stream-A", now), DEFAULT_POLICY);
        store.save(makeEvent("e2", "stream-A", now), DEFAULT_POLICY);
        store.save(makeEvent("e3", "stream-B", now), DEFAULT_POLICY);

        var streamAEvents = store.findByStreamName("stream-A");
        assertEquals(2, streamAEvents.size());
        assertTrue(streamAEvents.stream().allMatch(e -> "stream-A".equals(e.streamName())));

        var streamBEvents = store.findByStreamName("stream-B");
        assertEquals(1, streamBEvents.size());
        assertEquals("stream-B", streamBEvents.get(0).streamName());

        var emptyEvents = store.findByStreamName("stream-C");
        assertTrue(emptyEvents.isEmpty());
    }

    @Test
    void roundTripWithNullableFields() {
        assumeReplay();
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        var event = new FailedEvent(
                "evt-null", "stream-x", "DELETE",
                null, null, null,
                new FailedEvent.ErrorInfo("Ex", "msg", null),
                1, FailedEvent.STATUS_PENDING,
                now, now, now, null, Collections.emptyMap()
        );

        store.save(event, DEFAULT_POLICY);

        var found = store.findById("evt-null").orElseThrow();
        assertNull(found.documentKey());
        assertNull(found.fullDocument());
        assertNull(found.resumeToken());
        assertNull(found.error().stackTrace());
        assertNull(found.expiresAt());
    }

    @Test
    void saveOverwritesExisting() {
        assumeReplay();
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        store.save(makeEvent("e1", "s", now), DEFAULT_POLICY);
        store.save(new FailedEvent(
                "e1", "s-updated", "UPDATE",
                null, null, null,
                new FailedEvent.ErrorInfo("Ex", "updated", null),
                5, FailedEvent.STATUS_PENDING,
                now, now, now, null, Collections.emptyMap()
        ), DEFAULT_POLICY);

        var found = store.findById("e1").orElseThrow();
        assertEquals("s-updated", found.streamName());
        assertEquals(5, found.attempts());
    }

    @Test
    void countIsZeroOnEmptyBacklog() {
        assumeCount();
        assertEquals(0, store.count("no-such-stream"));
    }

    @Test
    void countReturnsPendingEntriesPerStream() {
        assumeCount();
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        store.save(makeEvent("c1", "stream-A", now), DEFAULT_POLICY);
        store.save(makeEvent("c2", "stream-A", now), DEFAULT_POLICY);
        store.save(makeEvent("c3", "stream-B", now), DEFAULT_POLICY);

        assertEquals(2, store.count("stream-A"));
        assertEquals(1, store.count("stream-B"));
        assertEquals(0, store.count("stream-C"));
    }

    @Test
    void countIgnoresNonPendingEntries() {
        assumeCount();
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        store.save(makeEvent("p1", "stream-A", now), DEFAULT_POLICY);
        // A reprocessed/acknowledged entry is no longer backlog.
        store.save(new FailedEvent(
                "p2", "stream-A", "INSERT",
                null, null, null,
                new FailedEvent.ErrorInfo("TestEx", "test", null),
                1, "REPROCESSED",
                now, now, now, null, Collections.emptyMap()
        ), DEFAULT_POLICY);

        assertEquals(1, store.count("stream-A"));
    }

    protected static FailedEvent makeEvent(String id, String streamName, Instant now) {
        return new FailedEvent(
                id, streamName, "INSERT",
                null, null, null,
                new FailedEvent.ErrorInfo("TestEx", "test", null),
                1, FailedEvent.STATUS_PENDING,
                now, now, now, null, Collections.emptyMap()
        );
    }
}
