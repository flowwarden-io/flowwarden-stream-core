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

import io.flowwarden.stream.FlowWardenMetrics;
import io.flowwarden.stream.spi.Checkpoint;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.StreamMetricsProvider;
import org.bson.BsonDocument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit contract of the per-stream processed-anchor policy: single-lock
 * serialization of its three writers (count threshold, time threshold,
 * manual save), counter meaning "settlements since the last CONFIRMED
 * persistence", and honest manual-save results.
 */
class ProcessedAnchorPolicyTest {

    private static final String STREAM = "anchor-test";
    private static final Instant NOW = Instant.now();
    private static final BsonDocument T1 = BsonDocument.parse("{\"_data\": \"t1\"}");
    private static final BsonDocument T2 = BsonDocument.parse("{\"_data\": \"t2\"}");
    private static final BsonDocument T3 = BsonDocument.parse("{\"_data\": \"t3\"}");

    private RecordingStore store;

    @BeforeEach
    void setUp() {
        store = new RecordingStore();
        FlowWardenMetrics.setProvider(StreamMetricsProvider.noOp());
    }

    @AfterEach
    void tearDown() {
        FlowWardenMetrics.setProvider(StreamMetricsProvider.noOp());
    }

    // --- serialization: a slow older write can never clobber a newer one ---

    @Test
    void slowFlushOfAnOlderToken_neverLandsAfterANewerThresholdWrite() throws Exception {
        // The review's T1/T2 scenario, made deterministic: the flush write of
        // T1 blocks inside the store while a settlement reaching the count
        // threshold wants to persist T2. The policy lock forces T2 to wait —
        // the store observes T1 then T2, never T2 then T1.
        ProcessedAnchorPolicy policy = new ProcessedAnchorPolicy(STREAM, store, 1);
        CountDownLatch writeEntered = new CountDownLatch(1);
        CountDownLatch releaseWrite = new CountDownLatch(1);
        store.blockOn = T1;
        store.writeEntered = writeEntered;
        store.releaseWrite = releaseWrite;

        // Seed the dirty state without triggering the count threshold.
        ProcessedAnchorPolicy flushSide = policy; // one policy, three writers
        Thread flush = new Thread(() -> {
            // Simulate the timer: T1 is the latest settled at flush time.
            flushSide.saveNow(T1, NOW); // enters the store and blocks
        }, "flush-side");
        flush.start();
        assertThat(writeEntered.await(5, TimeUnit.SECONDS)).isTrue();

        // Delivery side: T2 settles and reaches the threshold while T1's
        // write is still in flight.
        Thread delivery = new Thread(() -> flushSide.onSettled(T2, NOW), "delivery-side");
        delivery.start();
        // Give the delivery thread time to hit the lock (it must NOT write).
        Thread.sleep(200);
        assertThat(store.writes).as("T2 must wait behind T1's in-flight write").isEmpty();

        releaseWrite.countDown();
        flush.join(5_000);
        delivery.join(5_000);

        assertThat(store.writes)
                .as("serialized: the older write lands first, the newer last")
                .containsExactly("t1", "t2");
        assertThat(store.lastProcessed).isEqualTo(T2);
    }

    // --- counter: settlements since the last CONFIRMED persistence ---

    @Test
    void timeThresholdWrite_resetsTheCounter_soTheNextCountWriteIsAFullNAway() {
        ProcessedAnchorPolicy policy = new ProcessedAnchorPolicy(STREAM, store, 3);

        policy.onSettled(T1, NOW);
        policy.onSettled(T2, NOW);
        assertThat(store.writes).isEmpty(); // 2 < 3

        policy.flushIfDirty(); // timer confirms T2
        assertThat(store.writes).containsExactly("t2");

        // A flush right before the old absolute threshold: the counter
        // restarted, so the 3rd absolute settlement does NOT write.
        policy.onSettled(T3, NOW);
        assertThat(store.writes).containsExactly("t2");
    }

    @Test
    void failedThresholdWrite_isRetriedByTheVeryNextSettlement_notAtTwoN() {
        ProcessedAnchorPolicy policy = new ProcessedAnchorPolicy(STREAM, store, 2);

        policy.onSettled(T1, NOW);
        store.throwOnWrite = true;
        policy.onSettled(T2, NOW); // threshold reached, write fails
        assertThat(store.writes).isEmpty();

        store.throwOnWrite = false;
        policy.onSettled(T3, NOW); // still >= N since last CONFIRMED write
        assertThat(store.writes)
                .as("the N-1 replay bound holds through store failures")
                .containsExactly("t3");
    }

    @Test
    void failedThresholdWrite_isAlsoRetriedByTheTimer() {
        ProcessedAnchorPolicy policy = new ProcessedAnchorPolicy(STREAM, store, 1);

        store.throwOnWrite = true;
        policy.onSettled(T1, NOW);
        assertThat(store.writes).isEmpty();

        store.throwOnWrite = false;
        policy.flushIfDirty();
        assertThat(store.writes).containsExactly("t1");
    }

    @Test
    void cleanAnchor_isNeverRewrittenByTheTimer() {
        ProcessedAnchorPolicy policy = new ProcessedAnchorPolicy(STREAM, store, 1);

        policy.onSettled(T1, NOW); // threshold writes
        policy.flushIfDirty();
        policy.flushIfDirty();

        assertThat(store.writes).containsExactly("t1");
    }

    // --- manual save: honest result, no double write ---

    @Test
    void successfulManualSave_makesTheTokenClean_forCountAndTimer() {
        ProcessedAnchorPolicy policy = new ProcessedAnchorPolicy(STREAM, store, 1);

        assertThat(policy.saveNow(T1, NOW)).isTrue();
        assertThat(store.writes).containsExactly("t1");

        // The same token then settles: already durable — no second write,
        // and the counter resets instead of counting.
        policy.onSettled(T1, NOW);
        policy.flushIfDirty();
        assertThat(store.writes).containsExactly("t1");
    }

    @Test
    void failedManualSave_reportsFalse_andTheAutomaticPolicyRetries() {
        ProcessedAnchorPolicy policy = new ProcessedAnchorPolicy(STREAM, store, 100);

        store.throwOnWrite = true;
        assertThat(policy.saveNow(T1, NOW))
                .as("a failed write must never be declared durable")
                .isFalse();

        // The settlement of that same event stays dirty; the timer repairs.
        policy.onSettled(T1, NOW);
        store.throwOnWrite = false;
        policy.flushIfDirty();
        assertThat(store.writes).containsExactly("t1");
    }

    @Test
    void cancelled_neverWrites() {
        ProcessedAnchorPolicy policy = new ProcessedAnchorPolicy(STREAM, store, 1);
        policy.cancel();

        policy.onSettled(T1, NOW);
        policy.flushIfDirty();
        assertThat(policy.saveNow(T1, NOW)).isFalse();

        assertThat(store.writes).isEmpty();
    }

    private static final class RecordingStore implements CheckpointStore {
        final List<String> writes = new ArrayList<>();
        BsonDocument lastProcessed;
        boolean throwOnWrite;
        BsonDocument blockOn;
        CountDownLatch writeEntered;
        CountDownLatch releaseWrite;

        @Override
        public void saveProcessed(String streamName, BsonDocument token, Instant timestamp,
                                  Instant heartbeatTimestamp) {
            if (blockOn != null && blockOn.equals(token) && writeEntered != null) {
                writeEntered.countDown();
                try {
                    releaseWrite.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (throwOnWrite) {
                throw new RuntimeException("store down");
            }
            synchronized (writes) {
                writes.add(token.getString("_data").getValue());
            }
            lastProcessed = token;
        }

        @Override
        public void save(Checkpoint checkpoint) {
        }

        @Override
        public Optional<Checkpoint> findByStreamName(String streamName) {
            return Optional.empty();
        }

        @Override
        public void delete(String streamName) {
        }
    }
}
