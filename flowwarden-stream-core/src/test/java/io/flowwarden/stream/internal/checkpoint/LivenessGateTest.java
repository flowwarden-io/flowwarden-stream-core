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

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The gate is the mutual exclusion between "a handler starts" and "the
 * stream is declared stuck": exactly one of the two transitions wins.
 */
class LivenessGateTest {

    @Test
    void handlingBlocksTheVerdict_untilExit() {
        LivenessGate gate = new LivenessGate();

        assertThat(gate.enterHandling()).isTrue();
        assertThat(gate.claimStuck()).as("deferred while a dispatch is in flight").isFalse();
        assertThat(gate.enterHandling()).as("dispatches are sequential per stream").isFalse();

        gate.exitHandling();
        assertThat(gate.claimStuck()).isTrue();
    }

    @Test
    void stuckBlocksEveryLaterHandler_andIsClaimedOnce() {
        LivenessGate gate = new LivenessGate();

        assertThat(gate.claimStuck()).isTrue();
        assertThat(gate.claimStuck()).isFalse();
        assertThat(gate.enterHandling()).as("the generation is dead — the event is dropped").isFalse();
        assertThat(gate.isStuck()).isTrue();

        gate.exitHandling(); // a stray exit must not resurrect the generation
        assertThat(gate.isStuck()).isTrue();
    }

    @Test
    void racingHandlerAndVerdict_exactlyOneWins() throws Exception {
        // Review finding: a plain flag read-then-act lets a handler start
        // between the heartbeat's read and the manager's eviction. With the
        // CAS gate, however the two threads interleave, one wins and the
        // other observes it.
        for (int round = 0; round < 2_000; round++) {
            LivenessGate gate = new LivenessGate();
            CyclicBarrier go = new CyclicBarrier(2);
            AtomicInteger handlerWon = new AtomicInteger();
            AtomicInteger verdictWon = new AtomicInteger();
            CountDownLatch done = new CountDownLatch(2);

            Thread handler = new Thread(() -> {
                await(go);
                if (gate.enterHandling()) {
                    handlerWon.incrementAndGet();
                }
                done.countDown();
            });
            Thread verdict = new Thread(() -> {
                await(go);
                if (gate.claimStuck()) {
                    verdictWon.incrementAndGet();
                }
                done.countDown();
            });
            handler.start();
            verdict.start();
            done.await();

            assertThat(handlerWon.get() + verdictWon.get())
                    .as("round %s: exactly one transition wins", round)
                    .isEqualTo(1);
            if (handlerWon.get() == 1) {
                assertThat(gate.isHandling()).isTrue();
            } else {
                assertThat(gate.isStuck()).isTrue();
            }
        }
    }

    private static void await(CyclicBarrier barrier) {
        try {
            barrier.await();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
