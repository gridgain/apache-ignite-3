/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.hlc;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runMultiThreaded;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.lang.IgniteBiTuple;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link TrackableHybridClock}.
 */
public class TrackableHybridClockTest {
    @Test
    public void testSimpleWaitFor() {
        TrackableHybridClock clock = new TrackableHybridClock();

        HybridTimestamp ts = clock.now();

        HybridTimestamp ts1 = new HybridTimestamp(ts.getPhysical() + 1_000_000, 0);
        HybridTimestamp ts2 = new HybridTimestamp(ts.getPhysical() + 2_000_000, 0);
        HybridTimestamp ts3 = new HybridTimestamp(ts.getPhysical() + 3_000_000, 0);

        CompletableFuture<Void> f0 = clock.waitFor(ts2, UUID.randomUUID());
        CompletableFuture<Void> f1 = clock.waitFor(ts2, UUID.randomUUID());
        CompletableFuture<Void> f2 = clock.waitFor(ts3, UUID.randomUUID());

        assertFalse(f0.isDone());
        assertFalse(f1.isDone());
        assertFalse(f2.isDone());

        clock.sync(ts1);
        assertFalse(f0.isDone());
        assertFalse(f1.isDone());
        assertFalse(f2.isDone());

        clock.sync(ts2);
        assertThat(f0, willCompleteSuccessfully());
        assertThat(f1, willCompleteSuccessfully());
        assertFalse(f2.isDone());

        clock.sync(ts3);
        assertThat(f2, willCompleteSuccessfully());
    }

    @Test
    public void testMultithreadedWaitFor() throws Exception {
        TrackableHybridClock clock = new TrackableHybridClock();

        clock.now();

        int threads = Runtime.getRuntime().availableProcessors() * 2;

        Collection<IgniteBiTuple<CompletableFuture<Void>, HybridTimestamp>> allFutures = new ConcurrentLinkedDeque<>();

        int iterations = 10_000;

        runMultiThreaded(
                () -> {
                    List<IgniteBiTuple<CompletableFuture<Void>, HybridTimestamp>> prevFutures = new ArrayList<>();
                    ThreadLocalRandom random = ThreadLocalRandom.current();

                    for (int i = 0; i < iterations; i++) {
                        HybridTimestamp now = clock.now();
                        HybridTimestamp timestampToWait =
                            new HybridTimestamp(now.getPhysical() + 1, now.getLogical() + random.nextInt(1000));

                        CompletableFuture<Void> future = clock.waitFor(timestampToWait, UUID.randomUUID());

                        IgniteBiTuple<CompletableFuture<Void>, HybridTimestamp> pair = new IgniteBiTuple<>(future, timestampToWait);

                        prevFutures.add(pair);
                        allFutures.add(pair);

                        if (i % 10 == 0) {
                            for (Iterator<IgniteBiTuple<CompletableFuture<Void>, HybridTimestamp>> it = prevFutures.iterator();
                                    it.hasNext();) {
                                IgniteBiTuple<CompletableFuture<Void>, HybridTimestamp> t = it.next();

                                if (t.get2().compareTo(now) <= 0) {
                                    assertTrue(t.get1().isDone());

                                    it.remove();
                                }
                            }
                        }
                    }

                    return null;
                },
                threads,
                "trackableHybridClockTest"
        );

        Thread.sleep(5);

        clock.now();

        List<IgniteBiTuple<CompletableFuture<Void>, HybridTimestamp>> uncompleted =
                allFutures.stream().filter(f -> !f.get1().isDone()).collect(toList());

        assertTrue(uncompleted.isEmpty());
    }
}
