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

package org.apache.ignite.internal.tx;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.apache.ignite.internal.tx.test.LockConflictMatcher.conflictsWith;
import static org.apache.ignite.internal.tx.test.LockWaiterMatcher.waitsFor;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.tx.impl.WoundWaitDeadlockPreventionPolicy;
import org.apache.ignite.internal.tx.test.LockWaiterMatcher;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link WoundWaitDeadlockPreventionPolicy}.
 */
public class WoundWaitDeadlockPreventionTest extends AbstractDeadlockPreventionTest {
    private static ExecutorService failExecutor;

    @Override
    protected Matcher<CompletableFuture<Lock>> conflictMatcher(UUID txId) {
        return waitsFor(txId);
    }

    @BeforeAll
    static void beforeAll() {
        failExecutor = Executors.newSingleThreadExecutor();
    }

    @AfterAll
    static void shutdown() throws InterruptedException {
        failExecutor.shutdown();
        failExecutor.awaitTermination(5, TimeUnit.MILLISECONDS);
    }

    @Override
    protected DeadlockPreventionPolicy deadlockPreventionPolicy() {
        return new WoundWaitDeadlockPreventionPolicy() {
            @Override
            public void failAction(UUID owner) {
                //failExecutor.execute(() -> lockManager.releaseAll(owner));
                // No-op. Will causes waiting instead of triggering a conflict.
            }
        };
    }

    @Test
    @Disabled
    public void testLockOrderAfterRelease2() {
        var tx1 = beginTx();
        var tx2 = beginTx();
        var tx3 = beginTx();
        var tx4 = beginTx();

        var k = lockKey("test");

        assertThat(xlock(tx1, k), willSucceedFast());

        CompletableFuture<?> futTx2 = slock(tx2, k);
        assertFalse(futTx2.isDone());

        CompletableFuture<?> futTx3 = xlock(tx3, k);
        assertFalse(futTx3.isDone());

        CompletableFuture<?> futTx4 = slock(tx4, k);
        assertFalse(futTx4.isDone());

        commitTx(tx1);

        assertThat(futTx2, willSucceedFast());
        assertThat(futTx4, willSucceedFast());
        assertFalse(futTx3.isDone());

        commitTx(tx4);
        commitTx(tx2);

        assertThat(futTx3, willSucceedFast());
    }
}
