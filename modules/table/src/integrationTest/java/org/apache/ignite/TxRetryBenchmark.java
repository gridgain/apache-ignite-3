/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite;

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.MismatchingTransactionOutcomeException;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.Test;

public class TxRetryBenchmark extends ClusterPerTestIntegrationTest {
    @Override
    protected int initialNodes() {
        return 3;
    }

    private IgniteImpl anyNode() {
        return runningNodes().map(TestWrappers::unwrapIgniteImpl).findFirst().orElseThrow();
    }

    @Test
    public void test() throws ExecutionException, InterruptedException {
        int keysUpperBound = 10;

        IgniteImpl ignite = anyNode();

        IgniteSql sql = ignite.sql();

        String zoneName = "TEST_ZONE";
        String tableName = "TEST";

        sql.execute(null, String.format("CREATE ZONE IF NOT EXISTS %s WITH REPLICAS=%d, PARTITIONS=%d, STORAGE_PROFILES='%s'",
                zoneName, initialNodes(), 10, DEFAULT_STORAGE_PROFILE));
        sql.execute(null, "CREATE TABLE IF NOT EXISTS " + tableName
                + "(id INT PRIMARY KEY, name VARCHAR) WITH PRIMARY_ZONE='" + zoneName + "';");

        for (int i = 0; i < keysUpperBound; i++) {
            sql.execute(null, "INSERT INTO " + tableName + "(id, name) VALUES (?, ?)", i, "val_" + i);
        }

        RecordView recordView = ignite.tables().table(tableName).recordView();

        measure(60_000, recordView, ignite.transactions(), keysUpperBound);
        measure(60_000, recordView, ignite.transactions(), keysUpperBound);

        measure(60_000, recordView, ignite.transactions(), keysUpperBound);
    }

    private void measure(
            long time,
            RecordView view,
            IgniteTransactions transactions,
            int keyUpperBound
    ) throws InterruptedException, ExecutionException {
        int cores = Runtime.getRuntime().availableProcessors();

        long startTime = System.currentTimeMillis();
        long untilTime = startTime + time;

        List<AtomicLong> txCounters = new ArrayList<>();
        List<AtomicLong> opsTimes = new ArrayList<>();
        List<AtomicInteger> rolledBackTxnsList = new ArrayList<>();

        for (int i = 0; i < cores; i++) {
            txCounters.add(new AtomicLong());
            opsTimes.add(new AtomicLong());
            rolledBackTxnsList.add(new AtomicInteger());
        }

        ExecutorService executor = Executors.newFixedThreadPool(cores);

        List<Future> futures = new ArrayList<>();

        for (int i = 0; i < cores; i++) {
            AtomicLong txCounter = txCounters.get(i);
            AtomicLong opsTime = opsTimes.get(i);
            AtomicInteger rolledBackTxns = rolledBackTxnsList.get(i);
            futures.add(executor.submit(() -> doTxs(view, transactions, untilTime, keyUpperBound, txCounter, opsTime, rolledBackTxns)));
        }

        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        for (Future f : futures) {
            f.get();
        }

        long totalTxs = txCounters.stream().mapToLong(AtomicLong::get).sum();
        long totalDuration = opsTimes.stream().mapToLong(AtomicLong::get).sum();
        int rolledBackTxns = rolledBackTxnsList.stream().mapToInt(AtomicInteger::get).sum();

        double avgDuration = totalDuration / 1_000_000.0 / totalTxs;

        System.out.println("total txns: " + totalTxs);
        System.out.println("rolled back txns: " + rolledBackTxns);
        System.out.printf("avg upsert duration: %f\n", avgDuration);
    }

    private void doTxs(
            RecordView view,
            IgniteTransactions transactions,
            long untilTimeout,
            int keyUpperBound,
            AtomicLong txCounter,
            AtomicLong opsTime,
            AtomicInteger rolledBackTxns
    ) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        long currentTime = System.currentTimeMillis();

        while (currentTime < untilTimeout) {
            Transaction tx = transactions.begin();

            int key = random.nextInt(keyUpperBound);

            try {
                long upsertStart = System.nanoTime();
                view.upsert(tx, Tuple.create().set("id", key).set("name", "val_" + random.nextInt(keyUpperBound)));
                long upsertDuration = System.nanoTime() - upsertStart;
                opsTime.addAndGet(upsertDuration);
            } catch (Exception e) {
                System.out.println("qqq " + e);

                boolean rolledBack = true;
                try {
                    tx.rollback();
                } catch (MismatchingTransactionOutcomeException ex) {
                    rolledBack = false;
                }

                if (rolledBack) {
                    rolledBackTxns.incrementAndGet();
                }
            }

            try {
                tx.commit();
            } catch (MismatchingTransactionOutcomeException ex) {
                rolledBackTxns.incrementAndGet();
            }

            currentTime = System.currentTimeMillis();

            txCounter.incrementAndGet();
        }
    }
}
