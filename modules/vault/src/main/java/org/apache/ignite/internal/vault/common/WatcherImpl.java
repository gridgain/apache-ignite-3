/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.vault.common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;

public class WatcherImpl implements Watcher {
    private final BlockingQueue<Value> queue = new LinkedBlockingQueue<>();

    private final Map<IgniteUuid, Watch> watches = new HashMap<>();

    private volatile boolean stop;

    private final Object mux = new Object();

    private final ExecutorService exec;

    public WatcherImpl() {
        exec = Executors.newFixedThreadPool(1);

        exec.execute(new WatcherWorker());
    }

    @Override public CompletableFuture<IgniteUuid> register(@NotNull Watch watch) {
        synchronized (mux) {
            IgniteUuid key = new IgniteUuid(UUID.randomUUID(), 0);

            watches.put(key, watch);

            return CompletableFuture.completedFuture(key);
        }
    }

    @Override public void notify(@NotNull Value val) {
        queue.offer(val);
    }

    @Override
    public void cancel(@NotNull IgniteUuid uuid) {
        synchronized (mux) {
            watches.remove(uuid);
        }
    }

    public void shutdown() {
        stop = true;

        if (exec != null) {
            List<Runnable> tasks = exec.shutdownNow();

            if (!tasks.isEmpty())
                System.out.println("Runnable tasks outlived thread pool executor service");

            try {
                exec.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException ignored) {
                System.out.println("Got interrupted while waiting for executor service to stop.");

                exec.shutdownNow();

                // Preserve interrupt status.
                Thread.currentThread().interrupt();
            }
        }
    }

    private class WatcherWorker implements Runnable {
        @Override public void run() {
            while (!stop) {
                try {
                    Value val = queue.poll(100, TimeUnit.MILLISECONDS);

                    if (val != null) {
                        synchronized (mux) {
                            watches.forEach((k, w) -> w.notify(val));
                        }
                    }
                }
                catch (InterruptedException interruptedException) {
                    // No-op.
                }
            }
        }
    }
}
