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

package org.apache.ignite.example;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.thread.StripedThreadPoolExecutor;

/**
 * Tests for ExecutorService. Add system property "-Dotel.instrumentation.executors.enabled=false" to disable instrumentation.
 */
public class FutureExample {
    private static final IgniteLogger LOG = Loggers.forClass(FutureExample.class);

    private final StripedThreadPoolExecutor stripedThreadPoolExecutor;

    private FutureExample() {
        stripedThreadPoolExecutor = new StripedThreadPoolExecutor(
                2,
                NamedThreadFactory.create("test", "example-execution-pool", LOG),
                false,
                0
        );
    }

    private void start() {
        var futs = asList(submitToPool(), submitToPool());

        futs.forEach(CompletableFuture::join);
    }

    private CompletableFuture<?> submitToPool() {
        return stripedThreadPoolExecutor.submit(() -> process(1L), 0)
                .thenAccept(unused -> process(2L));
    }

    private void process(long delay) {
        try {
            SECONDS.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Main method of the example.
     *
     * @param args The command line arguments.
     */
    public static void main(String[] args) {
        new FutureExample().start();
    }
}