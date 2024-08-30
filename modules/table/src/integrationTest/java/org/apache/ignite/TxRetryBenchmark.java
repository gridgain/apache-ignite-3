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

package org.apache.ignite;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;

import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.app.IgniteServerImpl;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Feature timeout benchmark.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 20, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class TxRetryBenchmark {
    private IgniteImpl ignite;

    /**
     * Prepare to start the benchmark.
     */
    @Setup
    public void setUp() {
        String cfgString = "ignite {\n"
                + "  network.port: 3344,\n"
                + "  network.nodeFinder.netClusterNodes: [localhost\":\"3344]\n"
                + "  network.membership: {\n"
                + "    membershipSyncInterval: 1000,\n"
                + "    failurePingInterval: 500,\n"
                + "    scaleCube: {\n"
                + "      membershipSuspicionMultiplier: 1,\n"
                + "      failurePingRequestMembers: 1,\n"
                + "      gossipInterval: 10\n"
                + "    },\n"
                + "  },\n"
                + "  raft: {\n"
                + "  fsync: false,\n"
                + "  retryDelay: 20\n"
                + "},\n"
                + "  clientConnector.port: 10800,\n"
                + "  storage: {\n"
                + "    profiles: {default_aimem: { engine: \"aimem\"}}\n"
                + "  },\n"
                + "  rest: {\n"
                + "    port: 10300, \n"
                + "    ssl.port: 10400 \n"
                + "  },\n"
                + "  nodeAttributes: {    nodeAttributes: {}  }}";

        String nodeName = "node0";
        Path workDir = workDir();

        IgniteServer igniteServer = TestIgnitionManager.start(
                nodeName,
                cfgString,
                workDir.resolve(nodeName)
        );

        InitParameters initParameters = InitParameters.builder()
                .metaStorageNodeNames(nodeName)
                .clusterName("cluster")
                .build();

        TestIgnitionManager.init(igniteServer, initParameters);

        ignite = (IgniteImpl) igniteServer.api();
    }

    private Path workDir() {
        return Path.of("build", "work")
                .resolve(this.getClass().getSimpleName())
                .resolve("work_" +  ThreadLocalRandom.current().nextInt(Short.MAX_VALUE));
    }

    /**
     * Closes resources.
     */
    @TearDown
    public void tearDown() throws InterruptedException, ExecutionException {
        ignite.stop();
    }

    /**
     * Benchmark for KV upsert via embedded client.
     */
    @Benchmark
    public void test() {

    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + TxRetryBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }
}
