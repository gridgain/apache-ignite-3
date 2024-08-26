package org.apache.ignite.raft.jraft.core;

import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.junit.jupiter.api.Test;

public class ItNodeVtTest extends ItNodeTest {
    @Override
    protected NodeOptions createNodeOptions(int nodeIdx) {
        NodeOptions options = super.createNodeOptions(nodeIdx);
        options.setUseVirtualThreads(true);

        return options;
    }

    @Override
    @Test
    public void testSingleNode() throws Exception {
        System.setProperty("jdk.virtualThreadScheduler.parallelism", "1");
        System.setProperty("jdk.virtualThreadScheduler.maxPoolSize", "1");
        System.setProperty("jdk.virtualThreadScheduler.minRunnable", "1");

        super.testSingleNode();
    }
}
