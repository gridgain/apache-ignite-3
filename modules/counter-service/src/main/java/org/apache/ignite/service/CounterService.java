package org.apache.ignite.service;

import java.util.UUID;
import java.util.concurrent.Future;
import org.apache.ignite.command.GetCommand;
import org.apache.ignite.command.IncrementAndGetCommand;
import org.apache.ignite.lang.IgniteUuidGenerator;

/**
 * Counter service client.
 */
public class CounterService {
    private final IgniteUuidGenerator igniteUuidGenerator = new IgniteUuidGenerator(UUID.randomUUID(), 1L);

    private final SingleGroupRaftService raftService;

    public CounterService(RoutingTable routingTable) {
        raftService = new SingleGroupRaftService(
            routingTable,
            new RaftGroupId(RaftGroupId.GroupType.COUNTER, 0));
    }

    /**
     * Get current value from counter
     */
    Future get() {
        return raftService.command(new GetCommand(igniteUuidGenerator.randomUuid()));
    }

    /**
     * Add delta to counter then get value
     */
    Future incrementAndGet(final long delta) {
        return raftService.command(new IncrementAndGetCommand(igniteUuidGenerator.randomUuid(), delta));
    }
}
