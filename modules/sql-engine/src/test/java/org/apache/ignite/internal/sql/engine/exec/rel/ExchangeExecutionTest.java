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

package org.apache.ignite.internal.sql.engine.exec.rel;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import it.unimi.dsi.fastutil.longs.Long2ObjectMaps;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.sql.engine.AsyncCursor.BatchedResult;
import org.apache.ignite.internal.sql.engine.exec.ExchangeService;
import org.apache.ignite.internal.sql.engine.exec.ExchangeServiceImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.MailboxRegistry;
import org.apache.ignite.internal.sql.engine.exec.MailboxRegistryImpl;
import org.apache.ignite.internal.sql.engine.framework.DataProvider;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.message.MessageService;
import org.apache.ignite.internal.sql.engine.message.MessageServiceImpl;
import org.apache.ignite.internal.sql.engine.metadata.FragmentDescription;
import org.apache.ignite.internal.sql.engine.trait.AllNodes;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests to verify Outbox to Inbox interoperation.
 */
public class ExchangeExecutionTest extends AbstractExecutionTest {
    private static final String ROOT_NODE_NAME = "N1";
    private static final String SECOND_NODE_NAME = "N2";
    private static final ClusterNode ROOT_NODE =
            new ClusterNode(ROOT_NODE_NAME, ROOT_NODE_NAME, NetworkAddress.from("127.0.0.1:10001"));
    private static final ClusterNode SECOND_NODE =
            new ClusterNode(SECOND_NODE_NAME, SECOND_NODE_NAME, NetworkAddress.from("127.0.0.1:10002"));
    private static final List<ClusterNode> NODES = List.of(ROOT_NODE, SECOND_NODE);
    private static final int SOURCE_FRAGMENT_ID = 0;
    private static final int TARGET_FRAGMENT_ID = 1;

    @ParameterizedTest(name = "rowCount={0}, prefetch={1}, ordered={2}")
    @MethodSource("testArgs")
    public void test(int rowCount, boolean prefetch, boolean ordered) {
        UUID queryId = UUID.randomUUID();

        int nodesCnt = 2;

        List<MailboxRegistry> mailBoxes = makeMailBoxes(nodesCnt);

        List<ExchangeService> exchangeService = createExchangeService(mailBoxes, NODES);

        List<Outbox<?>> outboxes = createSourceFragment(
                queryId,
                exchangeService,
                NODES,
                mailBoxes,
                rowCount
        );

        if (prefetch) {
            for (Outbox<?> outbox : outboxes) {
                await(outbox.context().submit(outbox::prefetch, outbox::onError));
            }
        }

        AsyncRootNode<Object[], Object[]> root = createRootFragment(
                queryId,
                ordered,
                NODES,
                exchangeService.get(0),
                mailBoxes.get(0)
        );

        BatchedResult<Object[]> res = await(root.requestNextAsync(rowCount));

        assertEquals(rowCount, res.items().size());

        if (ordered) {
            checkOrdered(res.items());
        }
    }

    private static void checkOrdered(List<Object[]> items) {
        List<Integer> collInitial = items.stream().map(vals -> (int) vals[0]).collect(Collectors.toList());

        assertEquals(items.stream().map(vals -> (int) vals[0]).sorted().collect(Collectors.toList()), collInitial);
    }

    private static Stream<Arguments> testArgs() {
        List<Integer> sizes = List.of(
                // half of the batch size
                Math.min(Commons.IO_BATCH_SIZE / 2, 1),

                // full batch
                Commons.IO_BATCH_SIZE,

                // full batch + one extra row
                Commons.IO_BATCH_SIZE + 1,

                // several batches
                2 * Commons.IO_BATCH_SIZE + 1,

                // more that count of so called "in-flight" batches. In flight batches
                // are batches that have been sent but not yet acknowledged
                2 * Commons.IO_BATCH_SIZE * Commons.IO_BATCH_COUNT
        );

        List<Boolean> trueFalseList = List.of(true, false);

        List<Arguments> args = new ArrayList<>(2 * sizes.size());
        for (int size : sizes) {
            for (boolean prefetch : trueFalseList) {
                for (boolean ordered : trueFalseList) {
                    args.add(Arguments.of(size, prefetch, ordered));
                }
            }
        }

        return args.stream();
    }

    private AsyncRootNode<Object[], Object[]> createRootFragment(
            UUID queryId,
            boolean ordered,
            List<ClusterNode> nodes,
            ExchangeService exchangeService,
            MailboxRegistry mailboxRegistry
    ) {
        ExecutionContext<Object[]> targetCtx = TestBuilders.executionContext()
                .queryId(queryId)
                .executor(taskExecutor)
                .fragment(new FragmentDescription(TARGET_FRAGMENT_ID, null, null, Long2ObjectMaps.emptyMap()))
                .localNode(ROOT_NODE)
                .build();

        Comparator<Object[]> comparator = ordered ? Comparator.comparingInt(o -> (Integer) o[0]) : null;

        List<String> nodeNames = nodes.stream().map(ClusterNode::name).collect(Collectors.toList());

        var inbox = new Inbox<>(
                targetCtx, exchangeService, mailboxRegistry, nodeNames, comparator, SOURCE_FRAGMENT_ID, SOURCE_FRAGMENT_ID
        );

        mailboxRegistry.register(inbox);

        var root = new AsyncRootNode<>(
                inbox, Function.identity()
        );

        inbox.onRegister(root);

        return root;
    }

    private List<Outbox<?>> createSourceFragment(
            UUID queryId,
            List<ExchangeService> exchangeService,
            List<ClusterNode> nodes,
            List<MailboxRegistry> mailBoxes,
            int rowCount
    ) {
        assert nodes.size() == exchangeService.size();

        List<Outbox<?>> outBoxes = new ArrayList<>();

        for (int i = 0; i < exchangeService.size(); ++i) {
            ClusterNode node = nodes.get(i);
            ExchangeService exchange = exchangeService.get(i);
            MailboxRegistry mBox = mailBoxes.get(i);

            ExecutionContext<Object[]> sourceCtx = TestBuilders.executionContext()
                    .queryId(queryId)
                    .executor(taskExecutor)
                    .fragment(new FragmentDescription(SOURCE_FRAGMENT_ID, null, null, Long2ObjectMaps.emptyMap()))
                    .localNode(node)
                    .build();

            var outbox = new Outbox<>(
                    sourceCtx, exchange, mBox, SOURCE_FRAGMENT_ID, TARGET_FRAGMENT_ID, new AllNodes<>(List.of(ROOT_NODE_NAME))
            );

            mBox.register(outbox);

            var source = new ScanNode<>(sourceCtx, DataProvider.fromRow(new Object[]{i, i}, rowCount));

            outbox.register(source);

            outBoxes.add(outbox);
        }

        return outBoxes;
    }

    private static List<MailboxRegistry> makeMailBoxes(int nodes) {
        List<MailboxRegistry> mBoxes = new ArrayList<>(nodes);

        for (int i = 0; i < nodes; ++i) {
            mBoxes.add(new MailboxRegistryImpl());
        }

        return mBoxes;
    }

    private List<ExchangeService> createExchangeService(List<MailboxRegistry> mailBoxes, List<ClusterNode> nodes) {
        List<ClusterService> clusterServices =
                TestBuilders.clusterService(nodes.stream().map(ClusterNode::name).collect(Collectors.toList()));
        List<ExchangeService> services = new ArrayList<>(mailBoxes.size());

        assert mailBoxes.size() == clusterServices.size();

        for (int i = 0; i < mailBoxes.size(); ++i) {
            ClusterService clusterService = clusterServices.get(i);

            MessageService messageService = new MessageServiceImpl(
                    clusterService.topologyService(),
                    clusterService.messagingService(),
                    taskExecutor,
                    new IgniteSpinBusyLock()
            );

            ExchangeService exchangeService = new ExchangeServiceImpl(
                    ROOT_NODE,
                    taskExecutor,
                    mailBoxes.get(i),
                    messageService
            );

            messageService.start();
            exchangeService.start();

            services.add(exchangeService);
        }

        return services;
    }
}
