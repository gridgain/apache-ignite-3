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

package org.apache.ignite.internal.sql.engine.exec;

import static org.apache.ignite.internal.sql.engine.externalize.RelJsonReader.fromJson;
import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.lang.ErrorGroups.Sql.DDL_EXEC_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.MESSAGE_SEND_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.NODE_LEFT_ERR;

import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.configuration.ConfigurationChangeException;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.sql.engine.AsyncCursor;
import org.apache.ignite.internal.sql.engine.exec.ddl.DdlCommandHandler;
import org.apache.ignite.internal.sql.engine.exec.rel.AbstractNode;
import org.apache.ignite.internal.sql.engine.exec.rel.AsyncRootNode;
import org.apache.ignite.internal.sql.engine.exec.rel.Outbox;
import org.apache.ignite.internal.sql.engine.message.ErrorMessage;
import org.apache.ignite.internal.sql.engine.message.MessageService;
import org.apache.ignite.internal.sql.engine.message.QueryCloseMessage;
import org.apache.ignite.internal.sql.engine.message.QueryStartRequest;
import org.apache.ignite.internal.sql.engine.message.QueryStartResponse;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessageGroup;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessagesFactory;
import org.apache.ignite.internal.sql.engine.metadata.ColocationGroup;
import org.apache.ignite.internal.sql.engine.metadata.FragmentDescription;
import org.apache.ignite.internal.sql.engine.metadata.MappingService;
import org.apache.ignite.internal.sql.engine.metadata.MappingServiceImpl;
import org.apache.ignite.internal.sql.engine.metadata.NodeWithTerm;
import org.apache.ignite.internal.sql.engine.metadata.RemoteException;
import org.apache.ignite.internal.sql.engine.prepare.DdlPlan;
import org.apache.ignite.internal.sql.engine.prepare.ExplainPlan;
import org.apache.ignite.internal.sql.engine.prepare.Fragment;
import org.apache.ignite.internal.sql.engine.prepare.FragmentPlan;
import org.apache.ignite.internal.sql.engine.prepare.IgniteRelShuttle;
import org.apache.ignite.internal.sql.engine.prepare.MappingQueryContext;
import org.apache.ignite.internal.sql.engine.prepare.MultiStepPlan;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.rel.SourceAwareIgniteRel;
import org.apache.ignite.internal.sql.engine.schema.InternalIgniteTable;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.HashFunctionFactoryImpl;
import org.apache.ignite.internal.sql.engine.util.LocalTxAttributesHolder;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/**
 * ExecutionServiceImpl. TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class ExecutionServiceImpl<RowT> implements ExecutionService, TopologyEventHandler {
    private static final int CACHE_SIZE = 1024;

    private final ConcurrentMap<String, IgniteRel> physNodesCache = Caffeine.newBuilder()
            .maximumSize(CACHE_SIZE)
            .<String, IgniteRel>build()
            .asMap();

    private static final IgniteLogger LOG = Loggers.forClass(ExecutionServiceImpl.class);

    private static final SqlQueryMessagesFactory FACTORY = new SqlQueryMessagesFactory();

    private final MessageService msgSrvc;

    private final TopologyService topSrvc;

    private final ClusterNode localNode;

    private final SqlSchemaManager sqlSchemaManager;

    private final QueryTaskExecutor taskExecutor;

    private final MappingService mappingSrvc;

    private final ExchangeService exchangeSrvc;

    private final RowHandler<RowT> handler;

    private final DdlCommandHandler ddlCmdHnd;

    private final ImplementorFactory<RowT> implementorFactory;

    private final Map<UUID, DistributedQueryManager> queryManagerMap = new ConcurrentHashMap<>();

    /**
     * Creates the execution services.
     *
     * @param topSrvc Topology service.
     * @param msgSrvc Message service.
     * @param sqlSchemaManager Schema manager.
     * @param ddlCommandHandler Handler of the DDL commands.
     * @param taskExecutor Task executor.
     * @param handler Row handler.
     * @param mailboxRegistry Mailbox registry.
     * @param exchangeSrvc Exchange service.
     * @param <RowT> Type of the sql row.
     * @return An execution service.
     */
    public static <RowT> ExecutionServiceImpl<RowT> create(
            TopologyService topSrvc,
            MessageService msgSrvc,
            SqlSchemaManager sqlSchemaManager,
            DdlCommandHandler ddlCommandHandler,
            QueryTaskExecutor taskExecutor,
            RowHandler<RowT> handler,
            MailboxRegistry mailboxRegistry,
            ExchangeService exchangeSrvc
    ) {
        return new ExecutionServiceImpl<>(
                msgSrvc,
                topSrvc,
                new MappingServiceImpl(topSrvc),
                sqlSchemaManager,
                ddlCommandHandler,
                taskExecutor,
                handler,
                exchangeSrvc,
                ctx -> new LogicalRelImplementor<>(
                        ctx,
                        new HashFunctionFactoryImpl<>(sqlSchemaManager, handler),
                        mailboxRegistry,
                        exchangeSrvc
                )
        );
    }

    /**
     * Constructor.
     *
     * @param msgSrvc Message service.
     * @param topSrvc Topology service.
     * @param mappingSrvc Nodes mapping calculation service.
     * @param sqlSchemaManager Schema manager.
     * @param ddlCmdHnd Handler of the DDL commands.
     * @param taskExecutor Task executor.
     * @param handler Row handler.
     * @param exchangeSrvc Exchange service.
     * @param implementorFactory Relational node implementor factory.
     */
    public ExecutionServiceImpl(
            MessageService msgSrvc,
            TopologyService topSrvc,
            MappingService mappingSrvc,
            SqlSchemaManager sqlSchemaManager,
            DdlCommandHandler ddlCmdHnd,
            QueryTaskExecutor taskExecutor,
            RowHandler<RowT> handler,
            ExchangeService exchangeSrvc,
            ImplementorFactory<RowT> implementorFactory
    ) {
        this.localNode = topSrvc.localMember();
        this.handler = handler;
        this.msgSrvc = msgSrvc;
        this.mappingSrvc = mappingSrvc;
        this.topSrvc = topSrvc;
        this.sqlSchemaManager = sqlSchemaManager;
        this.taskExecutor = taskExecutor;
        this.exchangeSrvc = exchangeSrvc;
        this.ddlCmdHnd = ddlCmdHnd;
        this.implementorFactory = implementorFactory;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        msgSrvc.register((n, m) -> onMessage(n, (QueryStartRequest) m), SqlQueryMessageGroup.QUERY_START_REQUEST);
        msgSrvc.register((n, m) -> onMessage(n, (QueryStartResponse) m), SqlQueryMessageGroup.QUERY_START_RESPONSE);
        msgSrvc.register((n, m) -> onMessage(n, (QueryCloseMessage) m), SqlQueryMessageGroup.QUERY_CLOSE_MESSAGE);
        msgSrvc.register((n, m) -> onMessage(n, (ErrorMessage) m), SqlQueryMessageGroup.ERROR_MESSAGE);
    }

    private AsyncCursor<List<Object>> executeQuery(
            BaseQueryContext ctx,
            MultiStepPlan plan
    ) {
        DistributedQueryManager queryManager = new DistributedQueryManager(ctx, ctx.transaction());

        DistributedQueryManager old = queryManagerMap.put(ctx.queryId(), queryManager);

        assert old == null;

        ctx.cancel().add(() -> queryManager.close(true));

        return queryManager.execute(plan);
    }

    private BaseQueryContext createQueryContext(UUID queryId, @Nullable String schema, Object[] params, HybridTimestamp txTime, UUID txId) {
        return BaseQueryContext.builder()
                .queryId(queryId)
                .parameters(params)
                .frameworkConfig(
                        Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                                .defaultSchema(sqlSchemaManager.schema(schema))
                                .build()
                )
                .logger(LOG)
                .transaction(new LocalTxAttributesHolder(txId, txTime))
                .build();
    }

    private QueryPlan prepareFragment(String jsonFragment) {
        IgniteRel plan = physNodesCache.computeIfAbsent(jsonFragment, ser -> fromJson(sqlSchemaManager, ser));

        return new FragmentPlan(plan);
    }

    /** {@inheritDoc} */
    @Override
    public AsyncCursor<List<Object>> executePlan(
            QueryPlan plan, BaseQueryContext ctx
    ) {
        switch (plan.type()) {
            case DML:
                // TODO a barrier between previous operation and this one
            case QUERY:
                return executeQuery(
                        ctx,
                        (MultiStepPlan) plan
                );
            case EXPLAIN:
                return executeExplain((ExplainPlan) plan);
            case DDL:
                return executeDdl((DdlPlan) plan);

            default:
                throw new AssertionError("Unexpected plan type: " + plan);
        }
    }

    /** Cancels the query with given id. */
    public CompletableFuture<?> cancel(UUID qryId) {
        var mgr = queryManagerMap.get(qryId);

        if (mgr == null) {
            return CompletableFuture.completedFuture(null);
        }

        return mgr.close(true);
    }

    private AsyncCursor<List<Object>> executeDdl(DdlPlan plan) {
        CompletableFuture<Iterator<List<Object>>> ret = ddlCmdHnd.handle(plan.command())
                .thenApply(applied -> List.of(List.<Object>of(applied)).iterator())
                .exceptionally(th -> {
                    throw convertDdlException(th);
                });

        return new AsyncWrapper<>(ret, Runnable::run);
    }

    private static RuntimeException convertDdlException(Throwable e) {
        if (e instanceof CompletionException) {
            e = e.getCause();
        }

        if (e instanceof ConfigurationChangeException) {
            assert e.getCause() != null;
            // Cut off upper configuration error`s as uninformative.
            e = e.getCause();
        }

        if (e instanceof IgniteInternalCheckedException) {
            return new IgniteInternalException(DDL_EXEC_ERR, "Failed to execute DDL statement [stmt=" /*+ qry.sql()*/
                    + ", err=" + e.getMessage() + ']', e);
        }

        return (e instanceof RuntimeException) ? (RuntimeException) e : new SqlException(DDL_EXEC_ERR, e);
    }

    private AsyncCursor<List<Object>> executeExplain(ExplainPlan plan) {
        List<List<Object>> res = List.of(List.of(plan.plan()));

        return new AsyncWrapper<>(res.iterator());
    }

    private void onMessage(String nodeName, QueryStartRequest msg) {
        assert nodeName != null && msg != null;

        DistributedQueryManager queryManager = queryManagerMap.computeIfAbsent(msg.queryId(), key -> {
            BaseQueryContext ctx = createQueryContext(key, msg.schema(), msg.parameters(), msg.txTime(), msg.txId());

            return new DistributedQueryManager(ctx);
        });

        queryManager.submitFragment(nodeName, msg.root(), msg.fragmentDescription());
    }

    private void onMessage(String nodeName, QueryStartResponse msg) {
        assert nodeName != null && msg != null;

        DistributedQueryManager dqm = queryManagerMap.get(msg.queryId());

        if (dqm != null) {
            dqm.acknowledgeFragment(nodeName, msg.fragmentId(), msg.error());
        }
    }

    private void onMessage(String nodeName, ErrorMessage msg) {
        assert nodeName != null && msg != null;

        DistributedQueryManager dqm = queryManagerMap.get(msg.queryId());

        if (dqm != null) {
            RemoteException e = new RemoteException(nodeName, msg.queryId(), msg.fragmentId(), msg.error());

            dqm.onError(e);
        }
    }

    private void onMessage(String nodeName, QueryCloseMessage msg) {
        assert nodeName != null && msg != null;

        DistributedQueryManager dqm = queryManagerMap.get(msg.queryId());

        if (dqm != null) {
            dqm.close(true);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        CompletableFuture.allOf(queryManagerMap.values().stream()
                .filter(mgr -> mgr.rootFragmentId != null)
                .map(mgr -> mgr.close(true))
                .toArray(CompletableFuture[]::new)
        ).join();
    }

    /** {@inheritDoc} */
    @Override
    public void onAppeared(ClusterNode member) {
        // NO_OP
    }

    /** {@inheritDoc} */
    @Override
    public void onDisappeared(ClusterNode member) {
        queryManagerMap.values().forEach(qm -> qm.onNodeLeft(member.name()));
    }

    /** Returns local fragments for the query with given id. */
    public List<AbstractNode<?>> localFragments(UUID queryId) {
        DistributedQueryManager mgr = queryManagerMap.get(queryId);

        if (mgr == null) {
            return List.of();
        }

        return mgr.localFragments();
    }

    /**
     * A convenient class that manages the initialization and termination of distributed queries.
     */
    class DistributedQueryManager {
        private final BaseQueryContext ctx;

        private final CompletableFuture<Void> cancelFut = new CompletableFuture<>();

        private final AtomicBoolean cancelled = new AtomicBoolean();

        private final Map<RemoteFragmentKey, CompletableFuture<Void>> remoteFragmentInitCompletion = new ConcurrentHashMap<>();

        private final Queue<AbstractNode<RowT>> localFragments = new LinkedBlockingQueue<>();

        private final CompletableFuture<AsyncRootNode<RowT, List<Object>>> root;

        private volatile Long rootFragmentId = null;

        private @Nullable InternalTransaction tx;

        private DistributedQueryManager(
                BaseQueryContext ctx,
                @Nullable InternalTransaction tx
        ) {
            this(ctx);

            this.tx = tx;
        }

        private DistributedQueryManager(BaseQueryContext ctx) {
            this.ctx = ctx;

            var root = new CompletableFuture<AsyncRootNode<RowT, List<Object>>>();

            root.exceptionally(t -> {
                DistributedQueryManager.this.close(true);

                return null;
            });

            this.root = root;
        }

        private List<AbstractNode<?>> localFragments() {
            return List.copyOf(localFragments);
        }

        private void sendFragment(
                String targetNodeName, Fragment fragment, FragmentDescription desc
        ) throws IgniteInternalCheckedException {
            QueryStartRequest req = FACTORY.queryStartRequest()
                    .queryId(ctx.queryId())
                    .fragmentId(fragment.fragmentId())
                    .schema(ctx.schemaName())
                    .root(fragment.serialized())
                    .fragmentDescription(desc)
                    .parameters(ctx.parameters())
                    .txTime(ctx.transaction().readTimestamp())
                    .txId(ctx.transaction().id())
                    .build();

            var fut = new CompletableFuture<Void>();
            remoteFragmentInitCompletion.put(new RemoteFragmentKey(targetNodeName, fragment.fragmentId()), fut);

            try {
                msgSrvc.send(targetNodeName, req);
            } catch (Exception ex) {
                fut.complete(null);

                throw ex;
            }
        }

        private void acknowledgeFragment(String nodeName, long fragmentId, @Nullable Throwable ex) {
            if (ex != null) {
                Long rootFragmentId0 = rootFragmentId;

                if (rootFragmentId0 != null && fragmentId == rootFragmentId0) {
                    root.completeExceptionally(ex);
                } else {
                    root.thenAccept(root -> {
                        root.onError(ex);

                        close(true);
                    });
                }
            }

            remoteFragmentInitCompletion.get(new RemoteFragmentKey(nodeName, fragmentId)).complete(null);
        }

        private void onError(RemoteException ex) {
            root.thenAccept(root -> {
                root.onError(ex);

                close(true);
            });
        }

        private void onNodeLeft(String nodeName) {
            remoteFragmentInitCompletion.entrySet().stream()
                    .filter(e -> nodeName.equals(e.getKey().nodeName()))
                    .forEach(e -> e.getValue()
                            .completeExceptionally(new IgniteInternalException(
                                    NODE_LEFT_ERR, "Node left the cluster [nodeName=" + nodeName + "]")));
        }

        private void executeFragment(FragmentPlan plan, ExecutionContext<RowT> ectx) {
            String origNodeName = ectx.originatingNodeName();

            AbstractNode<RowT> node = implementorFactory.create(ectx).go(plan.root());

            localFragments.add(node);

            if (!(node instanceof Outbox)) {
                Function<RowT, RowT> internalTypeConverter = TypeUtils.resultTypeConverter(ectx, plan.root().getRowType());

                AsyncRootNode<RowT, List<Object>> rootNode = new AsyncRootNode<>(node, inRow -> {
                    inRow = internalTypeConverter.apply(inRow);

                    int rowSize = ectx.rowHandler().columnCount(inRow);

                    List<Object> res = new ArrayList<>(rowSize);

                    for (int i = 0; i < rowSize; i++) {
                        res.add(ectx.rowHandler().get(i, inRow));
                    }

                    return res;
                });
                node.onRegister(rootNode);

                rootNode.prefetch();

                root.complete(rootNode);
            }

            try {
                msgSrvc.send(
                        origNodeName,
                        FACTORY.queryStartResponse()
                                .queryId(ectx.queryId())
                                .fragmentId(ectx.fragmentId())
                                .build()
                );
            } catch (IgniteInternalCheckedException e) {
                throw new IgniteInternalException(MESSAGE_SEND_ERR, "Failed to send reply. [nodeName=" + origNodeName + ']', e);
            }

            if (node instanceof Outbox) {
                ((Outbox<?>) node).prefetch();
            }
        }

        private ExecutionContext<RowT> createContext(String initiatorNodeName, FragmentDescription desc) {
            return new ExecutionContext<>(
                    ctx,
                    taskExecutor,
                    ctx.queryId(),
                    localNode,
                    initiatorNodeName,
                    desc,
                    handler,
                    Commons.parametersMap(ctx.parameters())
            );
        }

        private void submitFragment(String initiatorNode, String fragmentString, FragmentDescription desc) {
            try {
                QueryPlan qryPlan = prepareFragment(fragmentString);

                FragmentPlan plan = (FragmentPlan) qryPlan;

                executeFragment(plan, createContext(initiatorNode, desc));
            } catch (Throwable ex) {
                LOG.debug("Unable to start query fragment", ex);

                try {
                    msgSrvc.send(
                            initiatorNode,
                            FACTORY.queryStartResponse()
                                    .queryId(ctx.queryId())
                                    .fragmentId(desc.fragmentId())
                                    .error(ex)
                                    .build()
                    );
                } catch (Exception e) {
                    LOG.info("Unable to send error message", e);

                    close(true);
                }
            }
        }

        private AsyncCursor<List<Object>> execute(MultiStepPlan plan) {
            taskExecutor.execute(() -> {
                try {
                    plan.init(mappingSrvc, new MappingQueryContext(localNode.name()));

                    List<Fragment> fragments = plan.fragments();

                    // we rely on the fact that the very first fragment is a root. Otherwise we need to handle
                    // the case when a non-root fragment will fail before the root is processed.
                    assert !nullOrEmpty(fragments) && fragments.get(0).rootFragment() : fragments;

                    // start remote execution
                    for (Fragment fragment : fragments) {
                        if (tx != null && !tx.isReadOnly()) {
                            enlistPartitions(fragment, tx);
                        }

                        if (fragment.rootFragment()) {
                            assert rootFragmentId == null;

                            rootFragmentId = fragment.fragmentId();
                        }

                        FragmentDescription fragmentDesc = new FragmentDescription(
                                fragment.fragmentId(),
                                plan.mapping(fragment),
                                plan.target(fragment),
                                plan.remotes(fragment)
                        );

                        for (String nodeName : fragmentDesc.nodeNames()) {
                            sendFragment(nodeName, fragment, fragmentDesc);
                        }
                    }
                } catch (Throwable t) {
                    if (!root.completeExceptionally(t)) {
                        root.thenAccept(root -> root.onError(t));
                    }
                }
            });

            return new AsyncCursor<>() {
                @Override
                public CompletableFuture<BatchedResult<List<Object>>> requestNextAsync(int rows) {
                    return root.thenCompose(cur -> {
                        var fut = cur.requestNextAsync(rows);

                        fut.thenAccept(batch -> {
                            if (!batch.hasMore()) {
                                DistributedQueryManager.this.close(false);
                            }
                        });

                        return fut;
                    });
                }

                @Override
                public CompletableFuture<Void> closeAsync() {
                    return root.thenCompose(none -> DistributedQueryManager.this.close(false));
                }
            };
        }

        private void enlistPartitions(Fragment fragment, InternalTransaction tx) {
            // TODO IGNITE-17952 Next condition should be removed.
            if (!TraitUtils.distributionEnabled(fragment.root())) {
                return;
            }

            new IgniteRelShuttle() {
                @Override
                public IgniteRel visit(IgniteIndexScan rel) {
                    enlist(rel);

                    return super.visit(rel);
                }

                @Override
                public IgniteRel visit(IgniteTableScan rel) {
                    enlist(rel);

                    return super.visit(rel);
                }

                private void enlist(SourceAwareIgniteRel rel) {
                    InternalIgniteTable tbl = rel.getTable().unwrap(InternalIgniteTable.class);
                    ColocationGroup grp = fragment.mapping().findGroup(rel.sourceId());

                    if (grp.assignments().isEmpty()) {
                        return;
                    }

                    int partsCnt = grp.assignments().size();

                    tx.assignCommitPartition(new TablePartitionId(tbl.id(), ThreadLocalRandom.current().nextInt(partsCnt)));

                    for (int p = 0; p < partsCnt; p++) {
                        List<NodeWithTerm> assign = grp.assignments().get(p);
                        NodeWithTerm leaderWithTerm = assign.get(0);

                        tx.enlist(new TablePartitionId(tbl.id(), p),
                                new IgniteBiTuple<>(topSrvc.getByConsistentId(leaderWithTerm.name()), leaderWithTerm.term()));
                    }
                }
            }.visit(fragment.root());
        }

        private CompletableFuture<Void> close(boolean cancel) {
            if (!cancelled.compareAndSet(false, true)) {
                return cancelFut.thenApply(Function.identity());
            }

            CompletableFuture<Void> start = closeExecNode(cancel);

            start
                    .thenCompose(tmp -> {
                        Map<String, List<CompletableFuture<?>>> requestsPerNode = new HashMap<>();
                        for (Map.Entry<RemoteFragmentKey, CompletableFuture<Void>> entry : remoteFragmentInitCompletion.entrySet()) {
                            requestsPerNode.computeIfAbsent(entry.getKey().nodeName(), key -> new ArrayList<>()).add(entry.getValue());
                        }

                        List<CompletableFuture<?>> cancelFuts = new ArrayList<>();
                        for (Map.Entry<String, List<CompletableFuture<?>>> entry : requestsPerNode.entrySet()) {
                            String nodeId = entry.getKey();

                            if (!exchangeSrvc.alive(nodeId)) {
                                continue;
                            }

                            cancelFuts.add(
                                    CompletableFuture.allOf(entry.getValue().toArray(new CompletableFuture[0]))
                                            .handle((none2, t) -> {
                                                try {
                                                    exchangeSrvc.closeQuery(nodeId, ctx.queryId());
                                                } catch (IgniteInternalCheckedException e) {
                                                    throw new IgniteInternalException(MESSAGE_SEND_ERR,
                                                            "Failed to send cancel message. [nodeId=" + nodeId + ']', e);
                                                }

                                                return null;
                                            })
                            );
                        }

                        if (cancel) {
                            ExecutionCancelledException ex = new ExecutionCancelledException();

                            for (AbstractNode<?> node : localFragments) {
                                node.context().execute(() -> node.onError(ex), node::onError);
                            }
                        }

                        var compoundCancelFut = CompletableFuture.allOf(cancelFuts.toArray(new CompletableFuture[0]));
                        var finalStepFut = compoundCancelFut.thenRun(() -> {
                            queryManagerMap.remove(ctx.queryId());

                            try {
                                ctx.cancel().cancel();
                            } catch (Exception ex) {
                                // NO-OP
                            }

                            cancelFut.complete(null);
                        });

                        return compoundCancelFut.thenCombine(finalStepFut, (none1, none2) -> null);
                    });

            start.completeAsync(() -> null, taskExecutor);

            return cancelFut.thenApply(Function.identity());
        }

        /**
         * Synchronously closes the tree's execution iterator.
         *
         * @param cancel Forces execution to terminate with {@link ExecutionCancelledException}.
         * @return Completable future that should run asynchronously.
         */
        private CompletableFuture<Void> closeExecNode(boolean cancel) {
            CompletableFuture<Void> fut = new CompletableFuture<>();

            if (!root.completeExceptionally(new ExecutionCancelledException()) && !root.isCompletedExceptionally()) {
                AsyncRootNode<RowT, List<Object>> node = root.getNow(null);

                if (!cancel) {
                    CompletableFuture<Void> closeFut = node.closeAsync();

                    return fut.thenCompose(v -> closeFut);
                }

                node.onError(new ExecutionCancelledException());
            }

            return fut;
        }
    }

    /**
     * A factory of the relational node implementors.
     *
     * @param <RowT> A type of the row the execution tree will be working with.
     * @see LogicalRelImplementor
     */
    @FunctionalInterface
    public interface ImplementorFactory<RowT> {
        /** Creates the relational node implementor with the given context. */
        LogicalRelImplementor<RowT> create(ExecutionContext<RowT> ctx);
    }
}
