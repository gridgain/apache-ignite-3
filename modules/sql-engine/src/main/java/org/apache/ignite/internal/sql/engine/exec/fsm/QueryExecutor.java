package org.apache.ignite.internal.sql.engine.exec.fsm;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.SqlOperationContext;
import org.apache.ignite.internal.sql.engine.exec.AsyncDataCursor;
import org.apache.ignite.internal.sql.engine.exec.ExecutionService;
import org.apache.ignite.internal.sql.engine.prepare.KeyValueGetPlan;
import org.apache.ignite.internal.sql.engine.prepare.KeyValueModifyPlan;
import org.apache.ignite.internal.sql.engine.prepare.MultiStepPlan;
import org.apache.ignite.internal.sql.engine.prepare.PrepareService;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.property.SqlProperties;
import org.apache.ignite.internal.sql.engine.property.SqlPropertiesHelper;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.sql.engine.sql.ParserService;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionContext;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionWrapper;
import org.apache.ignite.internal.sql.engine.util.cache.Cache;
import org.apache.ignite.internal.sql.engine.util.cache.CacheFactory;
import org.jetbrains.annotations.Nullable;

public class QueryExecutor {
    private final Cache<String, ParsedResult> queryToParsedResultCache;
    private final ParserService parserService;
    private final Executor executor;
    private final ClockService clockService;
    private final SchemaSyncService schemaSyncService;
    private final PrepareService prepareService;
    private final CatalogService catalogService;
    private final ExecutionService executionService;
    private final SqlProperties defaultProperties;

    public QueryExecutor(
            CacheFactory cacheFactory,
            int parsedResultsCacheSize,
            ParserService parserService,
            Executor executor,
            ClockService clockService,
            SchemaSyncService schemaSyncService,
            PrepareService prepareService,
            CatalogService catalogService,
            ExecutionService executionService,
            SqlProperties defaultProperties
    ) {
        this.queryToParsedResultCache = cacheFactory.create(parsedResultsCacheSize);
        this.parserService = parserService;
        this.executor = executor;
        this.clockService = clockService;
        this.schemaSyncService = schemaSyncService;
        this.prepareService = prepareService;
        this.catalogService = catalogService;
        this.executionService = executionService;
        this.defaultProperties = defaultProperties;
    }

    public CompletableFuture<AsyncSqlCursor<InternalSqlRow>> executeQuery(
            SqlProperties properties,
            QueryTransactionContext txContext,
            String sql,
            Object... params
    ) {
        SqlProperties properties0 = SqlPropertiesHelper.chain(properties, defaultProperties);

        Query query = new Query(
                this,
                UUID.randomUUID(),
                sql,
                properties0,
                txContext,
                params
        );

        // next state after OPENING_CURSOR. Cursor must be ready at this point.
        CompletableFuture<Void> trigger = query.onStateEnter(State.EXECUTING);

        query.run();

        return trigger
                .thenApply(ignored -> query.cursor);
    }

    @Nullable ParsedResult lookupParsedResultInCache(String sql) {
        return queryToParsedResultCache.get(sql);
    }

    void updateParsedResultCache(String sql, ParsedResult result) {
        queryToParsedResultCache.put(sql, result);
    }

    ParsedResult parse(String sql) {
        return parserService.parse(sql);
    }

    void execute(Runnable runnable) {
        executor.execute(runnable);
    }

    HybridTimestamp deriveOperationTime(QueryTransactionContext txContext) {
        QueryTransactionWrapper txWrapper = txContext.explicitTx();

        if (txWrapper == null) {
            return clockService.now();
        }

        return txWrapper.unwrap().startTimestamp();
    }

    CompletableFuture<Void> waitForMetadata(HybridTimestamp timestamp) {
        return schemaSyncService.waitForMetadataCompleteness(timestamp);
    }

    CompletableFuture<QueryPlan> prepare(ParsedResult result, SqlOperationContext operationContext) {
        return prepareService.prepareAsync(result, operationContext);
    }

    HybridTimestamp deriveMinimalRequiredTime(QueryPlan plan) {
        Integer catalogVersion = null;

        if (plan instanceof MultiStepPlan) {
            catalogVersion = ((MultiStepPlan) plan).catalogVersion();
        } else if (plan instanceof KeyValueModifyPlan) {
            catalogVersion = ((KeyValueModifyPlan) plan).catalogVersion();
        } else if (plan instanceof KeyValueGetPlan) {
            catalogVersion = ((KeyValueGetPlan) plan).catalogVersion();
        }

        if (catalogVersion != null) {
            Catalog catalog = catalogService.catalog(catalogVersion);

            assert catalog != null;

            return HybridTimestamp.hybridTimestamp(catalog.time());
        }

        return clockService.now();
    }

    AsyncDataCursor<InternalSqlRow> executePlan(
            SqlOperationContext ctx,
            QueryPlan plan
    ) {
        return executionService.executePlan(plan, ctx);
    }

    HybridTimestamp clockNow() {
        return clockService.now();
    }
}
