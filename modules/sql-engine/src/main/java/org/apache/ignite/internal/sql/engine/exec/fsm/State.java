package org.apache.ignite.internal.sql.engine.exec.fsm;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;

import java.time.ZoneId;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursorImpl;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryProperty;
import org.apache.ignite.internal.sql.engine.SqlOperationContext;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor.PrefetchCallback;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.exec.AsyncDataCursor;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.property.SqlProperties;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionContext;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.sql.SqlException;

enum State {
    REGISTERED {
        @Override
        Result evaluate(Query query) {
            query.moveTo(State.PARSING);

            return Result.proceedImmediately();
        }
    },

    PARSING {
        @Override
        Result evaluate(Query query) {
            ParsedResult parsedResult = query.executor.lookupParsedResultInCache(query.sql);

            if (parsedResult != null) {
                query.parsedResult = parsedResult;

                query.moveTo(State.OPTIMIZING);

                return Result.proceedImmediately();
            }

            CompletableFuture<Void> awaitFuture = new CompletableFuture<>();
            query.executor.execute(() -> {
                try {
                    ParsedResult result = query.executor.parse(query.sql);

                    if (shouldBeCached(result.queryType())) {
                        query.executor.updateParsedResultCache(query.sql, result);
                    }

                    query.parsedResult = result;

                    query.moveTo(State.OPTIMIZING);

                    awaitFuture.complete(null);
                } catch (Throwable th) {
                    awaitFuture.completeExceptionally(th);
                }
            });

            return Result.proceedAfter(awaitFuture);
        }

        private boolean shouldBeCached(SqlQueryType queryType) {
            return queryType == SqlQueryType.QUERY || queryType == SqlQueryType.DML;
        }
    },

    OPTIMIZING {
        @Override
        Result evaluate(Query query) {
            ParsedResult result = query.parsedResult;

            assert result != null;

            validateParsedStatement(query.properties, result);
            validateDynamicParameters(result.dynamicParamsCount(), query.params, true);

            HybridTimestamp operationTime = query.executor.deriveOperationTime(query.txContext);

            String schemaName = query.properties.get(QueryProperty.DEFAULT_SCHEMA);
            ZoneId timeZoneId = query.properties.get(QueryProperty.TIME_ZONE_ID);

            SqlOperationContext operationContext = SqlOperationContext.builder()
                    .queryId(query.id)
                    .cancel(query.cancel)
                    .prefetchCallback(new PrefetchCallback())
                    .parameters(query.params)
                    .timeZoneId(timeZoneId)
                    .defaultSchemaName(schemaName)
                    .operationTime(operationTime)
                    .txContext(query.txContext)
                    .build();

            query.operationContext = operationContext;

//                ensureStatementMatchesTx(parsedResult.queryType(), txContext);

            CompletableFuture<Void> awaitFuture = query.executor.waitForMetadata(operationTime)
                    .thenCompose(none -> query.executor.prepare(query.parsedResult, operationContext)
                            .thenAccept(plan -> {
                                if (query.txContext.explicitTx() == null) {
                                    // in case of implicit tx we have to update observable time to prevent tx manager to start
                                    // implicit transaction too much in the past where version of catalog we used to prepare the
                                    // plan was not yet available
                                    query.txContext.updateObservableTime(query.executor.deriveMinimalRequiredTime(plan));
                                }

                                query.plan = plan;

                                query.moveTo(State.OPENING_CURSOR);
                            }));

            return Result.proceedAfter(awaitFuture);
        }

        /** Performs additional validation of a parsed statement. **/
        private void validateParsedStatement(
                SqlProperties properties,
                ParsedResult parsedResult
        ) {
            Set<SqlQueryType> allowedTypes = properties.get(QueryProperty.ALLOWED_QUERY_TYPES);
            SqlQueryType queryType = parsedResult.queryType();

            if (parsedResult.queryType() == SqlQueryType.TX_CONTROL) {
                String message = "Transaction control statement can not be executed as an independent statement";

                throw new SqlException(STMT_VALIDATION_ERR, message);
            }

            if (!allowedTypes.contains(queryType)) {
                String message = format("Invalid SQL statement type. Expected {} but got {}", allowedTypes, queryType);

                throw new SqlException(STMT_VALIDATION_ERR, message);
            }
        }

        private void validateDynamicParameters(int expectedParamsCount, Object[] params, boolean exactMatch) throws SqlException {
            if (exactMatch && expectedParamsCount != params.length || params.length > expectedParamsCount) {
                String message = format(
                        "Unexpected number of query parameters. Provided {} but there is only {} dynamic parameter(s).",
                        params.length, expectedParamsCount
                );

                throw new SqlException(STMT_VALIDATION_ERR, message);
            }

            for (Object param : params) {
                if (!TypeUtils.supportParamInstance(param)) {
                    String message = format(
                            "Unsupported dynamic parameter defined. Provided '{}' is not supported.", param.getClass().getName());

                    throw new SqlException(STMT_VALIDATION_ERR, message);
                }
            }
        }
    },

    OPENING_CURSOR {
        @Override
        Result evaluate(Query query) {
            QueryPlan plan = query.plan;
            SqlOperationContext context = query.operationContext;

            AsyncDataCursor<InternalSqlRow> dataCursor = query.executor.executePlan(context, plan);

            SqlQueryType queryType = plan.type();

            PrefetchCallback prefetchCallback = query.operationContext.prefetchCallback();

            assert prefetchCallback != null;

            AsyncSqlCursorImpl<InternalSqlRow> cursor = new AsyncSqlCursorImpl<>(
                    queryType,
                    plan.metadata(),
                    dataCursor,
                    null
            );

            query.cursor = cursor;

            QueryTransactionContext txContext = query.txContext;

            assert txContext != null;

            if (queryType == SqlQueryType.QUERY) {
                if (txContext.explicitTx() == null) {
                    // TODO: IGNITE-20322
                    // implicit transaction started by InternalTable doesn't update observableTimeTracker. At
                    // this point we don't know whether tx was started by InternalTable or ExecutionService, thus
                    // let's update tracker explicitly to preserve consistency
                    txContext.updateObservableTime(query.executor.clockNow());
                }

                query.moveTo(State.EXECUTING);

                // preserve lazy execution for statements that only reads
                return Result.proceedImmediately(); 
            }

            // for other types let's wait for the first page to make sure premature
            // close of the cursor won't cancel an entire operation
            CompletableFuture<Void> awaitFuture = cursor.onFirstPageReady()
                    .thenApply(none -> {
                        if (txContext.explicitTx() == null) {
                            // TODO: IGNITE-20322
                            // implicit transaction started by InternalTable doesn't update observableTimeTracker. At
                            // this point we don't know whether tx was started by InternalTable or ExecutionService, thus
                            // let's update tracker explicitly to preserve consistency
                            txContext.updateObservableTime(query.executor.clockNow());
                        }

                        query.moveTo(State.EXECUTING);

                        return null;
                    });

            return Result.proceedAfter(awaitFuture);
        }
    },

    EXECUTING {
        @Override
        Result evaluate(Query query) {
            return Result.stop();
        }
    },

    TERMINATING {
        @Override
        Result evaluate(Query query) {
            throw new UnsupportedOperationException("TERMINATING");
        }
    },

    TERMINATED {
        @Override
        Result evaluate(Query query) {
            return Result.stop();
        }
    };

    abstract Result evaluate(Query query);
}
