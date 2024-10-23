package org.apache.ignite.internal.sql.engine.exec.fsm;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryCancel;
import org.apache.ignite.internal.sql.engine.SqlOperationContext;
import org.apache.ignite.internal.sql.engine.exec.fsm.Result.Status;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.property.SqlProperties;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionContext;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Represents a query initiated on current node.
 * 
 * <p>Encapsulates intermediate state populated throughout query lifecycle.
 */
class Query implements Runnable {
    // Below are attributes the query was initialized with
    final UUID id;
    final String sql;
    final Object[] params;
    final QueryCancel cancel = new QueryCancel();
    final QueryExecutor executor;
    final SqlProperties properties;
    final QueryTransactionContext txContext;

    // Below is volatile state populated during processing of particular stage
    volatile ParsedResult parsedResult = null;
    volatile SqlOperationContext operationContext = null;
    volatile QueryPlan plan = null;
    volatile AsyncSqlCursor<InternalSqlRow> cursor = null;

    // Below are internal attributes
    private final ConcurrentMap<State, CompletableFuture<Void>> onStateEnterCallback = new ConcurrentHashMap<>();
    private volatile State currentState = State.REGISTERED;

    Query(
            QueryExecutor executor,
            UUID id,
            String sql,
            SqlProperties properties,
            QueryTransactionContext txContext,
            Object[] params
    ) {
        this.executor = executor;
        this.id = id;
        this.sql = sql;
        this.properties = properties;
        this.txContext = txContext;
        this.params = params;
    }

    @Override
    public void run() {
        Result result;
        do {
            State stateBefore = currentState;
            result = stateBefore.evaluate(this);

            if (IgniteUtils.assertionsEnabled()
                    && result.status() == Result.Status.PROCEED_IMMEDIATELY
                    && currentState == stateBefore) {
                throw new AssertionError("Attempt to immediately execute the same state. Did you forget to move query to next state?");
            }

            if (result.status() == Status.PROCEED_IMMEDIATELY) {
                continue;
            }

            if (result.status() == Status.SCHEDULE) {
                CompletableFuture<Void> awaitFuture = result.await();

                assert awaitFuture != null;

                if (awaitFuture.isDone() && !awaitFuture.isCompletedExceptionally()) {
                    // all required computations has been competed already, no need to reschedule
                    continue;
                }

                break;
            }
        } while (true);

        if (result.status() == Result.Status.SCHEDULE) {
            CompletableFuture<Void> awaitFuture = result.await();

            assert awaitFuture != null;

            awaitFuture
                    .whenComplete((ignored, ex) -> {
                        if (ex != null) {
                            onStateEnterCallback.values().forEach(f -> f.completeExceptionally(ex));
                        }
                    })
                    .thenRun(this);
        }
    }

    CompletableFuture<Void> onStateEnter(State state) {
        CompletableFuture<Void> result = onStateEnterCallback.computeIfAbsent(state, k -> new CompletableFuture<>());

        assert currentState == State.REGISTERED : "Listeners must be added during query registration [currentState=" + currentState + "]";

        return result;
    }

    /** Moves the query to a given state. */ 
    void moveTo(State newState) {
        assert currentState.ordinal() < newState.ordinal() : "oldState=" + currentState + ", newState=" + newState;

        currentState = newState;

        CompletableFuture<Void> callback = onStateEnterCallback.get(newState);

        if (callback != null) {
            callback.complete(null);
        }
    }
}
