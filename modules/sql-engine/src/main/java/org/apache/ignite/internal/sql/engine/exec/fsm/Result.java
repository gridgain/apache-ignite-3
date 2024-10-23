package org.apache.ignite.internal.sql.engine.exec.fsm;

import java.util.concurrent.CompletableFuture;
import org.jetbrains.annotations.Nullable;

/**
 * Result of evaluation of particular {@link State}. 
 */
class Result {
    enum Status {
        PROCEED_IMMEDIATELY, SCHEDULE, STOP
    }

    private final Status status;

    private final @Nullable CompletableFuture<Void> await;

    static Result proceedImmediately() {
        return new Result(Status.PROCEED_IMMEDIATELY, null);
    }

    static Result proceedAfter(CompletableFuture<Void> stage) {
        assert stage != null;

        return new Result(Status.SCHEDULE, stage);
    }

    static Result stop() {
        return new Result(Status.STOP, null);
    }

    private Result(Status status, @Nullable CompletableFuture<Void> await) {
        this.status = status;
        this.await = await;
    }

    Status status() {
        return status;
    }

    @Nullable CompletableFuture<Void> await() {
        return await;
    }
}
