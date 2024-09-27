package org.apache.ignite.sql;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * A handle which may be used to request the cancellation of execution.
 */
public interface CancelHandle {
    /** A factory method to create a handle. */
    static CancelHandle create() {
        return new CancelHandleImpl();
    }

    /**
     * Abruptly terminates an execution of an associated process.
     *
     * <p>Control flow will return after the process has been terminated and the resources associated with that process have been freed.
     */
    void cancel();

    /**
     * Abruptly terminates an execution of a associated process.
     *
     * @return A future that will be completed after the process has been terminated and the resources associated with that process have
     *         been freed.
     */
    CompletableFuture<Void> cancelAsync();

    void addListener(Supplier<CompletableFuture<Void>> onCancel);
}
