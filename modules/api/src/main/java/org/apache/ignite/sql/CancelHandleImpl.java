package org.apache.ignite.sql;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

class CancelHandleImpl implements CancelHandle {
    private final CompletableFuture<Void> trigger = new CompletableFuture<>();
    private final List<CompletableFuture<Void>> cancellationResults = new CopyOnWriteArrayList<>();

    @Override
    public void cancel() {
        cancelAsync().join();
    }

    @Override
    public synchronized CompletableFuture<Void> cancelAsync() {
        trigger.complete(null);

        return CompletableFuture.allOf(cancellationResults.toArray(new CompletableFuture[0]));
    }

    @Override
    public synchronized void addListener(Supplier<CompletableFuture<Void>> onCancel) {
        if (trigger.isDone()) {
            onCancel.get();
        }

        cancellationResults.add(
                trigger.thenCompose(ignored -> onCancel.get())
        );
    }
}
