package org.apache.ignite.internal.metastorage.server;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class WatcherImpl implements Watcher {
    private final BlockingQueue<Entry> queue = new LinkedBlockingQueue<>();

    private final List<Watch> watches = new ArrayList<>();

    private volatile boolean stop;

    private final Object mux = new Object();

    @Override public void register(@NotNull Watch watch) {
        synchronized (mux) {
            watches.add(watch);
        }
    }

    @Override public void notify(@NotNull Entry e) {
        queue.offer(e);
    }

    @Override
    public void cancel(@NotNull Watch watch) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    public void shutdown() {
        stop = true;
    }

    private class WatcherWorker implements Runnable {
        @Override public void run() {
            while (!stop) {
                try {
                    Entry e = queue.poll(100, TimeUnit.MILLISECONDS);

                    if (e != null) {
                        synchronized (mux) {
                            watches.forEach(w -> w.notify(e));
                        }
                    }
                }
                catch (InterruptedException interruptedException) {
                    // No-op.
                }
            }
        }
    }
}

