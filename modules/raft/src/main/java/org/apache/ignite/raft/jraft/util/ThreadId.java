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
package org.apache.ignite.raft.jraft.util;

import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * Replicator id with lock.
 */
public class ThreadId {

    private static final IgniteLogger LOG = Loggers.forClass(ThreadId.class);

    private static final int TRY_LOCK_TIMEOUT_MS = 10;

    private final Object data;
    private final ReentrantLock lock = new ReentrantLock();
    private final OnError onError;
    private volatile boolean destroyed;

    /**
     *
     */
    public interface OnError {

        /**
         * Error callback, it will be called in lock.
         *
         * @param id the thread id
         * @param data the data
         * @param errorCode the error code
         */
        void onError(final ThreadId id, final Object data, final int errorCode);
    }

    public ThreadId(final Object data, final OnError onError) {
        super();
        this.data = data;
        this.onError = onError;
        this.destroyed = false;
    }

    public boolean isDestroyed() {
        return this.destroyed;
    }

    // TODO asch why Object here ? IGNITE-14832
    public Object getData() {
        return this.data;
    }

    public Object lock() {
        if (this.destroyed) {
            return null;
        }
        this.lock.lock();
        // Got the lock, double checking state.
        if (this.destroyed) {
            // should release lock
            this.lock.unlock();
            return null;
        }
        return this.data;
    }

    public void unlock() {
        if (!this.lock.isHeldByCurrentThread()) {
            LOG.warn("Fail to unlock with {}, the lock is not held by current thread {}.", this.data,
                Thread.currentThread());
            return;
        }
        this.lock.unlock();
    }

    public void join() {
        while (!this.destroyed) {
            ThreadHelper.onSpinWait();
        }
    }

    @Override
    public String toString() {
        return this.data.toString();
    }

    public void unlockAndDestroy() {
        if (this.destroyed) {
            return;
        }
        this.destroyed = true;
        unlock();
    }

    /**
     * Set error code, run the onError callback
     * with code immediately in lock.
     *
     * @param errorCode error code
     */
    public void setError(final int errorCode) {
        if (this.destroyed) {
            LOG.warn("ThreadId: {} already destroyed, ignore error code: {}", this.data, errorCode);
            return;
        }
        this.lock.lock();
        try {
            if (this.destroyed) {
                LOG.warn("ThreadId: {} already destroyed, ignore error code: {}", this.data, errorCode);
                return;
            }
            if (this.onError != null) {
                this.onError.onError(this, this.data, errorCode);
            }

        } finally {
            // Maybe destroyed in callback
            if (!this.destroyed) {
                this.lock.unlock();
            }
        }
    }
}
