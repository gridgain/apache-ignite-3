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

package org.apache.ignite.internal.sql.engine.session;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.sql.engine.AsyncCloseable;
import org.apache.ignite.internal.sql.engine.property.PropertiesHolder;

/**
 * A session object.
 *
 * <p>This is a server-side sql session which keeps track of associated resources like opened query cursor, or keeps a properties holder
 * with all properties set during session's creation.
 */
public class Session implements AsyncCloseable {
    private final Set<AsyncCloseable> resources = Collections.synchronizedSet(
            Collections.newSetFromMap(new IdentityHashMap<>())
    );

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final AtomicReference<CompletableFuture<Void>> closeFutRef = new AtomicReference<>();

    private final SessionId sessionId;
    private final PropertiesHolder properties;
    private final Runnable onClose;

    /**
     * Constructor.
     *
     * @param sessionId A session identifier.
     * @param properties The properties to keep within.
     */
    public Session(
            SessionId sessionId,
            PropertiesHolder properties,
            Runnable onClose
    ) {
        this.sessionId = sessionId;
        this.properties = properties;
        this.onClose = onClose;
    }

    /** Returns the properties this session associated with. */
    public PropertiesHolder properties() {
        return properties;
    }

    /** Returns the identifier of this session. */
    public SessionId sessionId() {
        return sessionId;
    }

    /**
     * Registers a resource within current session to release in case this session will be closed.
     *
     * <p>This method will throw an {@link IllegalStateException} if someone try to register resource to an
     * already closed/expired session.
     *
     * @param resource Resource to be registered within session.
     */
    public void registerResource(AsyncCloseable resource) {
        if (!lock.readLock().tryLock()) {
            throw new IllegalStateException(format("Attempt to register resource to an expired session [{}]", sessionId));
        }

        try {
            resources.add(resource);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Unregisters the given resource from session.
     *
     * @param resource Resource to unregister.
     */
    public void unregisterResource(AsyncCloseable resource) {
        if (!lock.readLock().tryLock()) {
            return;
        }

        try {
            resources.remove(resource);
        } finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> closeAsync() {
        if (closeFutRef.compareAndSet(null, new CompletableFuture<>())) {
            lock.writeLock().lock();

            onClose.run();

            var futs = new CompletableFuture[resources.size()];

            var idx = 0;
            for (var resource : resources) {
                futs[idx++] = resource.closeAsync();
            }

            resources.clear();

            CompletableFuture.allOf(futs).thenRun(() -> closeFutRef.get().complete(null));
        }

        return closeFutRef.get().thenRun(() -> {});
    }
}
