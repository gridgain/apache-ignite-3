package org.apache.ignite.internal.vault.impl;

import java.util.Iterator;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.vault.common.*;
import org.apache.ignite.internal.vault.service.VaultService;

/**
 * Simple in-memory representation of vault. Only for test purposes.
 */
public class VaultServiceImpl implements VaultService {
    /** Map to store values. */
    private TreeMap<String, Value> storage = new TreeMap<>();

    private final Object mux = new Object();


    /** {@inheritDoc} */
    @Override public CompletableFuture<Value> get(String key) {
        synchronized (mux) {
            return CompletableFuture.completedFuture(storage.get(key));
        }
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Long> appliedRevision(String key) {
        synchronized (mux) {
            return CompletableFuture.completedFuture(storage.get(key).getRevision());
        }
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> put(String key, Value val) {
        synchronized (mux) {
            if (storage.containsKey(key) && storage.get(key).getRevision() >= val.getRevision())
                return CompletableFuture.allOf();

            storage.put(key, val);

            return CompletableFuture.allOf();
        }
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> remove(String key) {
        synchronized (mux) {
            storage.remove(key);

            return CompletableFuture.allOf();
        }
    }

    /** {@inheritDoc} */
    @Override public Iterator<Value> range(String fromKey, String toKey) {
        synchronized (mux) {
            return storage.subMap(fromKey, toKey).values().iterator();
        }
    }
}
