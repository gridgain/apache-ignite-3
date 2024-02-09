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

package org.apache.ignite.internal.client.table;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.lang.NullableValue;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Client cache implementation.
 * TODO reduce copy&paste comparing to EmbeddedCache.
 */
public class ClientCache<K, V> implements KeyValueView<K, V> {
    @Override
    public CompletableFuture<Void> streamData(Publisher<Entry<K, V>> publisher, @Nullable DataStreamerOptions options) {
        return null;
    }

    @Override
    public @Nullable V get(@Nullable Transaction tx, K key) {
        return null;
    }

    @Override
    public CompletableFuture<V> getAsync(@Nullable Transaction tx, K key) {
        return null;
    }

    @Override
    public NullableValue<V> getNullable(@Nullable Transaction tx, K key) {
        return null;
    }

    @Override
    public CompletableFuture<NullableValue<V>> getNullableAsync(@Nullable Transaction tx, K key) {
        return null;
    }

    @Override
    public @Nullable V getOrDefault(@Nullable Transaction tx, K key, @Nullable V defaultValue) {
        return null;
    }

    @Override
    public CompletableFuture<V> getOrDefaultAsync(@Nullable Transaction tx, K key, @Nullable V defaultValue) {
        return null;
    }

    @Override
    public Map<K, V> getAll(@Nullable Transaction tx, Collection<K> keys) {
        return null;
    }

    @Override
    public CompletableFuture<Map<K, V>> getAllAsync(@Nullable Transaction tx, Collection<K> keys) {
        return null;
    }

    @Override
    public boolean contains(@Nullable Transaction tx, K key) {
        return false;
    }

    @Override
    public CompletableFuture<Boolean> containsAsync(@Nullable Transaction tx, K key) {
        return null;
    }

    @Override
    public void put(@Nullable Transaction tx, K key, @Nullable V val) {

    }

    @Override
    public CompletableFuture<Void> putAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return null;
    }

    @Override
    public void putAll(@Nullable Transaction tx, Map<K, V> pairs) {

    }

    @Override
    public CompletableFuture<Void> putAllAsync(@Nullable Transaction tx, Map<K, V> pairs) {
        return null;
    }

    @Override
    public @Nullable V getAndPut(@Nullable Transaction tx, K key, @Nullable V val) {
        return null;
    }

    @Override
    public CompletableFuture<V> getAndPutAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return null;
    }

    @Override
    public NullableValue<V> getNullableAndPut(@Nullable Transaction tx, K key, @Nullable V val) {
        return null;
    }

    @Override
    public CompletableFuture<NullableValue<V>> getNullableAndPutAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return null;
    }

    @Override
    public boolean putIfAbsent(@Nullable Transaction tx, K key, @Nullable V val) {
        return false;
    }

    @Override
    public CompletableFuture<Boolean> putIfAbsentAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return null;
    }

    @Override
    public boolean remove(@Nullable Transaction tx, K key) {
        return false;
    }

    @Override
    public boolean remove(@Nullable Transaction tx, K key, V val) {
        return false;
    }

    @Override
    public CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, K key) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, K key, V val) {
        return null;
    }

    @Override
    public Collection<K> removeAll(@Nullable Transaction tx, Collection<K> keys) {
        return null;
    }

    @Override
    public CompletableFuture<Collection<K>> removeAllAsync(@Nullable Transaction tx, Collection<K> keys) {
        return null;
    }

    @Override
    public @Nullable V getAndRemove(@Nullable Transaction tx, K key) {
        return null;
    }

    @Override
    public CompletableFuture<V> getAndRemoveAsync(@Nullable Transaction tx, K key) {
        return null;
    }

    @Override
    public NullableValue<V> getNullableAndRemove(@Nullable Transaction tx, K key) {
        return null;
    }

    @Override
    public CompletableFuture<NullableValue<V>> getNullableAndRemoveAsync(@Nullable Transaction tx, K key) {
        return null;
    }

    @Override
    public boolean replace(@Nullable Transaction tx, K key, @Nullable V val) {
        return false;
    }

    @Override
    public boolean replace(@Nullable Transaction tx, K key, V oldValue, @Nullable V newValue) {
        return false;
    }

    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, K key, @Nullable V oldVal, @Nullable V newVal) {
        return null;
    }

    @Override
    public @Nullable V getAndReplace(@Nullable Transaction tx, @Nullable K key, @Nullable V val) {
        return null;
    }

    @Override
    public CompletableFuture<V> getAndReplaceAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return null;
    }

    @Override
    public NullableValue<V> getNullableAndReplace(@Nullable Transaction tx, K key, @Nullable V val) {
        return null;
    }

    @Override
    public CompletableFuture<NullableValue<V>> getNullableAndReplaceAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return null;
    }
}
