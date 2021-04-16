/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.vault.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.vault.common.Value;
import org.apache.ignite.internal.vault.common.Watch;
import org.apache.ignite.internal.vault.service.VaultService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

public class InMemoryVaultTest {

    private VaultService storage;

    @BeforeEach
    public void setUp() {
        storage = new VaultServiceImpl();
    }

    @Test
    public void put() throws ExecutionException, InterruptedException {
        String key = getKey(1);
        Value val = getValue(key, 1);

        assertNull(storage.get(key).get());

        storage.put(key, val);

        Value v = storage.get(key).get();

        assertFalse(v.empty());
        assertEquals(val, v);

        storage.put(key, val);

        v = storage.get(key).get();

        assertFalse(v.empty());
        assertEquals(val, v);
    }

    @Test
    public void remove() throws ExecutionException, InterruptedException {
        String key = getKey(1);
        Value val = getValue(key, 1);

        assertNull(storage.get(key).get());

        // Remove non-existent value.
        storage.remove(key);

        assertNull(storage.get(key).get());

        storage.put(key, val);

        Value v = storage.get(key).get();

        assertFalse(v.empty());
        assertEquals(val, v);

        // Remove existent value.
        storage.remove(key);

        v = storage.get(key).get();

        assertNull(v);
    }

    @Test
    public void range() throws ExecutionException, InterruptedException {
        String key;

        Map<String, Value> values = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            key = getKey(i);

            values.put(key, getValue(key, i));

            assertNull(storage.get(key).get());
        }

        values.forEach((k, v) -> storage.put(k, v));

        for (Map.Entry<String, Value> entry : values.entrySet())
            assertEquals(entry.getValue(), storage.get(entry.getKey()).get());

        Iterator<Value> it = storage.range(getKey(3), getKey(7));

        List<Value> rangeRes = new ArrayList<>();

        it.forEachRemaining(rangeRes::add);

        assertEquals(4, rangeRes.size());

        //Check that we have exact range from "key3" to "key6"
        for (int i = 3; i < 7; i++)
            assertEquals(values.get(getKey(i)), rangeRes.get(i - 3));
    }

    @Test
    public void watch() throws ExecutionException, InterruptedException {
        String key;

        Map<String, Value> values = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            key = getKey(i);

            values.put(key, getValue(key, i));
        }

        values.forEach((k, v) -> storage.put(k, v));

        for (Map.Entry<String, Value> entry : values.entrySet())
            assertEquals(entry.getValue(), storage.get(entry.getKey()).get());

        AtomicInteger counter = new AtomicInteger();

        Watch watch = new Watch(changedValue -> counter.incrementAndGet());

        watch.startKey(getKey(3));
        watch.endKey(getKey(7));

        storage.watch(watch);

        for (int i = 3; i < 7; i++)
            storage.put(getKey(i), new Value(getKey(i), ("new" + i).getBytes(), -1));

        Thread.sleep(500);

        assertEquals(4, counter.get());
    }

    private static String getKey(int k) {
        return ("key" + k);
    }

    private static Value getValue(String k, int v) {
        return new Value(k, ("key" + k + '_' + "val" + v).getBytes(), -1);
    }
}
