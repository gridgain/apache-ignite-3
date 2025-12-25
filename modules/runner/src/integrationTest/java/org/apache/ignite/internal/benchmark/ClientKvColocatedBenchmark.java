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

package org.apache.ignite.internal.benchmark;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.ignite.catalog.ColumnType;
import org.apache.ignite.catalog.IgniteCatalog;
import org.apache.ignite.catalog.definitions.ColumnDefinition;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.client.table.ClientTable;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.runner.RunnerException;

/**
 * Colocated explicit tx benchmark.
 */
@Fork(0)
public class ClientKvColocatedBenchmark extends ClientKvBenchmark {
    private static final int DEF_PAYLOAD_SIZE = 1024;

    private static final String DATA_TABLE = "TABLE_DATA";
    private static final String TAG_TABLE = "TABLE_TAG";

    private KeyValueView<Tuple, Tuple> dataView;
    private KeyValueView<Tuple, Tuple> tagsView;

    private static final byte[] binaryPayload = buildPayload(DEF_PAYLOAD_SIZE);
    private static final String TAG1 = "index1";
    private static final String TAG2 = "index2";

    private static final String TAG1_VALUE_PREFIX = TAG1 + "_" + "v".repeat(25) + "_";
    private static final String TAG2_VALUE_PREFIX = TAG2 + "_" + "v".repeat(25) + "_";

    @Override
    protected void createTablesOnStartup() {
        IgniteCatalog catalog = publicIgnite.catalog();

        catalog.createTable(
                TableDefinition.builder(DATA_TABLE)
                        .columns(
                                ColumnDefinition.column("KEY", ColumnType.VARCHAR),
                                ColumnDefinition.column("IVVERSIONID", ColumnType.INT32),
                                ColumnDefinition.column("IVMODIFICATIONDATE", ColumnType.INT64),
                                ColumnDefinition.column("IVVERSION", ColumnType.INT64),
                                ColumnDefinition.column("IVAPPVERSION", ColumnType.INT16),
                                ColumnDefinition.column("IVTAGS", ColumnType.VARBINARY),
                                ColumnDefinition.column("IVEXTENDEDATTRIBUTES", ColumnType.VARBINARY),
                                ColumnDefinition.column("IVEXPIRATIONDATE", ColumnType.INT64),
                                ColumnDefinition.column("PARTSCATALOG", ColumnType.VARBINARY),
                                ColumnDefinition.column("PART_0", ColumnType.VARBINARY),
                                ColumnDefinition.column("PART_1", ColumnType.VARBINARY),
                                ColumnDefinition.column("PART_2", ColumnType.VARBINARY),
                                ColumnDefinition.column("PART_3", ColumnType.VARBINARY),
                                ColumnDefinition.column("PART_4", ColumnType.VARBINARY),
                                ColumnDefinition.column("PART_5", ColumnType.VARBINARY),
                                ColumnDefinition.column("PART_6", ColumnType.VARBINARY),
                                ColumnDefinition.column("PART_7", ColumnType.VARBINARY),
                                ColumnDefinition.column("PART_8", ColumnType.VARBINARY),
                                ColumnDefinition.column("PART_9", ColumnType.VARBINARY)
                        )
                        .primaryKey("KEY")
                        .colocateBy("KEY")
                        .index("IVEXPIRATIONDATE")
                        .ifNotExists()
                        .zone(ZONE_NAME)
                        .build()
        );

        catalog.createTable(
                TableDefinition.builder(TAG_TABLE)
                        .columns(
                                ColumnDefinition.column("TAGNAME", ColumnType.VARCHAR),
                                ColumnDefinition.column("TAGVALUE", ColumnType.VARCHAR),
                                ColumnDefinition.column("KEY", ColumnType.VARCHAR),
                                ColumnDefinition.column("DUMMY", ColumnType.INT8)
                        )
                        .primaryKey("TAGNAME", "TAGVALUE", "KEY")
                        .colocateBy("KEY")
                        .ifNotExists()
                        .zone(ZONE_NAME)
                        .build()
        );
    }

    @Override
    public void setUp() {
        client = IgniteClient.builder().addresses(addresses()).build();
        ClientTable dataTable = (ClientTable) client.tables().table(DATA_TABLE);
        ClientTable tagsTable = (ClientTable) client.tables().table(TAG_TABLE);

        dataView = dataTable.keyValueView();
        tagsView = tagsTable.keyValueView();
    }

    private static String keyPrefixOfThread(long threadIndex) {
        return "key-" + threadIndex + "-";
    }

    public void createOrReplace(String aKey)
    {
        Map<String, Set<String>> lvTags = new HashMap<>();
        lvTags.put(TAG1, Set.of(TAG1_VALUE_PREFIX + aKey));

        var tx = client.transactions().begin();
        var oldValue = dataView.getAndPut(tx, createDataKey(aKey), createDataValue(aKey, lvTags, binaryPayload));
        if (oldValue == null) {
            tagsView.putAll(tx, buildTagMap(aKey, lvTags));
        } else {
            tagsView.putAll(tx, buildTagMap(aKey, lvTags));
        }
        tx.commit();
    }

    public byte[] get(String aKey)
    {
        var tuple = dataView.get(null, createDataKey(aKey));
        return tuple != null ? tuple.bytesValue("PART_0") : null;
    }

    public void delete(String aKey)
    {
        var tx = client.transactions().begin();
        var oldValue = dataView.getAndRemove(tx, createDataKey(aKey));
        if (oldValue != null)
        {
            byte[] bytes = oldValue.bytesValue("IVTAGS");
            Map<String, Collection<String>> tags = readTags(bytes);
            tagsView.removeAll(tx, buildTagMap(aKey, tags).keySet());
        }
        tx.commit();
    }

    private void replace(String aKey) {
        Map<String, Set<String>> lvTags = new HashMap<>();
        lvTags.put(TAG2, Set.of(TAG2_VALUE_PREFIX + aKey));

        var tx = client.transactions().begin();
        Tuple dataKey = createDataKey(aKey);
        Tuple dataValue = createDataValue(aKey, lvTags, binaryPayload);
        if (dataView.replace(tx, dataKey, dataValue)) {
            tagsView.putAll(tx, buildTagMap(aKey, lvTags));
        }
        tx.commit();
    }

    @Benchmark
    public void flow1() {
        int opId = nextId();

        String aKey = keyPrefixOfThread(Thread.currentThread().getId()) + opId;

        createOrReplace(aKey);
        get(aKey);
        replace(aKey);
        get(aKey);
        delete(aKey);
    }


    /**
     * Benchmark's entry point. Can be started from command line: ./gradlew ":ignite-runner:ClientKvColocatedBenchmark" --args='jmh.batch=10
     * jmh.threads=1'
     */
    public static void main(String[] args) throws RunnerException {
        runBenchmark(ClientKvColocatedBenchmark.class, args);
    }

    private static Tuple createDataKey(String key) {
        var tuple = Tuple.create();
        tuple.set("KEY", key);
        return tuple;
    }

    private static Tuple createDataValue(String aKey, Map<String, ? extends Collection<String>> tags, byte[] binaryPayload) {
        var tuple = Tuple.create();
        Map<String, String> lvExt = Map.of("ext", "extvalue" + aKey);
        long lvExpTime = System.currentTimeMillis() + 3600 * 24 * 1000;

        tuple.set("IVVERSIONID", 1);
        tuple.set("IVMODIFICATIONDATE", System.currentTimeMillis());
        tuple.set("IVVERSION", 1L);
        tuple.set("IVAPPVERSION", (short) 1);

        tuple.set("IVTAGS", writeTags(tags));
        tuple.set("IVEXTENDEDATTRIBUTES", writeExt(lvExt));
        tuple.set("IVEXPIRATIONDATE", lvExpTime);

        tuple.set("PARTSCATALOG", writeExt(Map.of("binpart", "PART_0")));
        tuple.set("PART_0", binaryPayload);
        return tuple;
    }

    private static byte[] writeTags(Map<String, ? extends Collection<String>> tags) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            dos.write(tags.size());
            for (Entry<String, ? extends Collection<String>> stringEntry : tags.entrySet()) {
                dos.writeUTF(stringEntry.getKey());
                Collection<String> value = stringEntry.getValue();
                dos.write(value.size());
                for (String s : value) {
                    dos.writeUTF(s);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return baos.toByteArray();
    }

    private static Map<String, Collection<String>> readTags(byte[] data) {
        Map<String, Collection<String>> res = new HashMap<>();

        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        try (DataInputStream dis = new DataInputStream(bais)) {
            int tagsSize = dis.read();
            while(tagsSize-- > 0) {
                String key = dis.readUTF();
                ArrayList<String> vals = new ArrayList<>();
                res.put(key, vals);

                int valSize = dis.read();
                while(valSize-- > 0) {
                    vals.add(dis.readUTF());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return res;
    }

    private static byte[] writeExt(Map<String, String> tags) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            dos.write(tags.size());
            for (Entry<String, String> stringCEntry : tags.entrySet()) {
                dos.writeUTF(stringCEntry.getKey());
                dos.writeUTF(stringCEntry.getValue());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return baos.toByteArray();
    }

    private static Tuple createTagsKey(String key, String tagName, String tagValue) {
        var tuple = Tuple.create();
        tuple.set("TAGNAME", tagName);
        tuple.set("TAGVALUE", tagValue);
        tuple.set("KEY", key);
        return tuple;
    }

    private static Tuple createTagsValue() {
        var tuple = Tuple.create();
        tuple.set("DUMMY", (byte) 0);
        return tuple;
    }

    private static Map<Tuple, Tuple> buildTagMap(String aKey, Map<String, ? extends Collection<String>> tags) {
        Map<Tuple, Tuple> map = new LinkedHashMap<>();
        for (var tag : tags.entrySet()) {
            for (var value : tag.getValue()) {
                map.put(createTagsKey(tag.getKey(), value, aKey), createTagsValue());
            }
        }
        return map;
    }

    private static byte[] buildPayload(int size) {
        final byte[] payload = new byte[size];
        for (int ii = 0; ii < size; ii++) {
            payload[ii] = (byte) ii;
        }
        return payload;
    }

    @Override
    protected int nodes() {
        return 1;
    }
}
