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

package org.apache.ignite.internal.benchmark.trafgen;

import static org.apache.ignite.internal.benchmark.trafgen.DaoTrafGen.addrs;
import static org.apache.ignite.internal.benchmark.trafgen.DaoTrafGen.partitions;
import static org.apache.ignite.internal.benchmark.trafgen.DaoTrafGen.replicas;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.catalog.ColumnType;
import org.apache.ignite.catalog.definitions.ColumnDefinition;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.catalog.definitions.ZoneDefinition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;

public class Main
{
    private ObjectMapper objectMapper = objectMapper();
    private IgniteClient client;
    private KeyValueView<Tuple, Tuple> dataView;
    private KeyValueView<Tuple, Tuple> tagsView;

    private static ObjectMapper objectMapper()
    {
        var objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.INDENT_OUTPUT);
        objectMapper.disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE);
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.disable(MapperFeature.ALLOW_COERCION_OF_SCALARS);
        return objectMapper;
    }

    private Tuple createDataKey(String key)
    {
        var tuple = Tuple.create();
        tuple.set("KEY", key);
        return tuple;
    }

    private <C extends Collection<String>> Tuple createDataValue(String aKey, Map<String, C> tags, byte[] binaryPayload)
        throws JsonProcessingException
    {
        var tuple = Tuple.create();
        Map<String, String> lvExt = Map.of("ext", "extvalue" + aKey);
        long lvExpTime = System.currentTimeMillis() + 3600 * 24 * 1000;

        tuple.set("IVVERSIONID", 1);
        tuple.set("IVMODIFICATIONDATE", System.currentTimeMillis());
        tuple.set("IVVERSION", 1L);
        tuple.set("IVAPPVERSION", (short) 1);
        tuple.set("IVTAGS", objectMapper.writeValueAsString(tags).getBytes());
        tuple.set("IVEXTENDEDATTRIBUTES", objectMapper.writeValueAsString(lvExt).getBytes());
        tuple.set("IVEXPIRATIONDATE", lvExpTime);

        tuple.set("PARTSCATALOG", objectMapper.writeValueAsString(Map.of("binpart", "PART_0")).getBytes());
        tuple.set("PART_0", binaryPayload);
        return tuple;
    }

    private Tuple createTagsKey(String key, String tagName, String tagValue)
    {
        var tuple = Tuple.create();
        tuple.set("TAGNAME", tagName);
        tuple.set("TAGVALUE", tagValue);
        tuple.set("KEY", key);
        return tuple;
    }

    private Tuple createTagsValue()
    {
        var tuple = Tuple.create();
        tuple.set("DUMMY", (byte) 0);
        return tuple;
    }

    private <C extends Collection<String>> Map<Tuple, Tuple> buildTagMap(String aKey, Map<String, C> tags)
    {
        Map<Tuple, Tuple> map = new LinkedHashMap<>();
        for (var tag : tags.entrySet())
        {
            for (var value : tag.getValue())
            {
                map.put(createTagsKey(tag.getKey(), value, aKey), createTagsValue());
            }
        }
        return map;
    }

    public void createOrReplace(String aKey, Map<String, Set<String>> lvTags, byte[] binaryPayload)
        throws JsonProcessingException
    {
        var tx = client.transactions().begin();
        var oldValue = dataView.getAndPut(tx, createDataKey(aKey), createDataValue(aKey, lvTags, binaryPayload));
        if (oldValue == null)
        {
            tagsView.putAll(tx, buildTagMap(aKey, lvTags));
        } else
        {
            // If a strict version control was required, we would use oldvalue to check its version versus the
            // expected one and then
            // update the records with a new version number. But the test does not go into this path.
            // dataView.put(tx, createDataKey(aKey), createDataValue(lvTags));
            // We would have to compute the changes in the tags map and remove those that are not longer there.
            // But the test don't go into this path.
            // tagsView.removeAll(tx, ...);
            tagsView.putAll(tx, buildTagMap(aKey, lvTags));
        }
        tx.commit();
    }

    public byte[] get(String aKey) throws JsonProcessingException
    {
        var tuple = dataView.get(null, createDataKey(aKey));
        return tuple != null ? tuple.bytesValue("PART_0") : null;
    }

    public void delete(String aKey) throws IOException
    {
        var tx = client.transactions().begin();
        var oldValue = dataView.getAndRemove(tx, createDataKey(aKey));
        if (oldValue != null)
        {
            @SuppressWarnings("unchecked")
            Map<String, List<String>> tags = objectMapper.readValue(oldValue.bytesValue("IVTAGS"), Map.class);
            // If a strict version control was required, we would use oldvalue to check its version versus the
            // expected one and then
            // update the records with a new version number. But the test does not go into this path.
            tagsView.removeAll(tx, buildTagMap(aKey, tags).keySet());
        }
        tx.commit();
    }

    public void replace(String aKey, Map<String, Set<String>> lvTags, byte[] binaryPayload)
        throws JsonProcessingException
    {
        var tx = client.transactions().begin();
        var oldvalue = dataView.get(null, createDataKey(aKey));
        if (oldvalue != null)
        {
            dataView.put(tx, createDataKey(aKey), createDataValue(aKey, lvTags, binaryPayload));

            tagsView.putAll(tx, buildTagMap(aKey, lvTags));
        }
        tx.commit();
    }

    public void intitialize(String realm, String storage)
    {
        var builder = IgniteClient.builder();
        client = builder.addresses(addrs).build();

        var catalog = client.catalog();

    // @formatter:off
    catalog.createZone(
        ZoneDefinition.builder("SDE_DEFAULT_"+partitions+"_" + replicas)
             .partitions(partitions)
             .replicas(replicas)
             .storageProfiles("default")
             .ifNotExists()
             .build()
    );
    var dataTable = catalog.createTable(
        TableDefinition.builder("DATA_"+realm.toUpperCase()+"_"+storage.toUpperCase())
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
                ColumnDefinition.column("PART_1",ColumnType.VARBINARY),
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
            .index("IVEXPIRATIONDATE")
            .ifNotExists()
            .build()
    );
    var tagsTable = catalog.createTable(
        TableDefinition.builder("TAGS_"+realm.toUpperCase()+"_"+storage.toUpperCase())
            .columns(
                ColumnDefinition.column("TAGNAME", ColumnType.VARCHAR),
                ColumnDefinition.column("TAGVALUE", ColumnType.VARCHAR),
                ColumnDefinition.column("KEY", ColumnType.VARCHAR),
                ColumnDefinition.column("DUMMY", ColumnType.INT8)
           )
            .primaryKey("TAGNAME","TAGVALUE","KEY")
            .ifNotExists()
            .build()
    );
    // @formatter:on

        dataView = dataTable.keyValueView();
        tagsView = tagsTable.keyValueView();
    }
}
