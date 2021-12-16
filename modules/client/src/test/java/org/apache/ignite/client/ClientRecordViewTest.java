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

package org.apache.ignite.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.Test;

/**
 * Record view tests.
 */
@SuppressWarnings("ZeroLengthArrayAllocation")
public class ClientRecordViewTest extends AbstractClientTableTest {
    @Test
    public void testBinaryPutPojoGet() {
        Table table = defaultTable();
        RecordView<PersonPojo> pojoView = table.recordView(Mapper.of(PersonPojo.class));

        table.recordView().upsert(tuple(), null);

        var key = new PersonPojo();
        key.id = DEFAULT_ID;

        PersonPojo val = pojoView.get(key, null);
        PersonPojo missingVal = pojoView.get(new PersonPojo(), null);

        assertEquals(DEFAULT_NAME, val.name);
        assertEquals(DEFAULT_ID, val.id);
        assertNull(missingVal);
    }

    @Test
    public void testBinaryPutPrimitiveGet() {
        Table table = defaultTable();
        RecordView<Long> primitiveView = table.recordView(Mapper.of(Long.class));

        table.recordView().upsert(tuple(), null);

        Long val = primitiveView.get(DEFAULT_ID, null);
        Long missingVal = primitiveView.get(-1L, null);

        assertEquals(DEFAULT_ID, val);
        assertNull(missingVal);
    }

    @Test
    public void testPrimitivePutBinaryGet() {
        Table table = oneColumnTable();
        RecordView<String> primitiveView = table.recordView(Mapper.of(String.class));

        primitiveView.upsert("abc", null);

        Tuple tuple = table.recordView().get(oneColumnTableKey("abc"), null);
        assertEquals("abc", tuple.stringValue(0));
    }

    @Test
    public void testMissingValueColumnsAreSkipped() {
        Table table = fullTable();
        KeyValueView<Tuple, Tuple> kvView = table.keyValueView();
        RecordView<IncompletePojo> pojoView = table.recordView(IncompletePojo.class);

        kvView.put(allClumnsTableKey(1), allColumnsTableVal("x"), null);

        var key = new IncompletePojo();
        key.id = "1";
        key.gid = 1;

        // This POJO does not have fields for all table columns, and this is ok.
        IncompletePojo val = pojoView.get(key, null);

        assertEquals(1, val.gid);
        assertEquals("1", val.id);
        assertEquals("x", val.zstring);
        assertEquals(2, val.zbytes[1]);
        assertEquals(11, val.zbyte);
    }

    @Test
    public void testAllColumnsBinaryPutPojoGet() {
        Table table = fullTable();
        RecordView<AllColumnsPojo> pojoView = table.recordView(Mapper.of(AllColumnsPojo.class));

        table.recordView().upsert(allColumnsTableVal("foo"), null);

        var key = new AllColumnsPojo();
        key.gid = (int) (long) DEFAULT_ID;
        key.id = String.valueOf(DEFAULT_ID);

        AllColumnsPojo res = pojoView.get(key, null);
        assertEquals(11, res.zbyte);
        assertEquals(12, res.zshort);
        assertEquals(13, res.zint);
        assertEquals(14, res.zlong);
        assertEquals(1.5f, res.zfloat);
        assertEquals(1.6, res.zdouble);
        assertEquals(localDate, res.zdate);
        assertEquals(localTime, res.ztime);
        assertEquals(instant, res.ztimestamp);
        assertEquals("foo", res.zstring);
        assertArrayEquals(new byte[]{1, 2}, res.zbytes);
        assertEquals(BitSet.valueOf(new byte[]{32}), res.zbitmask);
        assertEquals(21, res.zdecimal.longValue());
        assertEquals(22, res.znumber.longValue());
        assertEquals(uuid, res.zuuid);
    }

    @Test
    public void testAllColumnsPojoPutBinaryGet() {
        Table table = fullTable();
        RecordView<AllColumnsPojo> pojoView = table.recordView(Mapper.of(AllColumnsPojo.class));

        var val = new AllColumnsPojo();

        val.gid = 111;
        val.id = "112";
        val.zbyte = 113;
        val.zshort = 114;
        val.zint = 115;
        val.zlong = 116;
        val.zfloat = 1.17f;
        val.zdouble = 1.18;
        val.zdate = localDate;
        val.ztime = localTime;
        val.ztimestamp = instant;
        val.zstring = "119";
        val.zbytes = new byte[]{120};
        val.zbitmask = BitSet.valueOf(new byte[]{121});
        val.zdecimal = BigDecimal.valueOf(122);
        val.znumber = BigInteger.valueOf(123);
        val.zuuid = uuid;

        pojoView.upsert(val, null);

        Tuple res = table.recordView().get(Tuple.create().set("id", "112").set("gid", 111), null);

        assertNotNull(res);
        assertEquals(111, res.intValue("gid"));
        assertEquals("112", res.stringValue("id"));
        assertEquals(113, res.byteValue("zbyte"));
        assertEquals(114, res.shortValue("zshort"));
        assertEquals(115, res.intValue("zint"));
        assertEquals(116, res.longValue("zlong"));
        assertEquals(1.17f, res.floatValue("zfloat"));
        assertEquals(1.18, res.doubleValue("zdouble"));
        assertEquals(localDate, res.dateValue("zdate"));
        assertEquals(localTime, res.timeValue("ztime"));
        assertEquals(instant, res.timestampValue("ztimestamp"));
        assertEquals("119", res.stringValue("zstring"));
        assertEquals(120, ((byte[]) res.value("zbytes"))[0]);
        assertEquals(BitSet.valueOf(new byte[]{121}), res.bitmaskValue("zbitmask"));
        assertEquals(122, ((Number) res.value("zdecimal")).longValue());
        assertEquals(BigInteger.valueOf(123), res.value("znumber"));
        assertEquals(uuid, res.uuidValue("zuuid"));
    }

    @Test
    public void testMissingKeyColumnThrowsException() {
        RecordView<NamePojo> recordView = defaultTable().recordView(NamePojo.class);

        CompletionException e = assertThrows(CompletionException.class, () -> recordView.get(new NamePojo(), null));
        IgniteClientException ice = (IgniteClientException) e.getCause();

        assertEquals("No field found for column id", ice.getMessage());
    }

    @Test
    public void testNullablePrimitiveFields() {
        RecordView<IncompletePojoNullable> pojoView = fullTable().recordView(IncompletePojoNullable.class);
        RecordView<Tuple> tupleView = fullTable().recordView();

        var rec = new IncompletePojoNullable();
        rec.id = "1";
        rec.gid = 1;

        pojoView.upsert(rec, null);

        IncompletePojoNullable res = pojoView.get(rec, null);
        Tuple binRes = tupleView.get(Tuple.create().set("id", "1").set("gid", 1L), null);

        assertNotNull(res);
        assertNotNull(binRes);

        assertNull(res.zbyte);
        assertNull(res.zshort);
        assertNull(res.zint);
        assertNull(res.zlong);
        assertNull(res.zfloat);
        assertNull(res.zdouble);

        for (int i = 0; i < binRes.columnCount(); i++) {
            if (binRes.columnName(i).endsWith("id")) {
                continue;
            }

            assertNull(binRes.value(i));
        }
    }

    @Test
    public void testGetAll() {
        Table table = defaultTable();
        RecordView<PersonPojo> pojoView = table.recordView(Mapper.of(PersonPojo.class));

        table.recordView().upsert(tuple(), null);
        table.recordView().upsert(tuple(100L, "100"), null);

        Collection<PersonPojo> keys = List.of(
                new PersonPojo(DEFAULT_ID, "blabla"),
                new PersonPojo(101L, "1234"),
                new PersonPojo(100L, "qwerty"));

        PersonPojo[] res = pojoView.getAll(keys, null).toArray(new PersonPojo[0]);

        assertEquals(3, res.length);

        assertNotNull(res[0]);
        assertNull(res[1]);
        assertNotNull(res[2]);

        assertEquals(DEFAULT_ID, res[0].id);
        assertEquals(DEFAULT_NAME, res[0].name);

        assertEquals(100L, res[2].id);
        assertEquals("100", res[2].name);
    }

    @Test
    public void testGetAllPrimitive() {
        Table table = oneColumnTable();
        RecordView<String> primitiveView = table.recordView(Mapper.of(String.class));

        primitiveView.upsertAll(List.of("a", "c"), null);

        String[] res = primitiveView.getAll(List.of("a", "b", "c"), null).toArray(new String[0]);

        assertEquals("a", res[0]);
        assertNull(res[1]);
        assertEquals("c", res[2]);
    }

    @Test
    public void testUpsertAll() {
        RecordView<PersonPojo> pojoView = defaultTable().recordView(Mapper.of(PersonPojo.class));

        Collection<PersonPojo> pojos = List.of(
                new PersonPojo(DEFAULT_ID, DEFAULT_NAME),
                new PersonPojo(100L, "100"),
                new PersonPojo(101L, "101"));

        pojoView.upsertAll(pojos, null);

        assertEquals(DEFAULT_NAME, pojoView.get(new PersonPojo(DEFAULT_ID), null).name);
        assertEquals("100", pojoView.get(new PersonPojo(100L), null).name);
        assertEquals("101", pojoView.get(new PersonPojo(101L), null).name);
    }

    @Test
    public void testGetAndUpsert() {
        RecordView<PersonPojo> pojoView = defaultTable().recordView(Mapper.of(PersonPojo.class));

        pojoView.upsert(new PersonPojo(DEFAULT_ID, DEFAULT_NAME), null);

        PersonPojo res1 = pojoView.getAndUpsert(new PersonPojo(DEFAULT_ID, "new_name"), null);
        PersonPojo res2 = pojoView.getAndUpsert(new PersonPojo(100L, "name"), null);

        assertEquals(DEFAULT_NAME, res1.name);
        assertEquals("new_name", pojoView.get(new PersonPojo(DEFAULT_ID), null).name);

        assertNull(res2);
        assertEquals("name", pojoView.get(new PersonPojo(100L), null).name);
    }

    @Test
    public void testInsert() {
        RecordView<PersonPojo> pojoView = defaultTable().recordView(Mapper.of(PersonPojo.class));

        pojoView.upsert(new PersonPojo(DEFAULT_ID, DEFAULT_NAME), null);

        boolean res1 = pojoView.insert(new PersonPojo(DEFAULT_ID, "foobar"), null);
        boolean res2 = pojoView.insert(new PersonPojo(100L, "100"), null);

        assertFalse(res1);
        assertTrue(res2);
        assertEquals("100", pojoView.get(new PersonPojo(100L), null).name);
    }

    @Test
    public void testInsertAll() {
        RecordView<PersonPojo> pojoView = defaultTable().recordView(Mapper.of(PersonPojo.class));

        pojoView.upsert(new PersonPojo(DEFAULT_ID, DEFAULT_NAME), null);

        Collection<PersonPojo> res1 = pojoView.insertAll(List.of(new PersonPojo(10L, "10"), new PersonPojo(20L)), null);
        Collection<PersonPojo> res2 = pojoView.insertAll(List.of(new PersonPojo(DEFAULT_ID), new PersonPojo(10L)), null);
        Collection<PersonPojo> res3 = pojoView.insertAll(
                List.of(new PersonPojo(DEFAULT_ID, "new_name"), new PersonPojo(30L)),
                null
        );

        assertEquals(0, res1.size());
        assertEquals(2, res2.size());
        assertEquals(1, res3.size());

        assertEquals("10", pojoView.get(new PersonPojo(10L), null).name);
        assertNull(pojoView.get(new PersonPojo(20L), null).name);

        assertEquals("new_name", res3.iterator().next().name);
    }

    @Test
    public void testReplace() {
        RecordView<PersonPojo> pojoView = defaultTable().recordView(Mapper.of(PersonPojo.class));

        pojoView.upsert(new PersonPojo(DEFAULT_ID, DEFAULT_NAME), null);

        assertFalse(pojoView.replace(new PersonPojo(-1L), null));
        assertTrue(pojoView.replace(new PersonPojo(DEFAULT_ID, "new_name"), null));

        assertNull(pojoView.get(new PersonPojo(-1L), null));
        assertEquals("new_name", pojoView.get(new PersonPojo(DEFAULT_ID), null).name);
    }

    @Test
    public void testReplaceExact() {
        RecordView<PersonPojo> pojoView = defaultTable().recordView(Mapper.of(PersonPojo.class));

        pojoView.upsert(new PersonPojo(DEFAULT_ID, DEFAULT_NAME), null);

        assertFalse(pojoView.replace(new PersonPojo(DEFAULT_ID, "x"), new PersonPojo(DEFAULT_ID, "new_name"), null));
        assertFalse(pojoView.replace(new PersonPojo(-1L, "x"), new PersonPojo(DEFAULT_ID, "new_name"), null));
        assertTrue(pojoView.replace(new PersonPojo(DEFAULT_ID, DEFAULT_NAME), new PersonPojo(DEFAULT_ID, "new_name2"), null));

        assertNull(pojoView.get(new PersonPojo(-1L), null));
        assertEquals("new_name2", pojoView.get(new PersonPojo(DEFAULT_ID), null).name);
    }

    @Test
    public void testGetAndReplace() {
        RecordView<PersonPojo> pojoView = defaultTable().recordView(Mapper.of(PersonPojo.class));

        pojoView.upsert(new PersonPojo(DEFAULT_ID, DEFAULT_NAME), null);

        PersonPojo res1 = pojoView.getAndReplace(new PersonPojo(DEFAULT_ID, "new_name"), null);
        PersonPojo res2 = pojoView.getAndReplace(new PersonPojo(100L, "name"), null);

        assertEquals(DEFAULT_NAME, res1.name);
        assertEquals("new_name", pojoView.get(new PersonPojo(DEFAULT_ID), null).name);

        assertNull(res2);
        assertNull(pojoView.get(new PersonPojo(100L), null));
    }

    @Test
    public void testDelete() {
        RecordView<PersonPojo> pojoView = defaultTable().recordView(Mapper.of(PersonPojo.class));

        pojoView.upsert(new PersonPojo(DEFAULT_ID, DEFAULT_NAME), null);

        boolean res1 = pojoView.delete(new PersonPojo(DEFAULT_ID), null);
        boolean res2 = pojoView.delete(new PersonPojo(100L, "name"), null);

        assertTrue(res1);
        assertFalse(res2);

        assertNull(pojoView.get(new PersonPojo(DEFAULT_ID), null));
    }

    @Test
    public void testDeleteExact() {
        RecordView<PersonPojo> pojoView = defaultTable().recordView(Mapper.of(PersonPojo.class));

        pojoView.upsert(new PersonPojo(DEFAULT_ID, DEFAULT_NAME), null);
        pojoView.upsert(new PersonPojo(100L, "100"), null);

        boolean res1 = pojoView.deleteExact(new PersonPojo(DEFAULT_ID), null);
        boolean res2 = pojoView.deleteExact(new PersonPojo(100L), null);
        boolean res3 = pojoView.deleteExact(new PersonPojo(100L, "100"), null);

        assertFalse(res1);
        assertFalse(res2);
        assertTrue(res3);

        assertNotNull(pojoView.get(new PersonPojo(DEFAULT_ID), null));
        assertNull(pojoView.get(new PersonPojo(100L), null));
    }

    @Test
    public void testGetAndDelete() {
        RecordView<PersonPojo> pojoView = defaultTable().recordView(Mapper.of(PersonPojo.class));

        pojoView.upsert(new PersonPojo(DEFAULT_ID, DEFAULT_NAME), null);

        PersonPojo res1 = pojoView.getAndDelete(new PersonPojo(DEFAULT_ID), null);
        PersonPojo res2 = pojoView.getAndDelete(new PersonPojo(100L), null);

        assertEquals(DEFAULT_NAME, res1.name);
        assertNull(pojoView.get(new PersonPojo(DEFAULT_ID), null));

        assertNull(res2);
    }

    @Test
    public void testDeleteAll() {
        RecordView<PersonPojo> pojoView = defaultTable().recordView(Mapper.of(PersonPojo.class));

        pojoView.upsertAll(
                List.of(new PersonPojo(1L, "1"), new PersonPojo(2L, "2"), new PersonPojo(3L, "3")),
                null
        );

        Collection<PersonPojo> res1 = pojoView.deleteAll(List.of(new PersonPojo(10L), new PersonPojo(20L)), null);
        Collection<PersonPojo> res2 = pojoView.deleteAll(List.of(new PersonPojo(1L), new PersonPojo(3L)), null);

        assertEquals(2, res1.size());
        assertEquals(0, res2.size());

        assertNull(pojoView.get(new PersonPojo(1L), null));
        assertEquals("2", pojoView.get(new PersonPojo(2L), null).name);
        assertNull(pojoView.get(new PersonPojo(3L), null));
    }

    @Test
    public void testDeleteAllExact() {
        RecordView<PersonPojo> pojoView = defaultTable().recordView(Mapper.of(PersonPojo.class));

        pojoView.upsertAll(
                List.of(new PersonPojo(1L, "1"), new PersonPojo(2L, "2"), new PersonPojo(3L, "3")),
                null
        );

        Collection<PersonPojo> res1 = pojoView.deleteAllExact(
                List.of(new PersonPojo(1L, "a"), new PersonPojo(3L, "b")),
                null
        );

        Collection<PersonPojo> res2 = pojoView.deleteAllExact(
                List.of(new PersonPojo(1L, "1"), new PersonPojo(3L, "3")),
                null
        );

        assertEquals(2, res1.size());
        assertEquals(0, res2.size());

        assertNull(pojoView.get(new PersonPojo(1L), null));
        assertEquals("2", pojoView.get(new PersonPojo(2L), null).name);
        assertNull(pojoView.get(new PersonPojo(3L), null));
    }

    private static class PersonPojo {
        public long id;

        public String name;

        public PersonPojo() {
            // No-op.
        }

        public PersonPojo(long id) {
            this.id = id;
        }

        public PersonPojo(long id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    private static class NamePojo {
        public String name;
    }

    private static class IncompletePojo {
        public byte zbyte;
        public String id;
        public int gid;
        public String zstring;
        public byte[] zbytes;
    }

    private static class IncompletePojoNullable {
        public int gid;
        public String id;
        public Byte zbyte;
        public Short zshort;
        public Integer zint;
        public Long zlong;
        public Float zfloat;
        public Double zdouble;
    }

    private static class AllColumnsPojo {
        public int gid;
        public String id;
        public byte zbyte;
        public short zshort;
        public int zint;
        public long zlong;
        public float zfloat;
        public double zdouble;
        public LocalDate zdate;
        public LocalTime ztime;
        public Instant ztimestamp;
        public String zstring;
        public byte[] zbytes;
        public UUID zuuid;
        public BitSet zbitmask;
        public BigDecimal zdecimal;
        public BigInteger znumber;
    }
}
