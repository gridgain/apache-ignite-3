package org.apache.ignite.metastorage.client;

import java.util.List;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.metastorage.common.Conditions;
import org.apache.ignite.metastorage.common.Entry;
import org.apache.ignite.metastorage.common.Key;
import org.apache.ignite.metastorage.common.Operation;
import org.apache.ignite.metastorage.common.Operations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** */
public class MetaStorageServiceBasicTest {
    private static final IgniteLogger LOG = IgniteLogger.forClass(MetaStorageServiceBasicTest.class);

    private TestMetaStorageService metaStorageService;

    @BeforeEach
    public void before() {
        metaStorageService = new TestMetaStorageService();
    }

    @Test
    public void testOperations() throws Exception {
        Key key = new Key("k");

        assertNull(metaStorageService.get(key).get());

        metaStorageService.put(key, ByteUtils.toBytes(0));
        metaStorageService.put(key, ByteUtils.toBytes(1));
        metaStorageService.put(key, ByteUtils.toBytes(2));

        Key key2 = new Key("kk");

        assertNull(metaStorageService.get(key2).get());

        metaStorageService.put(key2, ByteUtils.toBytes(0));
        metaStorageService.put(key2, ByteUtils.toBytes(1));
        metaStorageService.put(key2, ByteUtils.toBytes(2));

        Entry lastEntry = metaStorageService.get(key).get();
        assertEquals(3, lastEntry.revision());

        Entry lastEntry2 = metaStorageService.get(key2).get();
        assertEquals(6, lastEntry2.revision());

        Entry entry = metaStorageService.get(key, 2).get();
        assertEquals(2, entry.revision());
        Entry entry2 = metaStorageService.get(key2, 2).get();
        assertNull(entry2);

        entry = metaStorageService.get(key, 5).get();
        assertEquals(3, entry.revision());
        entry2 = metaStorageService.get(key2, 5).get();
        assertEquals(5, entry2.revision());

        List<Key> key1 = List.of(key, key2);
        List<Operation> suc = List.of(Operations.put(ByteUtils.toBytes(100)), Operations.put(ByteUtils.toBytes(100)));
        List<Operation> fail = List.of(Operations.noop(), Operations.noop());

        metaStorageService.invoke(key1, Conditions.revision().eq(2), suc, fail, 0);

        // Expecting nothing changed.
        entry = metaStorageService.get(key, 5).get();
        assertEquals(3, entry.revision());
        entry2 = metaStorageService.get(key2, 5).get();
        assertEquals(5, entry2.revision());

        metaStorageService.invoke(key1, Conditions.revision().eq(3), suc, fail, 0);

        // Expecting nothing changed.
        entry = metaStorageService.get(key).get();
        assertEquals(100, fromBytes(entry.value()));
        entry2 = metaStorageService.get(key2).get();
        assertEquals(100, fromBytes(entry2.value()));

        System.out.println(metaStorageService.data);
    }
}
