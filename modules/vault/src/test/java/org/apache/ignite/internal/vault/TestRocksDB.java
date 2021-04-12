package org.apache.ignite.internal.vault;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;


public class TestRocksDB {

    private static final String dbPath   = "./src/test/resources/org.apache.ignite.internal.vault/rocksdb-data/";

    @BeforeAll
    public static void setUp() {
        RocksDB.loadLibrary();
    }

    @AfterEach
    public void cleanUp() {
        Path pathToBeDeleted = Paths.get(dbPath);

        try {
            Files.walk(pathToBeDeleted)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDefaultColumnFamily() {
        try (final Options options = new Options().setCreateIfMissing(true)) {
            try (final RocksDB rocksDB = RocksDB.open(options, dbPath)) {
                byte[] key = "Hello".getBytes();

                rocksDB.put(key, "World".getBytes());

                System.out.println(new String(rocksDB.get(key)));

                rocksDB.put("SecondKey".getBytes(), "SecondValue".getBytes());

                List<byte[]> keys = Arrays.asList(key, "SecondKey".getBytes(), "missKey".getBytes());

                List<byte[]> values = rocksDB.multiGetAsList(keys);

                for (int i = 0; i < keys.size(); i++)
                    System.out.println("multiGet " + new String(keys.get(i)) + ":" + (values.get(i) != null ? new String(values.get(i)) : null));

                RocksIterator iter = rocksDB.newIterator();

                for (iter.seekToFirst(); iter.isValid(); iter.next())
                    System.out.println("iterator key:" + new String(iter.key()) + ", iter value:" + new String(iter.value()));

                rocksDB.delete(key);

                System.out.println("after remove key:" + new String(key));

                iter = rocksDB.newIterator();

                for (iter.seekToFirst(); iter.isValid(); iter.next())
                    System.out.println("iterator key:" + new String(iter.key()) + ", iter value:" + new String(iter.value()));
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }
}
