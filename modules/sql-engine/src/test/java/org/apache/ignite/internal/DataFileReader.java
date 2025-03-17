package org.apache.ignite.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import org.apache.ignite.internal.sql.engine.querydb.DbSchema;

public class DataFileReader {

    private final DbSchema schema;

    public DataFileReader(DbSchema schema) {
        this.schema = schema;
    }

    @SuppressWarnings("unchecked")
    public void read(InputStream is) throws IOException {
        ObjectMapper mapper = new ObjectMapper();

        try (var reader = new InputStreamReader(is, StandardCharsets.UTF_8)) {
            try (BufferedReader bis = new BufferedReader(reader)) {
                String line = bis.readLine();
                while (line != null) {
                    System.err.println(line);
                    HashMap<String, Object> map = mapper.readValue(line, HashMap.class);
                    System.err.println(map);
                    line = bis.readLine();
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        var reader = new DataFileReader(new DbSchema());
        var path = Paths.get("/Users/max/Projects/java/ignite-3/modules/sql-engine/queries.txt");

        try (var is = Files.newInputStream(path)) {
            reader.read(is);
        }
    }
}
