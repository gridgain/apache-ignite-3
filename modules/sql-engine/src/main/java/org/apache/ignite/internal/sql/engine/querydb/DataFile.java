package org.apache.ignite.internal.sql.engine.querydb;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class DataFile {

    public static String escaceString(String valStr) {
        valStr = valStr.replace("'", "''");
        valStr = valStr.replace("\"", "\\\"");
        valStr = valStr.replace("\t", "\\t");
        valStr = valStr.replace("\n", "\\n");
        return valStr.replace("\r", "\\r");
    }

    private final DbSchema dbSchema;

    private final DbRecords dbRecords;

    public DataFile(DbSchema dbSchema, DbRecords records) {
        this.dbSchema = dbSchema;
        this.dbRecords = records;
    }

    public void dumpToSql(StandardOpenOption... options) {
        dumpToSql(Paths.get("queries.sql"), options);
    }

    public void dumpSqlSchema(Path path, StandardOpenOption... options) {
        try (var out = Files.newBufferedWriter(path, StandardCharsets.UTF_8, options)) {
            out.write("--- schema ");
            out.newLine();
            out.write(dbSchema.makeSqlString());
            out.newLine();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void dumpToSql(Path path, StandardOpenOption... options) {
        try (var out = Files.newBufferedWriter(path, StandardCharsets.UTF_8, options)) {
            for (var stmt : dbRecords.statements()) {
                for (var insert : stmt.inserts()) {
                    String sql = insert.getKey().makeSqlString(insert.getValue());
                    out.write(sql);
                    out.write(";");
                    out.newLine();
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void dumpToJson(StandardOpenOption... options) {
        dumpToSql(Paths.get("queries.jsonl"), options);
    }

    public void dumpToJson(Path path, StandardOpenOption... options) {
        try (var out = Files.newBufferedWriter(path, StandardCharsets.UTF_8, options)) {
            for (var stmt : dbRecords.statements()) {
                for (var insert : stmt.inserts()) {
                    StringBuilder json = new StringBuilder().append("{");
                    json.append("\"").append("_TABLE").append("\"");
                    json.append(": ").append("\"").append(insert.getKey().table().name()).append("\"");

                    for (var entry : insert.getValue().entrySet()) {
                        String key = entry.getKey();
                        Object val = entry.getValue();
                        json.append(", ");
                        json.append("\"").append(key).append("\"");
                        json.append(": ");
                        if (val instanceof String) {
                            String valStr = (String) val;
                            json.append("\"").append(escaceString(valStr)).append("\"");
                        } else {
                            json.append(val);
                        }
                    }

                    json.append("}");
                    out.write(json.toString());
                    out.newLine();
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
