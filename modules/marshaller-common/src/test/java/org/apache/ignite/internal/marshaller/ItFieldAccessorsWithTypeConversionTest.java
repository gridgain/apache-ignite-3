package org.apache.ignite.internal.marshaller;

import static org.assertj.core.api.Assertions.assertThat;

import java.text.DateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Date;
import org.apache.ignite.catalog.ColumnType;
import org.apache.ignite.catalog.definitions.ColumnDefinition;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.mapper.TypeConverter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ItFieldAccessorsWithTypeConversionTest extends ClusterPerClassIntegrationTest {

    private static IgniteClient client;

    @BeforeAll
    static void startClient() {
        client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build();
    }

    @AfterAll
    static void stopClient() throws Exception {
        client.close();
    }

    @Test
    void valueWithTypeConverter() {
        var tableDef = TableDefinition.builder("PrimTestTable")
                .columns(
                        ColumnDefinition.column("ID", ColumnType.INT32),
                        ColumnDefinition.column("VAL", ColumnType.DATE)
                )
                .primaryKey("ID")
                .build();

        client.catalog().createTable(tableDef).execute();

        var keyMapper = Mapper.of(Integer.class);
        var valMapper = Mapper.of(Date.class, "VAL", new DateTypeConverter());
        var table = client.tables().table("PrimTestTable").keyValueView(keyMapper, valMapper);

        var expected = new Date();
        table.put(null, 1, expected);
        var actual = table.get(null, 1);

        var df = DateFormat.getDateInstance();
        assertThat(df.format(actual)).isEqualTo(df.format(expected));
    }

    @Test
    void pojoWithTypeConverter() {
        var tableDef = TableDefinition.builder("PojoTestTable")
                .key(Integer.class)
                .value(MyPojoWithDate.class)
                .build();

        client.catalog().createTable(tableDef).execute();

        var keyMapper = Mapper.of(Integer.class);
        var valMapper = Mapper.builder(MyPojoWithDate.class)
                .map("name", "NAME")
                .map("birthday", "BIRTHDAY", new DateTypeConverter())
                .build();

        var table = client.tables().table("PojoTestTable").keyValueView(keyMapper, valMapper);

        var expectedDate = new Date();
        var expected = new MyPojoWithDate("someName", expectedDate);

        table.put(null, 1, expected);
        var actual = table.get(null, 1);

        var df = DateFormat.getDateInstance();
        assertThat(df.format(actual.birthday)).isEqualTo(df.format(expectedDate));
    }

    private static class DateTypeConverter implements TypeConverter<Date, LocalDate> {

        @Override
        public LocalDate toColumnType(Date obj) throws Exception {
            var timestamp = obj.toInstant();
            return LocalDate.ofInstant(timestamp, ZoneOffset.UTC);
        }

        @Override
        public Date toObjectType(LocalDate data) throws Exception {
            Instant instant = data.atStartOfDay().toInstant(ZoneOffset.UTC);
            return Date.from(instant);
        }
    }

    private static class MyPojoWithDate {
        private String name;

        private Date birthday;

        public MyPojoWithDate() {
            // Intentionally left blank.
        }

        public MyPojoWithDate(String name, Date birthday) {
            this.name = name;
            this.birthday = birthday;
        }
    }

}
