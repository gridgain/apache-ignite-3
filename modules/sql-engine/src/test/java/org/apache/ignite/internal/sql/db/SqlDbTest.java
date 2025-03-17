package org.apache.ignite.internal.sql.db;

import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.prepare.MultiStepPlan;
import org.apache.ignite.internal.sql.engine.querydb.DataFile;
import org.apache.ignite.internal.sql.engine.querydb.QueryDetailsCollector;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Test;

public class SqlDbTest extends BaseIgniteAbstractTest {

    private final TestCluster cluster = TestBuilders.cluster()
            .nodes("N1")
            .addTable().name("T1")
            .addKeyColumn("K", NativeTypes.INT32)
            .addColumn("COL1", NativeTypes.STRING)
            .addColumn("COL2", NativeTypes.BOOLEAN)
            .addColumn("COL3", NativeTypes.INT32)
            .end()
            .addTable().name("T2")
            .addKeyColumn("K", NativeTypes.INT32)
            .addColumn("COL1", NativeTypes.STRING)
            .end()
            .build();

    @Test
    public void test() {
        cluster.start();

        String query1 = "SELECT a.col2, a.col1, a.col3 + 1, 42, CAST(b.col3 AS VARCHAR), b.col3::VARCHAR FROM T1 a JOIN T1 b USING (col1)";
        String query2 = "SELECT COUNT(*), MAX(col1), SUM(k), MIN(col3 + k) FROM t1  WHERE k > 1000 GROUP BY col2, true";
        String query3 = "SELECT DISTINCT(col3 + k) FROM t1 GROUP BY k, col3";
        String query4 = "SELECT SUM(k) FROM t1 GROUP BY col3";
        String query5 = "SELECT * FROM t1";
        String query6 = "SELECT a.*, * FROM t1 as a, t2 as b WHERE a.col1 = b.col1";
        String query7 = "SELECT * FROM system_range(1, 1000)";
        String query8 = "SELECT * FROM t1 ORDER BY col1";
        String query9 = "SELECT\n\t*\nFROM \nt1\nORDER BY\ncol1";

        QueryDetailsCollector collector = new QueryDetailsCollector();

        List<String> queryList = List.of(query1, query2, query3, query4, query5, query6, query7, query8, query9);

        for (String query : queryList) {
            MultiStepPlan plan = (MultiStepPlan) cluster.node("N1").prepare(query);

            collector.collect(query, plan);
        }

        System.err.println("--- schema ");
        System.err.println(collector.schema().makeSqlString());

        System.err.println("--- data ");
        for (var stmt : collector.records().statements()) {
            for (var insert : stmt.inserts()) {
                String sql = insert.getKey().makeSqlString(insert.getValue());
                System.err.print(sql);
                System.err.print(";");
                System.err.print(System.lineSeparator());
            }
        }

        DataFile dataFile = new DataFile(collector.schema(), collector.records());
        dataFile.dumpToJson(Paths.get("q.jsonl"), StandardOpenOption.APPEND, StandardOpenOption.CREATE);

        dataFile.dumpSqlSchema(Paths.get("query_schema.sql"));
        dataFile.dumpToSql(Paths.get("q.sql"), StandardOpenOption.APPEND, StandardOpenOption.CREATE);
    }
}
