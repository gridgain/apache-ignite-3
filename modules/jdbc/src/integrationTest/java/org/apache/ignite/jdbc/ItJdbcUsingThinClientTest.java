package org.apache.ignite.jdbc;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.junit.jupiter.api.Test;

public class ItJdbcUsingThinClientTest extends ClusterPerClassIntegrationTest {
    /** URL. */
    protected static final String URL = "jdbc:ignite:thin://127.0.0.1:10800";
    /** Default schema. */
    protected static final String DEFAULT_SCHEMA = "PUBLIC";

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    void testX() throws SQLException {
        try (Connection conn = DriverManager.getConnection(URL)) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT 'xxx' as QQQ")) {
                    assertTrue(rs.next());

                    System.out.println(rs.getString(1));
                    System.out.println(rs.getString("QQQ"));

                    assertFalse(rs.next());
                }
            }
        }
    }
}
