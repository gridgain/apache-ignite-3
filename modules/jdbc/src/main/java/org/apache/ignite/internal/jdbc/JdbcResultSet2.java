package org.apache.ignite.internal.jdbc;

import java.math.BigDecimal;
import java.net.URL;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.internal.jdbc.proto.SqlStateCode;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.jetbrains.annotations.Nullable;

public class JdbcResultSet2 extends AbstractJdbcResultSet {

    private final ResultSet<SqlRow> clientSet;

    private final JdbcStatement stmt;

    private @Nullable SqlRow row;

    public JdbcResultSet2(JdbcStatement stmt, ResultSet<SqlRow> clientSet) {
        this.clientSet = clientSet;
        this.stmt = stmt;
    }

    public boolean hasResultSet() {
        return clientSet.hasRowSet();
    }

    public long updatedCount() {
        long affected = clientSet.affectedRows();

        if (affected >= 0) {
            return affected;
        }

        return clientSet.wasApplied() ? 0 : -1;
    }

    @Nullable JdbcResultSet2 getNextResultSet() throws SQLException {
        return null;
    }

    public void closeStatement(boolean closeStmt) {
        // TODO
        // this.closeStmt = closeStmt;
    }

    boolean holdResults() {
        throw new UnsupportedOperationException();
    }

    boolean holdsResources() {
        return false;
    }

    void close0(boolean removeFromResources) throws SQLException {
        // TODO
    }


    @Override
    public boolean next() throws SQLException {
        ensureNotClosed();

        if (clientSet.hasNext()) {
            row = clientSet.next();

            return true;
        }

        row = null;

        return false;
    }

    @Override
    public void close() throws SQLException {
        if (!closed) {
            closed = true;

            clientSet.close();
        }
    }

    @Override
    public boolean wasNull() throws SQLException {
        ensureNotClosed();
        ensureHasCurrentRow();

        return wasNull;
    }

    @Override
    protected Object getJdbcValue(int colIdx) throws SQLException {
        Object value = getValue(colIdx);

        if (value instanceof Instant) {
            LocalDateTime localDateTime = instantWithLocalTimeZone((Instant) value);
            return Timestamp.valueOf(localDateTime);
        } else if (value instanceof LocalTime) {
            return Time.valueOf((LocalTime) value);
        } else if (value instanceof LocalDateTime) {
            return Timestamp.valueOf((LocalDateTime) value);
        } else {
            return value;
        }
    }

    private Object getValue(int colIdx) throws SQLException {
        ensureNotClosed();
        ensureHasCurrentRow();

        try {
            assert row != null;

            Object val = row.value(colIdx - 1);

            wasNull = val == null;

            return val;
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("Invalid column index: " + colIdx, SqlStateCode.PARSING_EXCEPTION, e);
        }
    }

    private LocalDateTime instantWithLocalTimeZone(Instant val) throws SQLException {
        JdbcConnection connection = (JdbcConnection) stmt.getConnection();
        ZoneId zoneId = connection.connectionProperties().getConnectionTimeZone();
        if (zoneId == null) {
            zoneId = ZoneId.systemDefault();
        }
        return LocalDateTime.ofInstant(val, zoneId);
    }

    /**
     * Ensures that result set is positioned on a row.
     *
     * @throws SQLException If result set is not positioned on a row.
     */
    @Override
    protected void ensureHasCurrentRow() throws SQLException {
        if (row == null) {
            throw new SQLException("Result set is not positioned on a row.");
        }
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return false;
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return false;
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return 0;
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return 0;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return 0;
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return 0;
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return 0;
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return 0;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return new byte[0];
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return new byte[0];
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        ensureNotClosed();

        return metaOrThrow();
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return null;
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return null;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        ensureNotClosed();

        Objects.requireNonNull(columnLabel);

        Integer order = columnOrder().get(columnLabel.toUpperCase());

        if (order == null) {
            throw new SQLException("Column not found: " + columnLabel, SqlStateCode.PARSING_EXCEPTION);
        }

        assert order >= 0;

        return order + 1;

    }

    /** Column order map. */
    private @Nullable Map<String, Integer> colOrder;

    /**
     * Init if needed and return column order.
     *
     * @return Column order map.
     * @throws SQLException On error.
     */
    private Map<String, Integer> columnOrder() throws SQLException {
        if (colOrder != null) {
            return colOrder;
        }

        initColumnOrder(metaOrThrow());

        return colOrder;
    }

    /**
     * Init column order map.
     */
    private void initColumnOrder(ResultSetMetaData jdbcMeta) throws SQLException {
        colOrder = new HashMap<>(jdbcMeta.getColumnCount());

        for (int i = 0; i < jdbcMeta.getColumnCount(); ++i) {
            String colName = jdbcMeta.getColumnLabel(i + 1).toUpperCase();

            if (!colOrder.containsKey(colName)) {
                colOrder.put(colName, i);
            }
        }
    }

    private ResultSetMetaData metaOrThrow() throws SQLException {
        ResultSetMetadata meta = clientSet.metadata();

        if (meta == null) {
            throw new SQLException("Result doesn't have metadata");
        }

        return new JdbcResultSetMetadata2(meta);
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return false;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return false;
    }

    @Override
    public boolean isFirst() throws SQLException {
        return false;
    }

    @Override
    public boolean isLast() throws SQLException {
        return false;
    }

    @Override
    public int getRow() throws SQLException {
        return 0;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getType() throws SQLException {
        return 0;
    }

    @Override
    public Statement getStatement() throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }
}
