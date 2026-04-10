package org.isaacmcfadyen;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

public class D1PreparedStatement extends D1Queryable implements PreparedStatement {
    private final String sql;
    private JSONArray params = new JSONArray();
    private D1ResultSet cachedResults = null;

    D1PreparedStatement(String apiToken, String accountId, String databaseId, String sql) {
        super(apiToken, accountId, databaseId);
        this.sql = sql;
    }

    // ---------------------------------------------------------------------------
    // PreparedStatement execute methods
    // ---------------------------------------------------------------------------

    @Override
    public ResultSet executeQuery() throws SQLException {
        JSONObject json = queryDatabase(sql, params);
        cachedResults = generateResultSet(json, sql);
        return cachedResults;
    }

    @Override
    public int executeUpdate() throws SQLException {
        queryDatabase(sql, params);
        return Math.max(0, lastUpdateCount);
    }

    @Override
    public boolean execute() throws SQLException {
        JSONObject json = queryDatabase(sql, params);
        cachedResults = generateResultSet(json, sql);
        return cachedResults.getRowCount() > 0;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return cachedResults;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return lastUpdateCount;
    }

    // ---------------------------------------------------------------------------
    // Parameter binding
    // ---------------------------------------------------------------------------

    @Override
    public void clearParameters() throws SQLException {
        params = new JSONArray();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        params.put(parameterIndex - 1, JSONObject.NULL);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        params.put(parameterIndex - 1, JSONObject.NULL);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        params.put(parameterIndex - 1, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        params.put(parameterIndex - 1, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        params.put(parameterIndex - 1, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        params.put(parameterIndex - 1, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        params.put(parameterIndex - 1, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        params.put(parameterIndex - 1, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        params.put(parameterIndex - 1, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        params.put(parameterIndex - 1, x != null ? x.toPlainString() : JSONObject.NULL);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        params.put(parameterIndex - 1, x != null ? x : JSONObject.NULL);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBytes not supported");
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        params.put(parameterIndex - 1, x != null ? x.toString() : JSONObject.NULL);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        setDate(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        params.put(parameterIndex - 1, x != null ? x.toString() : JSONObject.NULL);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        setTime(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        params.put(parameterIndex - 1, x != null ? x.toString() : JSONObject.NULL);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        setTimestamp(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        params.put(parameterIndex - 1, x != null ? x : JSONObject.NULL);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        params.put(parameterIndex - 1, x != null ? x.toString() : JSONObject.NULL);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex, value);
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        params.put(parameterIndex - 1, x != null ? x.toString() : JSONObject.NULL);
    }

    // ---------------------------------------------------------------------------
    // Unsupported stream / LOB setters
    // ---------------------------------------------------------------------------

    @Override public void setAsciiStream(int i, InputStream x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setUnicodeStream(int i, InputStream x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setBinaryStream(int i, InputStream x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setCharacterStream(int i, Reader r, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setRef(int i, Ref x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setBlob(int i, Blob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setClob(int i, Clob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setArray(int i, Array x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setNClob(int i, NClob v) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setClob(int i, Reader r, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setBlob(int i, InputStream s, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setNClob(int i, Reader r, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setSQLXML(int i, SQLXML x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setAsciiStream(int i, InputStream x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setBinaryStream(int i, InputStream x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setCharacterStream(int i, Reader r, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setAsciiStream(int i, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setBinaryStream(int i, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setCharacterStream(int i, Reader r) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setNCharacterStream(int i, Reader v, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setNCharacterStream(int i, Reader v) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setClob(int i, Reader r) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setBlob(int i, InputStream s) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setNClob(int i, Reader r) throws SQLException { throw new SQLFeatureNotSupportedException(); }

    // ---------------------------------------------------------------------------
    // Metadata
    // ---------------------------------------------------------------------------

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return null;
    }

    // ---------------------------------------------------------------------------
    // Batch (not supported)
    // ---------------------------------------------------------------------------

    @Override
    public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch not supported");
    }

    // ---------------------------------------------------------------------------
    // Statement pass-through methods (sql argument ignored for PreparedStatement)
    // ---------------------------------------------------------------------------

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        throw new SQLException("Use executeQuery() without arguments on a PreparedStatement");
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new SQLException("Use executeUpdate() without arguments on a PreparedStatement");
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        throw new SQLException("Use execute() without arguments on a PreparedStatement");
    }

    @Override public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public int executeUpdate(String sql, int[] columnIndexes) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public int executeUpdate(String sql, String[] columnNames) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public boolean execute(String sql, int autoGeneratedKeys) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public boolean execute(String sql, int[] columnIndexes) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public boolean execute(String sql, String[] columnNames) throws SQLException { throw new SQLFeatureNotSupportedException(); }

    // ---------------------------------------------------------------------------
    // Standard Statement methods
    // ---------------------------------------------------------------------------

    @Override public void close() throws SQLException { }
    @Override public int getMaxFieldSize() throws SQLException { return 0; }
    @Override public void setMaxFieldSize(int max) throws SQLException { }
    @Override public int getMaxRows() throws SQLException { return 0; }
    @Override public void setMaxRows(int max) throws SQLException { }
    @Override public void setEscapeProcessing(boolean enable) throws SQLException { }
    @Override public int getQueryTimeout() throws SQLException { return 0; }
    @Override public void setQueryTimeout(int seconds) throws SQLException { }
    @Override public void cancel() throws SQLException { }
    @Override public SQLWarning getWarnings() throws SQLException { return null; }
    @Override public void clearWarnings() throws SQLException { }
    @Override public void setCursorName(String name) throws SQLException { }
    @Override public boolean getMoreResults() throws SQLException { return false; }
    @Override public void setFetchDirection(int direction) throws SQLException { }
    @Override public int getFetchDirection() throws SQLException { return 0; }
    @Override public void setFetchSize(int rows) throws SQLException { }
    @Override public int getFetchSize() throws SQLException { return 0; }
    @Override public int getResultSetConcurrency() throws SQLException { return 0; }
    @Override public int getResultSetType() throws SQLException { return ResultSet.TYPE_FORWARD_ONLY; }
    @Override public void addBatch(String sql) throws SQLException { }
    @Override public void clearBatch() throws SQLException { }
    @Override public int[] executeBatch() throws SQLException { return new int[0]; }
    @Override public Connection getConnection() throws SQLException { return null; }
    @Override public boolean getMoreResults(int current) throws SQLException { return false; }
    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        List<String> cols = Collections.singletonList("last_insert_rowid()");
        List<JSONObject> schema = Collections.singletonList(new JSONObject().put("type", "INTEGER"));
        List<List<Object>> rows = new ArrayList<>();
        if (lastInsertId > 0) {
            List<Object> row = new ArrayList<>();
            row.add(lastInsertId);
            rows.add(row);
        }
        return new D1ResultSet(apiToken, accountId, databaseId, rows, cols, schema);
    }
    @Override public int getResultSetHoldability() throws SQLException { return 0; }
    @Override public boolean isClosed() throws SQLException { return false; }
    @Override public void setPoolable(boolean poolable) throws SQLException { }
    @Override public boolean isPoolable() throws SQLException { return false; }
    @Override public void closeOnCompletion() throws SQLException { }
    @Override public boolean isCloseOnCompletion() throws SQLException { return false; }
    @Override public <T> T unwrap(Class<T> iface) throws SQLException { return null; }
    @Override public boolean isWrapperFor(Class<?> iface) throws SQLException { return false; }
}
