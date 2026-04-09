package org.isaacmcfadyen;

import org.json.JSONObject;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Objects;

public class D1ResultSetMetaData extends D1Queryable implements ResultSetMetaData {
    private final List<String> columnNames;
    private final List<List<Object>> rows;
    private final List<JSONObject> columnSchema;

    D1ResultSetMetaData(
            String apiToken,
            String accountId,
            String databaseId,
            List<String> columns,
            List<List<Object>> rows,
            List<JSONObject> columnSchema
    ) throws SQLException {
        super(apiToken, accountId, databaseId);
        this.columnNames = columns;
        this.rows = rows;
        this.columnSchema = columnSchema;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columnNames.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        String type = columnSchema.get(column - 1).optString("type", "TEXT");
        return type.equals("TEXT") || type.contains("CHAR") || type.contains("CLOB");
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        JSONObject schema = columnSchema.get(column - 1);
        if (schema.has("notnull")) {
            return schema.getInt("notnull") == 0 ? columnNullable : columnNoNulls;
        }
        return columnNullableUnknown;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        String type = columnSchema.get(column - 1).optString("type", "TEXT");
        return !Objects.equals(type, "TEXT")
                && !type.contains("CHAR")
                && !type.contains("BLOB")
                && !type.isEmpty();
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        int longest = 0;
        for (List<Object> row : rows) {
            Object val = row.get(column - 1);
            if (val != null && !JSONObject.NULL.equals(val)) {
                int len = val.toString().length();
                if (len > longest) longest = len;
            }
        }
        return longest;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return columnNames.get(column - 1);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return columnNames.get(column - 1);
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return null;
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        if (rows.isEmpty()) return 0;
        Object value = rows.get(0).get(column - 1);
        if (value instanceof Double) {
            String str = value.toString();
            int dot = str.indexOf('.');
            return dot >= 0 ? str.length() - dot - 1 : 0;
        }
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return null;
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return null;
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        String type = columnSchema.get(column - 1).optString("type", "TEXT").toUpperCase();
        if (type.contains("CHAR") || type.contains("CLOB") || type.contains("TEXT")) {
            return Types.VARCHAR;
        } else if (type.contains("INT")) {
            return Types.INTEGER;
        } else if (type.contains("REAL") || type.contains("DOUB") || type.contains("FLOA")) {
            return Types.DOUBLE;
        } else if (type.contains("BOOL")) {
            return Types.BOOLEAN;
        } else if (type.contains("BLOB") || type.isEmpty()) {
            return Types.BLOB;
        } else {
            return Types.NUMERIC;
        }
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return columnSchema.get(column - 1).optString("type", "TEXT");
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return true;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        String type = columnSchema.get(column - 1).optString("type", "TEXT").toUpperCase();
        if (type.contains("INT")) return Long.class.getName();
        if (type.contains("REAL") || type.contains("DOUB") || type.contains("FLOA")) return Double.class.getName();
        if (type.contains("BOOL")) return Boolean.class.getName();
        return String.class.getName();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
