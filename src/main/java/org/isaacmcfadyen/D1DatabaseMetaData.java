package org.isaacmcfadyen;

import org.json.JSONObject;
import org.json.JSONArray;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class D1DatabaseMetaData extends D1Queryable implements DatabaseMetaData {
    private final D1Connection parentConnection;

    D1DatabaseMetaData(String apiToken, String accountId, String databaseId, D1Connection connection) {
        super(apiToken, accountId, databaseId);
        this.parentConnection = connection;
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public String getURL() throws SQLException {
        return String.format("jdbc:d1://%s", databaseId);
    }

    @Override
    public String getUserName() throws SQLException {
        throw new SQLException("Not implemented: getUserName()");
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "SQLite";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return "3.45.1";
    }

    @Override
    public String getDriverName() throws SQLException {
        return "D1 JDBC Driver";
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return "2.0";
    }

    @Override
    public int getDriverMajorVersion() {
        return 2;
    }

    @Override
    public int getDriverMinorVersion() {
        return 0;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return true;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "\"";
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return "";
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return "ABS,MAX,MIN,ROUND,UPPER,LOWER,LENGTH,SUBSTR,REPLACE,TRIM,LTRIM,RTRIM,HEX,QUOTE,RANDOMBLOB,ZEROBLOB,TYPEOF,LAST_INSERT_ROWID,CHANGES,TOTAL_CHANGES,COALESCE,NULLIF,IFNULL,IIF";
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return "LENGTH,LOWER,LTRIM,REPLACE,RTRIM,SUBSTR,TRIM,UPPER,CHAR,GLOB,LIKE,HEX,QUOTE,SOUNDEX,UNICODE,INSTR";
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return "LAST_INSERT_ROWID,CHANGES,TOTAL_CHANGES,SQLITE_VERSION,SQLITE_SOURCE_ID";
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return "DATE,TIME,DATETIME,JULIANDAY,STRFTIME";
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "schema";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "not_implemented";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "catalog";
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return true;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return 0;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        throw new SQLException("Not implemented: getProcedures(String catalog, String schemaPattern, String procedureNamePattern)");
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        throw new SQLException("Not implemented: getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern)");
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        JSONObject json = queryDatabase("PRAGMA table_list", null);
        JSONArray tables = json.getJSONArray("results");

        List<String> columnNames = new ArrayList<>();
        columnNames.add("TABLE_CAT");
        columnNames.add("TABLE_SCHEM");
        columnNames.add("TABLE_NAME");
        columnNames.add("TABLE_TYPE");

        List<List<Object>> rows = new ArrayList<>();
        for (int i = 0; i < tables.length(); i++) {
            JSONObject table = tables.getJSONObject(i);
            String tableName = table.getString("name");

            // Hide Cloudflare-internal and SQLite system tables from table browsers.
            if (D1Queryable.isInternalTable(tableName)) continue;

            List<Object> row = new ArrayList<>();
            row.add(null);
            row.add(table.getString("schema"));
            row.add(tableName);
            row.add(table.getString("type").toUpperCase());
            rows.add(row);
        }

        JSONObject stringType = new JSONObject().put("type", "TEXT");
        List<JSONObject> columnSchema = new ArrayList<>();
        columnSchema.add(stringType);
        columnSchema.add(stringType);
        columnSchema.add(stringType);
        columnSchema.add(stringType);

        return new D1ResultSet(apiToken, accountId, databaseId, rows, columnNames, columnSchema);
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        List<String> columnNames = new ArrayList<>();
        columnNames.add("TABLE_SCHEM");
        columnNames.add("TABLE_CATALOG");

        List<Object> row = new ArrayList<>();
        row.add(null);
        row.add(null);
        List<List<Object>> rows = new ArrayList<>();
        rows.add(row);

        JSONObject stringType = new JSONObject().put("type", "TEXT");
        List<JSONObject> columnSchema = new ArrayList<>();
        columnSchema.add(stringType);
        columnSchema.add(stringType);

        return new D1ResultSet(apiToken, accountId, databaseId, rows, columnNames, columnSchema);
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        List<String> columnNames = new ArrayList<>();
        columnNames.add("TABLE_TYPE");

        List<Object> row = new ArrayList<>();
        row.add("TABLE");
        List<List<Object>> rows = new ArrayList<>();
        rows.add(row);

        JSONObject stringType = new JSONObject().put("type", "TEXT");
        List<JSONObject> columnSchema = new ArrayList<>();
        columnSchema.add(stringType);

        return new D1ResultSet(apiToken, accountId, databaseId, rows, columnNames, columnSchema);
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        if (tableNamePattern == null || tableNamePattern.isEmpty()) {
            return null;
        }
        String tableName = tableNamePattern.replaceAll("\\\\", "");
        String quotedTable = "\"" + tableName.replace("\"", "\"\"") + "\"";
        JSONObject json = queryDatabase("PRAGMA table_info(" + quotedTable + ")", null);
        JSONArray results = json.getJSONArray("results");

        List<String> columnNames = new ArrayList<>();
        columnNames.add("TABLE_CAT");
        columnNames.add("TABLE_SCHEM");
        columnNames.add("TABLE_NAME");
        columnNames.add("COLUMN_NAME");
        columnNames.add("DATA_TYPE");
        columnNames.add("TYPE_NAME");
        columnNames.add("COLUMN_SIZE");
        columnNames.add("BUFFER_LENGTH");
        columnNames.add("DECIMAL_DIGITS");
        columnNames.add("NUM_PREC_RADIX");
        columnNames.add("NULLABLE");
        columnNames.add("REMARKS");
        columnNames.add("COLUMN_DEF");
        columnNames.add("SQL_DATA_TYPE");
        columnNames.add("SQL_DATETIME_SUB");
        columnNames.add("CHAR_OCTET_LENGTH");
        columnNames.add("ORDINAL_POSITION");
        columnNames.add("IS_NULLABLE");
        columnNames.add("SCOPE_CATLOG");
        columnNames.add("SCOPE_SCHEMA");
        columnNames.add("SCOPE_TABLE");
        columnNames.add("SOURCE_DATA_TYPE");
        columnNames.add("IS_AUTOINCREMENT");
        columnNames.add("IS_GENERATEDCOLUMN");

        List<List<Object>> rows = new ArrayList<>();
        for (int i = 0; i < results.length(); i++) {
            JSONObject column = results.getJSONObject(i);
            List<Object> row = new ArrayList<>();
            String type = column.getString("type").toUpperCase();

            row.add(null); // TABLE_CAT
            row.add(null); // TABLE_SCHEM
            row.add(tableName); // TABLE_NAME
            row.add(column.getString("name")); // COLUMN_NAME

            // DATA_TYPE
            if (type.contains("CHAR") || type.contains("CLOB") || type.contains("TEXT")) {
                row.add(Types.VARCHAR);
            } else if (type.contains("INT")) {
                row.add(Types.INTEGER);
            } else if (type.contains("REAL") || type.contains("DOUB") || type.contains("FLOA")) {
                row.add(Types.DOUBLE);
            } else if (type.contains("BOOL")) {
                row.add(Types.BOOLEAN);
            } else if (type.contains("BLOB") || type.isEmpty()) {
                row.add(Types.BLOB);
            } else {
                row.add(Types.NUMERIC);
            }

            row.add(column.getString("type")); // TYPE_NAME
            row.add(null); // COLUMN_SIZE
            row.add(null); // BUFFER_LENGTH
            row.add(null); // DECIMAL_DIGITS
            row.add(null); // NUM_PREC_RADIX
            row.add(column.getInt("notnull") == 0 ? columnNullable : columnNoNulls); // NULLABLE
            row.add(null); // REMARKS

            // COLUMN_DEF
            Object dflt = column.get("dflt_value");
            if (JSONObject.NULL.equals(dflt)) {
                row.add(null);
            } else if (type.contains("REAL") || type.contains("DOUB") || type.contains("FLOA") || type.contains("INT")) {
                row.add(dflt.toString());
            } else {
                row.add("'" + dflt + "'");
            }

            row.add(null); // SQL_DATA_TYPE
            row.add(null); // SQL_DATETIME_SUB
            row.add(null); // CHAR_OCTET_LENGTH
            row.add(i + 1); // ORDINAL_POSITION
            row.add(column.getInt("notnull") == 0 ? "YES" : "NO"); // IS_NULLABLE
            row.add(null); // SCOPE_CATLOG
            row.add(null); // SCOPE_SCHEMA
            row.add(null); // SCOPE_TABLE
            row.add(null); // SOURCE_DATA_TYPE
            row.add(""); // IS_AUTOINCREMENT
            row.add(""); // IS_GENERATEDCOLUMN

            rows.add(row);
        }

        JSONObject str = new JSONObject().put("type", "TEXT");
        JSONObject num = new JSONObject().put("type", "INTEGER");
        List<JSONObject> columnSchema = new ArrayList<>();
        for (int i = 0; i < 24; i++) {
            // columns 4 (DATA_TYPE), 10 (NULLABLE), 6-9 (sizes), 13-16 (SQL_ fields), 21 (SOURCE_DATA_TYPE) are integers
            boolean isInt = (i == 4 || i == 10 || (i >= 6 && i <= 9) || (i >= 13 && i <= 16) || i == 21);
            columnSchema.add(isInt ? num : str);
        }

        return new D1ResultSet(apiToken, accountId, databaseId, rows, columnNames, columnSchema);
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        throw new SQLException("Not implemented: getColumnPrivileges()");
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        throw new SQLException("Not implemented: getTablePrivileges()");
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        throw new SQLException("Not implemented: getBestRowIdentifier()");
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        throw new SQLException("Not implemented: getVersionColumns()");
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        String quotedTable = "\"" + table.replace("\"", "\"\"") + "\"";
        JSONObject results = queryDatabase("PRAGMA table_info(" + quotedTable + ")", null);
        JSONArray columns = results.getJSONArray("results");

        JSONObject primaryKeyColumn = null;
        for (int i = 0; i < columns.length(); i++) {
            JSONObject column = columns.getJSONObject(i);
            if (column.getInt("pk") == 1) {
                primaryKeyColumn = column;
                break;
            }
        }

        if (primaryKeyColumn == null) {
            return null;
        }

        List<String> columnNames = new ArrayList<>();
        columnNames.add("TABLE_CAT");
        columnNames.add("TABLE_SCHEM");
        columnNames.add("TABLE_NAME");
        columnNames.add("COLUMN_NAME");
        columnNames.add("KEY_SEQ");
        columnNames.add("PK_NAME");

        List<Object> row = new ArrayList<>();
        row.add(null);
        row.add(null);
        row.add(table);
        row.add(primaryKeyColumn.getString("name"));
        row.add(1);
        row.add(primaryKeyColumn.getString("name"));
        List<List<Object>> rows = new ArrayList<>();
        rows.add(row);

        JSONObject str = new JSONObject().put("type", "TEXT");
        JSONObject num = new JSONObject().put("type", "INTEGER");
        List<JSONObject> columnSchema = new ArrayList<>();
        columnSchema.add(str); // TABLE_CAT
        columnSchema.add(str); // TABLE_SCHEM
        columnSchema.add(str); // TABLE_NAME
        columnSchema.add(str); // COLUMN_NAME
        columnSchema.add(num); // KEY_SEQ
        columnSchema.add(str); // PK_NAME

        return new D1ResultSet(apiToken, accountId, databaseId, rows, columnNames, columnSchema);
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return getCrossReference(null, null, null, catalog, schema, table);
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return getCrossReference(null, null, null, catalog, schema, table);
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        List<String> columnNames = new ArrayList<>();
        columnNames.add("PKTABLE_CAT");
        columnNames.add("PKTABLE_SCHEM");
        columnNames.add("PKTABLE_NAME");
        columnNames.add("PKCOLUMN_NAME");
        columnNames.add("FKTABLE_CAT");
        columnNames.add("FKTABLE_SCHEM");
        columnNames.add("FKTABLE_NAME");
        columnNames.add("FKCOLUMN_NAME");
        columnNames.add("KEY_SEQ");
        columnNames.add("UPDATE_RULE");
        columnNames.add("DELETE_RULE");
        columnNames.add("FK_NAME");
        columnNames.add("PK_NAME");

        JSONObject str = new JSONObject().put("type", "TEXT");
        JSONObject num = new JSONObject().put("type", "INTEGER");
        List<JSONObject> columnSchema = new ArrayList<>();
        columnSchema.add(str); // PKTABLE_CAT
        columnSchema.add(str); // PKTABLE_SCHEM
        columnSchema.add(str); // PKTABLE_NAME
        columnSchema.add(str); // PKCOLUMN_NAME
        columnSchema.add(str); // FKTABLE_CAT
        columnSchema.add(str); // FKTABLE_SCHEM
        columnSchema.add(str); // FKTABLE_NAME
        columnSchema.add(str); // FKCOLUMN_NAME
        columnSchema.add(num); // KEY_SEQ
        columnSchema.add(num); // UPDATE_RULE
        columnSchema.add(num); // DELETE_RULE
        columnSchema.add(str); // FK_NAME
        columnSchema.add(str); // PK_NAME

        JSONObject ruleMap = new JSONObject();
        ruleMap.put("NO ACTION", DatabaseMetaData.importedKeyNoAction);
        ruleMap.put("CASCADE", DatabaseMetaData.importedKeyCascade);
        ruleMap.put("SET NULL", DatabaseMetaData.importedKeySetNull);
        ruleMap.put("SET DEFAULT", DatabaseMetaData.importedKeySetDefault);
        ruleMap.put("RESTRICT", DatabaseMetaData.importedKeyRestrict);

        String quotedFk = "\"" + foreignTable.replace("\"", "\"\"") + "\"";
        JSONObject results = queryDatabase("PRAGMA foreign_key_list(" + quotedFk + ")", null);
        JSONArray fkList = results.getJSONArray("results");
        List<List<Object>> rows = new ArrayList<>();

        for (int i = 0; i < fkList.length(); i++) {
            JSONObject fkItem = fkList.getJSONObject(i);
            List<Object> row = new ArrayList<>();
            row.add(null);
            row.add(null);
            row.add(fkItem.get("table"));
            row.add(fkItem.get("to"));
            row.add(null);
            row.add(null);
            row.add(foreignTable);
            row.add(fkItem.get("from"));
            row.add(fkItem.get("seq"));
            row.add(ruleMap.get(fkItem.get("on_update").toString()));
            row.add(ruleMap.get(fkItem.get("on_delete").toString()));
            row.add(foreignTable + "_" + fkItem.get("id") + "_" + fkItem.get("seq"));
            row.add(null);
            rows.add(row);
        }

        return new D1ResultSet(apiToken, accountId, databaseId, rows, columnNames, columnSchema);
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        throw new SQLException("Not implemented: getTypeInfo()");
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        throw new SQLException("Not implemented: getIndexInfo()");
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        throw new SQLException("Not implemented: getUDTs()");
    }

    @Override
    public Connection getConnection() throws SQLException {
        return parentConnection;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return true;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        return null;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 2;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return 0;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return null;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
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
