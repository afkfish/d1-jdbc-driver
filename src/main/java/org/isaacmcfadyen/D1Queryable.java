package org.isaacmcfadyen;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class TableNameFinderNoSchema extends TablesNamesFinder {
    @Override
    protected String extractTableName(Table table) {
        // Ignore schema prefix so "main.users" resolves to "users".
        return table.getName();
    }
}

public abstract class D1Queryable {

    D1Queryable(String apiToken, String accountId, String databaseId) {
        this.apiToken = apiToken;
        this.accountId = accountId;
        this.databaseId = databaseId;
    }

    protected String accountId;
    protected String databaseId;
    protected String apiToken;

    /** Rows affected by the last DML statement (-1 if not a DML). */
    protected int lastUpdateCount = -1;
    /** Last row ID inserted (0 if not an INSERT or no row was inserted). */
    protected long lastInsertId = 0;

    /**
     * Per-connection cache of PRAGMA table_info results keyed by table name.
     * Avoids the extra round-trip that generateResultSet() previously made for
     * every SELECT query.
     */
    private final Map<String, JSONArray> tableSchemaCache = new HashMap<>();

    // ---------------------------------------------------------------------------
    // Result-set construction helpers
    // ---------------------------------------------------------------------------

    /**
     * Build a D1ResultSet from raw JSON results using only the keys present in
     * the response. Used as a fallback when PRAGMA table_info is unavailable.
     */
    private D1ResultSet parseFromJson(JSONArray results) throws SQLException {
        if (results.isEmpty()) {
            return new D1ResultSet(
                    apiToken, accountId, databaseId,
                    Collections.emptyList(), Collections.emptyList(), Collections.emptyList()
            );
        }

        // Column names from the first row's keys.
        List<String> columnNames = new ArrayList<>(results.getJSONObject(0).keySet());

        // Map rows.
        List<List<Object>> rows = new ArrayList<>();
        for (int i = 0; i < results.length(); i++) {
            JSONObject row = results.getJSONObject(i);
            List<Object> rowList = new ArrayList<>();
            for (String col : columnNames) {
                rowList.add(row.get(col));
            }
            rows.add(rowList);
        }

        // Infer column schema from the first row's value types.
        List<JSONObject> columnSchema = new ArrayList<>();
        for (String col : columnNames) {
            JSONObject schema = new JSONObject();
            Object value = results.getJSONObject(0).get(col);
            if (value instanceof Integer || value instanceof Long) {
                schema.put("type", "INTEGER");
            } else if (value instanceof Double || value instanceof Float) {
                schema.put("type", "REAL");
            } else if (value instanceof Boolean) {
                schema.put("type", "BOOLEAN");
            } else if (value instanceof JSONObject || value instanceof JSONArray) {
                schema.put("type", "JSON");
            } else {
                schema.put("type", "TEXT");
            }
            columnSchema.add(schema);
        }

        return new D1ResultSet(apiToken, accountId, databaseId, rows, columnNames, columnSchema);
    }

    protected D1ResultSet generateResultSet(JSONObject json, String query) throws SQLException {
        JSONArray results = json.getJSONArray("results");

        try {
            // Extract the first table name from the query, ignoring any schema prefix.
            String tableName = new TableNameFinderNoSchema()
                    .getTableList(net.sf.jsqlparser.parser.CCJSqlParserUtil.parse(query))
                    .get(0);

            // Fetch column schema from PRAGMA table_info (cached per table).
            JSONArray columnSchemaArray = tableSchemaCache.get(tableName);
            if (columnSchemaArray == null) {
                String quotedTable = "\"" + tableName.replace("\"", "\"\"") + "\"";
                JSONObject columnSchemaQuery = queryDatabase(
                        "PRAGMA table_info(" + quotedTable + ")",
                        null
                );
                columnSchemaArray = columnSchemaQuery.getJSONArray("results");
                tableSchemaCache.put(tableName, columnSchemaArray);
            }

            List<String> columnNames = new ArrayList<>();
            List<JSONObject> columnSchema = new ArrayList<>();
            for (int i = 0; i < columnSchemaArray.length(); i++) {
                JSONObject col = columnSchemaArray.getJSONObject(i);
                columnNames.add(col.getString("name"));
                columnSchema.add(col);
            }

            // Map result rows; D1 returns each row as a JSON object keyed by column name.
            List<List<Object>> rows = new ArrayList<>();
            for (int i = 0; i < results.length(); i++) {
                JSONObject row = results.getJSONObject(i);
                List<Object> rowList = new ArrayList<>();
                for (String colName : columnNames) {
                    // Use opt() so missing keys (aliased columns, expressions) become null.
                    rowList.add(row.has(colName) ? row.get(colName) : JSONObject.NULL);
                }
                rows.add(rowList);
            }

            D1ResultSet resultSet = new D1ResultSet(apiToken, accountId, databaseId, rows, columnNames, columnSchema);
            resultSet.setTableName(tableName);
            return resultSet;

        } catch (Exception e) {
            // Fall back to pure-JSON parsing.
            // Required for queries without a resolvable table name (subqueries,
            // expressions, PRAGMA results, etc.).
            System.err.println("Falling back to JSON-based schema parsing for: " + query);
            return parseFromJson(results);
        }
    }

    // ---------------------------------------------------------------------------
    // Internal table filtering
    // ---------------------------------------------------------------------------

    /**
     * Returns true for Cloudflare-internal and SQLite-system tables that should
     * never be surfaced to schema browsers.
     */
    static boolean isInternalTable(String name) {
        if (name == null) return false;
        if (name.startsWith("_cf_")) return true;
        // SQLite system tables
        return name.equals("sqlite_master")
                || name.equals("sqlite_schema")
                || name.equals("sqlite_temp_schema")
                || name.equals("sqlite_sequence")
                || name.equals("sqlite_stat1")
                || name.equals("sqlite_stat2")
                || name.equals("sqlite_stat3")
                || name.equals("sqlite_stat4");
    }

    /**
     * Strip internal/system tables from a {@code PRAGMA table_list} result so
     * that schema browsers (e.g. DataGrip) never try to introspect them.
     */
    private JSONObject filterTableList(JSONObject result) {
        JSONArray original = result.getJSONArray("results");
        JSONArray filtered = new JSONArray();
        for (int i = 0; i < original.length(); i++) {
            JSONObject row = original.getJSONObject(i);
            if (!isInternalTable(row.optString("name"))) {
                filtered.put(row);
            }
        }
        return new JSONObject(result.toMap()).put("results", filtered);
    }

    // ---------------------------------------------------------------------------
    // PRAGMA mocking
    // ---------------------------------------------------------------------------

    /**
     * Handle PRAGMA commands that D1 does not expose to users by returning
     * a pre-built response so tools like DataGrip don't error out during
     * introspection.
     */
    private JSONObject preProcessQuery(String sql) {
        String lower = sql.trim().replaceAll(";\\s*$", "").toLowerCase();

        if (lower.equals("pragma database_list")) {
            return new JSONObject().put("results",
                    new JSONArray().put(new JSONObject()
                            .put("seq", 0)
                            .put("name", "main")
                            .put("file", "main")));
        }

        // D1 blocks all PRAGMA operations on its internal _cf_KV table.
        if (lower.contains("_cf_kv")) {
            return new JSONObject().put("results", new JSONArray());
        }

        if (lower.equals("pragma collation_list")) {
            return new JSONObject().put("results",
                    new JSONArray()
                            .put(new JSONObject().put("seq", 0).put("name", "decimal"))
                            .put(new JSONObject().put("seq", 1).put("name", "uint"))
                            .put(new JSONObject().put("seq", 2).put("name", "RTRIM"))
                            .put(new JSONObject().put("seq", 3).put("name", "NOCASE"))
                            .put(new JSONObject().put("seq", 4).put("name", "BINARY")));
        }

        // D1 only exposes a curated subset of SQLite PRAGMAs (see
        // https://developers.cloudflare.com/d1/sql-api/sql-statements/).
        // The ones below are not in that list; we return sensible static
        // responses so that JDBC clients don't receive unexpected errors.

        if (lower.equals("pragma encoding")) {
            return new JSONObject().put("results",
                    new JSONArray().put(new JSONObject().put("encoding", "UTF-8")));
        }

        if (lower.equals("pragma journal_mode") || lower.equals("pragma main.journal_mode")) {
            // D1 manages its own write-ahead log; surface "wal" to clients.
            return new JSONObject().put("results",
                    new JSONArray().put(new JSONObject().put("journal_mode", "wal")));
        }

        if (lower.equals("pragma page_size") || lower.equals("pragma main.page_size")) {
            return new JSONObject().put("results",
                    new JSONArray().put(new JSONObject().put("page_size", 4096)));
        }

        if (lower.equals("pragma integrity_check") || lower.equals("pragma main.integrity_check")) {
            // D1 exposes quick_check but not integrity_check; return "ok" so
            // tools that send this pragma don't see an error.
            return new JSONObject().put("results",
                    new JSONArray().put(new JSONObject().put("integrity_check", "ok")));
        }

        if (lower.equals("pragma user_version") || lower.equals("pragma main.user_version")) {
            return new JSONObject().put("results",
                    new JSONArray().put(new JSONObject().put("user_version", 0)));
        }

        if (lower.equals("pragma schema_version") || lower.equals("pragma main.schema_version")) {
            return new JSONObject().put("results",
                    new JSONArray().put(new JSONObject().put("schema_version", 0)));
        }

        if (lower.equals("pragma freelist_count") || lower.equals("pragma main.freelist_count")) {
            return new JSONObject().put("results",
                    new JSONArray().put(new JSONObject().put("freelist_count", 0)));
        }

        if (lower.equals("pragma compile_options")) {
            // Return an empty list; D1's compile options are not user-visible.
            return new JSONObject().put("results", new JSONArray());
        }

        return null;
    }

    // ---------------------------------------------------------------------------
    // Batch query
    // ---------------------------------------------------------------------------

    /**
     * Execute multiple SQL statements in a single D1 API call using the
     * {@code batch} request format. Returns the number of rows changed by each
     * statement in the same order.
     */
    protected int[] executeBatchQuery(JSONArray batch) throws SQLException {
        try {
            URL url = new URL("https://api.cloudflare.com/client/v4/accounts/"
                    + accountId + "/d1/database/" + databaseId + "/query");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Authorization", "Bearer " + apiToken);
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Connection", "keep-alive");
            connection.setDoOutput(true);

            JSONObject body = new JSONObject().put("batch", batch);
            byte[] bodyBytes = body.toString().getBytes(StandardCharsets.UTF_8);
            connection.getOutputStream().write(bodyBytes);
            connection.getOutputStream().flush();
            connection.getOutputStream().close();

            if (connection.getResponseCode() != 200) {
                int statusCode = connection.getResponseCode();
                InputStream errorStream = connection.getErrorStream();
                String errorBody = errorStream != null
                        ? new BufferedReader(new InputStreamReader(errorStream, StandardCharsets.UTF_8))
                                .lines().collect(Collectors.joining("\n"))
                        : connection.getResponseMessage();
                throw new SQLException("HTTP " + statusCode + ": " + errorBody);
            }

            String result = new BufferedReader(
                    new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))
                    .lines().collect(Collectors.joining("\n"));
            JSONObject json = new JSONObject(result);

            if (!json.getBoolean("success")) {
                JSONArray errors = json.optJSONArray("errors");
                if (errors != null && !errors.isEmpty()) {
                    throw new SQLException(errors.getJSONObject(0).optString("message", "Batch failed"));
                }
                throw new SQLException("Batch query failed");
            }

            JSONArray results = json.getJSONArray("result");
            int[] updateCounts = new int[results.length()];
            for (int i = 0; i < results.length(); i++) {
                JSONObject res = results.getJSONObject(i);
                if (!res.optBoolean("success", true)) {
                    updateCounts[i] = Statement.EXECUTE_FAILED;
                } else {
                    JSONObject meta = res.optJSONObject("meta");
                    updateCounts[i] = meta != null ? Math.max(0, meta.optInt("changes", 0)) : 0;
                    // Update shared meta fields to the last statement's values.
                    if (meta != null && i == results.length() - 1) {
                        lastUpdateCount = meta.optInt("changes", -1);
                        lastInsertId = meta.optLong("last_row_id", 0);
                    }
                }
            }
            return updateCounts;

        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    // ---------------------------------------------------------------------------
    // HTTP query
    // ---------------------------------------------------------------------------

    /**
     * Returns true if {@code sql} is a read-only statement (SELECT, EXPLAIN,
     * or a WITH … SELECT CTE). These may be safely retried on transient errors.
     */
    private static boolean isReadOnly(String sql) {
        String trimmed = sql.trim().toLowerCase();
        return trimmed.startsWith("select")
                || trimmed.startsWith("explain")
                || trimmed.startsWith("with");
    }

    protected JSONObject queryDatabase(String sql, JSONArray params) throws SQLException {
        // Short-circuit for mocked PRAGMA commands.
        JSONObject mocked = preProcessQuery(sql);
        if (mocked != null) {
            return mocked;
        }

        // Invalidate schema cache on DDL so column type changes are reflected.
        String trimmedUpper = sql.trim().toUpperCase();
        if (trimmedUpper.startsWith("CREATE") || trimmedUpper.startsWith("DROP")
                || trimmedUpper.startsWith("ALTER")) {
            tableSchemaCache.clear();
        }

        boolean readOnly = isReadOnly(sql);
        // Up to 2 retries for read-only queries; 1 attempt total for writes.
        int maxAttempts = readOnly ? 3 : 1;
        int delayMs = 500;

        SQLException lastException = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return executeHttpQuery(sql, params);
            } catch (SQLException e) {
                lastException = e;
                String msg = e.getMessage();
                // Retry on 429 (rate-limit) and transient 5xx for read-only queries.
                boolean retryable = readOnly
                        && (msg != null && (msg.startsWith("HTTP 429") || msg.startsWith("HTTP 5")));
                if (!retryable || attempt == maxAttempts) {
                    throw e;
                }
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("Interrupted during retry", ie);
                }
                delayMs *= 2; // exponential back-off
            }
        }
        throw lastException; // unreachable but satisfies compiler
    }

    private JSONObject executeHttpQuery(String sql, JSONArray params) throws SQLException {
        try {
            URL url = new URL("https://api.cloudflare.com/client/v4/accounts/"
                    + accountId + "/d1/database/" + databaseId + "/query");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Authorization", "Bearer " + apiToken);
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Connection", "keep-alive");
            connection.setDoOutput(true);

            JSONObject body = new JSONObject();
            body.put("sql", sql);
            if (params != null && !params.isEmpty()) {
                body.put("params", params);
            }
            byte[] bodyBytes = body.toString().getBytes(StandardCharsets.UTF_8);
            connection.getOutputStream().write(bodyBytes);
            connection.getOutputStream().flush();
            connection.getOutputStream().close();

            if (connection.getResponseCode() != 200) {
                int statusCode = connection.getResponseCode();
                InputStream errorStream = connection.getErrorStream();
                if (errorStream != null) {
                    String errorBody = new BufferedReader(
                            new InputStreamReader(errorStream, StandardCharsets.UTF_8))
                            .lines().collect(Collectors.joining("\n"));
                    try {
                        JSONObject errorJson = new JSONObject(errorBody);
                        JSONArray errors = errorJson.optJSONArray("errors");
                        if (errors != null && errors.length() > 0) {
                            String msg = errors.getJSONObject(0).optString("message", errorBody);
                            throw new SQLException("HTTP " + statusCode + ": " + msg);
                        }
                    } catch (SQLException e) {
                        throw e;
                    } catch (Exception ignored) {
                    }
                    throw new SQLException("HTTP " + statusCode + ": " + errorBody);
                }
                throw new SQLException("HTTP " + statusCode + ": " + connection.getResponseMessage());
            }

            InputStream inputStream = connection.getInputStream();
            String result = new BufferedReader(
                    new InputStreamReader(inputStream, StandardCharsets.UTF_8))
                    .lines().collect(Collectors.joining("\n"));
            JSONObject json = new JSONObject(result);

            if (!json.getBoolean("success")) {
                JSONArray errors = json.optJSONArray("errors");
                if (errors != null && errors.length() > 0) {
                    throw new SQLException(errors.getJSONObject(0).optString("message", "Unknown error"));
                }
                throw new SQLException("Query failed");
            }

            JSONObject resultObj = json.getJSONArray("result").getJSONObject(0);

            // Capture DML meta: rows changed and last inserted row ID.
            JSONObject meta = resultObj.optJSONObject("meta");
            if (meta != null) {
                lastUpdateCount = meta.optInt("changes", -1);
                lastInsertId = meta.optLong("last_row_id", 0);
            }

            // Strip internal/system tables from PRAGMA table_list results so
            // that schema browsers never discover or attempt to introspect them.
            // Strip trailing semicolons before matching to handle user-typed queries.
            String trimmedLower = sql.trim().replaceAll(";\\s*$", "").toLowerCase();
            if (trimmedLower.equals("pragma table_list")
                    || trimmedLower.matches("pragma\\s+\"?\\w+\"?\\.table_list")) {
                resultObj = filterTableList(resultObj);
            }

            return resultObj;

        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }
        // Intentionally NOT calling connection.disconnect() so the JVM's
        // built-in HTTP keep-alive pool can reuse the underlying TCP socket
        // for subsequent requests to the same Cloudflare endpoint.
    }
}
