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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

            // Fetch column schema from PRAGMA table_info.
            JSONObject columnSchemaQuery = queryDatabase(
                    "PRAGMA table_info(" + tableName + ")",
                    null
            );
            JSONArray columnSchemaArray = columnSchemaQuery.getJSONArray("results");

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
    // PRAGMA mocking
    // ---------------------------------------------------------------------------

    /**
     * Handle PRAGMA commands that D1 does not expose to users by returning
     * a pre-built response so tools like DataGrip don't error out during
     * introspection.
     */
    private JSONObject preProcessQuery(String sql) {
        String lower = sql.trim().toLowerCase();

        if (lower.equals("pragma database_list")) {
            return new JSONObject().put("results",
                    new JSONArray().put(new JSONObject()
                            .put("seq", 0)
                            .put("name", "main")
                            .put("file", "main")));
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

        return null;
    }

    // ---------------------------------------------------------------------------
    // HTTP query
    // ---------------------------------------------------------------------------

    protected JSONObject queryDatabase(String sql, JSONArray params) throws SQLException {
        // Short-circuit for mocked PRAGMA commands.
        JSONObject mocked = preProcessQuery(sql);
        if (mocked != null) {
            return mocked;
        }

        HttpURLConnection connection = null;
        try {
            URL url = new URL("https://api.cloudflare.com/client/v4/accounts/"
                    + accountId + "/d1/database/" + databaseId + "/query");
            connection = (HttpURLConnection) url.openConnection();
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

            return json.getJSONArray("result").getJSONObject(0);

        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }
}
