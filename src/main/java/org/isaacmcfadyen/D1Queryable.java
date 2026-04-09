package org.isaacmcfadyen;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.json.JSONObject;
import org.json.JSONArray;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.stream.Collectors;

public abstract class D1Queryable {

    D1Queryable(String ApiKey, String AccountId, String DatabaseUuid) {
        this.ApiKey = ApiKey;
        this.AccountId = AccountId;
        this.DatabaseUuid = DatabaseUuid;
    }

    protected String AccountId;
    protected String DatabaseUuid;
    protected String ApiKey;

    protected D1ResultSet generateResultSet(JSONObject json, String query) throws SQLException {
        // Parse the table name.
        String tableName = null;
        try {
            Statement statement = CCJSqlParserUtil.parse(query);
            Select selectStatement = (Select) statement;
            TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
            tableName = tablesNamesFinder.getTableList(selectStatement).get(0);
        } catch (Exception e) {
            System.out.println("Error parsing query, perhaps it's not a select query? Returning empty response.");
            JSONArray columnSchema = new JSONArray();
            return new D1ResultSet(ApiKey, AccountId, DatabaseUuid, new ArrayList<>(), new ArrayList<>(), columnSchema);
        }

        JSONArray results = json.getJSONArray("results");
        if (results.length() == 0) {
            JSONArray columnSchema = new JSONArray();
            return new D1ResultSet(ApiKey, AccountId, DatabaseUuid, new ArrayList<>(), new ArrayList<>(), columnSchema);
        }

        // Run a query to get the column schema.
        JSONObject columnSchema = queryDatabase("PRAGMA table_info(" + tableName + ")");
        JSONArray columnSchemaArray = columnSchema.getJSONArray("results");

        ArrayList<String> columnNames = new ArrayList<>();
        // Map the name key in the columnSchemas to the column names.
        for (int i = 0; i < columnSchemaArray.length(); i++) {
            JSONObject columnSchemaObject = columnSchemaArray.getJSONObject(i);
            columnNames.add(columnSchemaObject.getString("name"));
        }

        // Map the rows to a list, making sure they are in the same order.
        ArrayList<ArrayList<Object>> rows = new ArrayList<>();
        for (int i = 0; i < results.length(); i++) {
            JSONObject row = results.getJSONObject(i);
            ArrayList<Object> rowValues = new ArrayList<>();
            for (int j = 0; j < columnNames.size(); j++) {
                rowValues.add(row.get(columnNames.get(j)));
            }
            rows.add(rowValues);
        }

        return new D1ResultSet(ApiKey, AccountId, DatabaseUuid, columnNames, rows, columnSchemaArray);
    }

    protected JSONObject queryDatabase(String sql) throws SQLException {
        HttpURLConnection connection = null;
        try {
            URL url = new URL("https://api.cloudflare.com/client/v4/accounts/" + AccountId + "/d1/database/" + DatabaseUuid + "/query");
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Authorization", "Bearer " + ApiKey);
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Accept", "application/json");
            connection.setDoOutput(true);

            JSONObject query = new JSONObject();
            query.put("sql", sql);
            connection.getOutputStream().write(query.toString().getBytes());
            connection.getOutputStream().flush();
            connection.getOutputStream().close();

            if (connection.getResponseCode() != 200) {
                int statusCode = connection.getResponseCode();
                InputStream errorStream = connection.getErrorStream();
                if (errorStream != null) {
                    String errorBody = new BufferedReader(new InputStreamReader(errorStream, StandardCharsets.UTF_8))
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
                    } catch (Exception ignored) {}
                    throw new SQLException("HTTP " + statusCode + ": " + errorBody);
                }
                throw new SQLException("HTTP " + statusCode + ": " + connection.getResponseMessage());
            }

            InputStream inputStream = connection.getInputStream();
            String result = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
                    .lines().collect(Collectors.joining("\n"));
            JSONObject json = new JSONObject(result);

            if (!json.getBoolean("success")) {
                JSONArray errors = json.optJSONArray("errors");
                if (errors != null && errors.length() > 0) {
                    throw new SQLException(errors.getJSONObject(0).optString("message", "Unknown error"));
                }
                throw new SQLException("Query failed");
            }

            connection.disconnect();

            return json.getJSONArray("result").getJSONObject(0);
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }
}
