package org.isaacmcfadyen;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class D1Driver implements Driver {

    static {
        try {
            DriverManager.registerDriver(new D1Driver());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to register D1Driver", e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        // Expected URL: jdbc:d1://<database-id>
        String[] parts = url.split("://", 2);
        if (parts.length < 2 || parts[1].trim().isEmpty()) {
            throw new SQLException("Invalid D1 URL: expected jdbc:d1://<database-id>, got: " + url);
        }
        String databaseId = parts[1].trim();
        String accountId = getRequired(info, "user", "AccountId (user property)");
        String apiToken = getRequired(info, "password", "API Token (password property)");
        return new D1Connection(apiToken, accountId, databaseId);
    }

    private String getRequired(Properties info, String key, String label) throws SQLException {
        if (info == null || !info.containsKey(key) || info.getProperty(key).trim().isEmpty()) {
            throw new SQLException("Missing required connection property: " + label);
        }
        return info.getProperty(key).trim();
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && url.startsWith("jdbc:d1://");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        DriverPropertyInfo user = new DriverPropertyInfo("user", info != null ? info.getProperty("user") : null);
        user.description = "Cloudflare Account ID";
        user.required = true;

        DriverPropertyInfo password = new DriverPropertyInfo("password", null);
        password.description = "Cloudflare API Token";
        password.required = true;

        return new DriverPropertyInfo[]{user, password};
    }

    @Override
    public int getMajorVersion() {
        return 2;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
