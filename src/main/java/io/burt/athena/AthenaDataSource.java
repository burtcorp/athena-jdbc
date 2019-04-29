package io.burt.athena;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public class AthenaDataSource implements DataSource {
    private final AwsClientFactory clientFactory;
    private final Driver driver;
    private final Properties properties;
    private String databaseName;

    public AthenaDataSource() {
        this(new AwsClientFactory(), System.getenv());
    }

    AthenaDataSource(AwsClientFactory clientFactory, Map<String, String> env) {
        this.clientFactory = clientFactory;
        this.driver = new AthenaDriver(clientFactory, env);
        this.databaseName = "default";
        this.properties = new Properties();
    }

    public void setRegion(String region) {
        properties.setProperty(AthenaDriver.REGION_PROPERTY_NAME, region);
    }

    public void setDatabase(String name) {
        databaseName = name;
    }

    public void setWorkGroup(String name) {
        properties.setProperty(AthenaDriver.WORK_GROUP_PROPERTY_NAME, name);
    }

    public void setOutputLocation(String uri) {
        properties.setProperty(AthenaDriver.OUTPUT_LOCATION_PROPERTY_NAME, uri);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return driver.connect(String.format("jdbc:%s://%s", AthenaDriver.JDBC_SUB_PROTOCOL, databaseName), properties);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connections cannot be created with username and password, see the documentation for how to configure AWS IAM credentials");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return iface.cast(this);
        } else {
            throw new SQLException(String.format("%s is not a wrapper for %s", this.getClass().getName(), iface.getName()));
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("java.util.logging is not used by this data source");
    }
}
