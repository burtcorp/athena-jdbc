package io.burt.athena;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class AthenaDataSource implements DataSource {
    private final Driver driver;
    private final Properties properties;
    private String databaseName;

    /**
     * Creates a new Athena data source with default configuration.
     */
    public AthenaDataSource() {
        this(new ConnectionConfigurationFactory());
    }

    AthenaDataSource(ConnectionConfigurationFactory connectionConfigurationFactory) {
        this.driver = new AthenaDriver(connectionConfigurationFactory);
        this.databaseName = "default";
        this.properties = new Properties();
    }

    /**
     * Sets the AWS region to use.
     *
     * This determines which AWS region that connections will run queries in.
     *
     * Corresponds to setting the {@link AthenaDriver#REGION_PROPERTY_NAME}
     * connection property.
     *
     * If not set it is left up to the AWS SDK to pick the region, for example
     * by using the value of the {@code AWS_REGION} environment variable.
     *
     * @param region a well formed AWS region, e.g. "us-east-1", "eu-west-1".
     */
    public void setRegion(String region) {
        properties.setProperty(AthenaDriver.REGION_PROPERTY_NAME, region);
    }

    /**
     * Sets the database to use.
     *
     * This determines which Athena database queries will be run in.
     *
     * Corresponds to setting the {@link AthenaDriver#REGION_PROPERTY_NAME}
     * connection property.
     *
     * Defaults to "default" if not set.
     *
     * @param name the database name to use
     */
    public void setDatabase(String name) {
        databaseName = name;
    }

    /**
     * Sets the work group to use.
     *
     * This determines which Athena work group that queries will be run in.
     *
     * Corresponds to setting the {@link AthenaDriver#WORK_GROUP_PROPERTY_NAME}
     * connection property.
     *
     * @param name the name of the work group to use.
     */
    public void setWorkGroup(String name) {
        properties.setProperty(AthenaDriver.WORK_GROUP_PROPERTY_NAME, name);
    }

    /**
     * Sets the S3 output location for results.
     *
     * This determines where on S3 Athena will write results.
     *
     * Corresponds to setting the {@link AthenaDriver#OUTPUT_LOCATION_PROPERTY_NAME}
     * connection property.
     *
     * @param uri the S3 URI where results should be written
     */
    public void setOutputLocation(String uri) {
        properties.setProperty(AthenaDriver.OUTPUT_LOCATION_PROPERTY_NAME, uri);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return driver.connect(AthenaDriver.createURL(databaseName), properties);
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
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isAssignableFrom(getClass());
    }

    /**
     * Always returns {@code null}.
     *
     * @return null
     */
    @Override
    public PrintWriter getLogWriter() {
        return null;
    }

    /**
     * Calling this method has no effect.
     *
     * @param out ignored
     */
    @Override
    public void setLogWriter(PrintWriter out) {
    }

    /**
     * Calling this method has no effect.
     *
     * Since Athena does not have a stateful protocol there is no login
     * procedure and a login timeout is not defined.
     *
     * @param seconds ignored
     */
    @Override
    public void setLoginTimeout(int seconds) {
    }

    /**
     * Always returns zero.
     *
     * @return zero
     */
    @Override
    public int getLoginTimeout() {
        return 0;
    }

    /**
     * Always throws {@link SQLFeatureNotSupportedException}.
     *
     * This driver does not use {@link java.util.logging}, and therefore throws
     * {@link SQLFeatureNotSupportedException} as per the JDBC specification.
     *
     * @return never returns
     * @throws SQLFeatureNotSupportedException always thrown
     */
    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("java.util.logging is not used by this data source");
    }
}
