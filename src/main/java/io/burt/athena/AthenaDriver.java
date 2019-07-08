package io.burt.athena;

import io.burt.athena.configuration.ConnectionConfiguration;
import io.burt.athena.configuration.ConnectionConfigurationFactory;
import software.amazon.awssdk.regions.Region;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.Enumeration;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AthenaDriver implements Driver {
    public static final String REGION_PROPERTY_NAME = "region";
    public static final String WORK_GROUP_PROPERTY_NAME = "workGroup";
    public static final String OUTPUT_LOCATION_PROPERTY_NAME = "outputLocation";
    public static final String DEFAULT_DATABASE_NAME = "default";
    public static final String JDBC_SUBPROTOCOL = "athena";

    private static final Pattern URL_PATTERN = Pattern.compile("^jdbc:" + JDBC_SUBPROTOCOL + "(?::([a-zA-Z]\\w*))?$");

    private final ConnectionConfigurationFactory connectionConfigurationFactory;

    static {
        try {
            register();
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public AthenaDriver() {
        this(new ConnectionConfigurationFactory());
    }

    AthenaDriver(ConnectionConfigurationFactory connectionConfigurationFactory) {
        this.connectionConfigurationFactory = connectionConfigurationFactory;
    }

    /**
     * Creates a JDBC URL for the specified database.
     *
     * This URL can be used to get a connection with
     * {@link java.sql.DriverManager#getConnection(String)} or
     * {@link java.sql.DriverManager#getDriver(String)}.
     *
     * @param databaseName the database to encode into the URL
     * @return a JDBC URL compatible with this driver
     */
    public static String createURL(String databaseName) {
        return String.format("jdbc:%s:%s", JDBC_SUBPROTOCOL, databaseName);
    }

    /**
     * Registers this driver with {@link java.sql.DriverManager}.
     *
     * This is done automatically when the driver is loaded, and calling
     * this method again should have no effect.
     *
     * @throws SQLException re-thrown from {@link java.sql.DriverManager#registerDriver(Driver)}
     */
    public static void register() throws SQLException {
        if (registeredDriver() == null) {
            DriverManager.registerDriver(new AthenaDriver());
        }
    }

    /**
     * Deregisters this driver from {@link java.sql.DriverManager}.
     *
     * After this method has been called the driver can be registered again with
     * {@link #register()}.
     *
     * @throws SQLException re-thrown from {@link java.sql.DriverManager#deregisterDriver(Driver)}
     */
    public static void deregister() throws SQLException {
        DriverManager.deregisterDriver(registeredDriver());
    }

    private static Driver registeredDriver() {
        for (Enumeration<Driver> e = DriverManager.getDrivers(); e.hasMoreElements(); ) {
            Driver d = e.nextElement();
            if (d.getClass() == AthenaDriver.class) {
                return d;
            }
        }
        return null;
    }

    /**
     * Creates an Athena connection object.
     *
     * Since Athena does not have a stateful protocol this method does not open
     * any persistent connections, it just creates SDK clients and other objects
     * necessary to perform API calls.
     *
     * The SDK clients maintain resources that can be freed by calling
     * {@link Connection#close()} when the connection is no longer in use.
     *
     * A connection can support any number of concurrent executions and is
     * thread safe. There is no need to create more than one connection for the
     * same URL and connection properties.
     *
     * @param url the JDBC URL that describes which database to connect to,
     *            see {@link #createURL(String)}.
     * @param connectionProperties a properties object containing one or more
     *                             of the keys
     *                             {@link AthenaDriver#REGION_PROPERTY_NAME},
     *                             {@link AthenaDriver#OUTPUT_LOCATION_PROPERTY_NAME},
     *                             and {@link AthenaDriver#WORK_GROUP_PROPERTY_NAME}.
     *                             All other keys will be ignored.
     * @return a JDBC connection ready to execute queries
     */
    @Override
    public Connection connect(String url, Properties connectionProperties) {
        Matcher m = matchURL(url);
        if (m.matches()) {
            String databaseName = m.group(1) == null ? DEFAULT_DATABASE_NAME : m.group(1);
            Region region = connectionProperties.containsKey(REGION_PROPERTY_NAME) ? Region.of(connectionProperties.getProperty(REGION_PROPERTY_NAME)) : null;
            String workGroup = connectionProperties.getProperty(WORK_GROUP_PROPERTY_NAME);
            String outputLocation = connectionProperties.getProperty(OUTPUT_LOCATION_PROPERTY_NAME);
            ConnectionConfiguration configuration = connectionConfigurationFactory.createConnectionConfiguration(
                    region,
                    databaseName,
                    workGroup,
                    outputLocation,
                    Duration.ofMinutes(1),
                    ConnectionConfiguration.ResultLoadingStrategy.S3
            );
            return new AthenaConnection(configuration);
        } else {
            return null;
        }
    }

    private Matcher matchURL(String url) {
        return URL_PATTERN.matcher(url);
    }

    @Override
    public boolean acceptsURL(String url) {
        return matchURL(url).matches();
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 2;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("java.util.logging is not used by this driver");
    }
}
