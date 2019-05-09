package io.burt.athena;

import io.burt.athena.polling.PollingStrategies;
import io.burt.athena.polling.PollingStrategy;
import io.burt.athena.result.PreloadingStandardResult;
import io.burt.athena.result.Result;
import io.burt.athena.result.StandardResult;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaAsyncClient;
import software.amazon.awssdk.services.athena.model.QueryExecution;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Supplier;
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

    private final AwsClientFactory clientFactory;
    private final Map<String, String> env;

    static {
        try {
            register();
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public AthenaDriver() {
        this(new AwsClientFactory(), System.getenv());
    }

    AthenaDriver(AwsClientFactory clientFactory, Map<String, String> env) {
        this.clientFactory = clientFactory;
        this.env = env;
    }

    public static String createURL(String databaseName) {
        return String.format("jdbc:%s:%s", JDBC_SUBPROTOCOL, databaseName);
    }

    public static void register() throws SQLException {
        if (registeredDriver() == null) {
            DriverManager.registerDriver(new AthenaDriver());
        }
    }

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

    @Override
    public Connection connect(String url, Properties connectionProperties) throws SQLException {
        Matcher m = matchURL(url);
        if (m.matches()) {
            String databaseName = m.group(1) == null ? DEFAULT_DATABASE_NAME : m.group(1);
            Region region = connectionProperties.containsKey(REGION_PROPERTY_NAME) ? Region.of(connectionProperties.getProperty(REGION_PROPERTY_NAME)) : null;
            String workGroup = connectionProperties.getProperty(WORK_GROUP_PROPERTY_NAME);
            String outputLocation = connectionProperties.getProperty(OUTPUT_LOCATION_PROPERTY_NAME);
            ConnectionConfiguration configuration = new ConnectionConfiguration(databaseName, workGroup, outputLocation, Duration.ofMinutes(1));
            AthenaAsyncClient athenaClient = clientFactory.createAthenaClient(region);
            Function<QueryExecution, Result> resultFactory = (queryExecution) -> new PreloadingStandardResult(athenaClient, queryExecution, StandardResult.MAX_FETCH_SIZE, configuration.apiCallTimeout());
            Supplier<PollingStrategy> pollingStrategyFactory = () -> PollingStrategies.backoff(Duration.ofMillis(10), Duration.ofSeconds(5));
            return new AthenaConnection(athenaClient, configuration, resultFactory, pollingStrategyFactory);
        } else {
            return null;
        }
    }

    private Matcher matchURL(String url) {
        return URL_PATTERN.matcher(url);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return matchURL(url).matches();
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
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
