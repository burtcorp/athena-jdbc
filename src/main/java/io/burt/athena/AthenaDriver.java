package io.burt.athena;

import software.amazon.awssdk.regions.Region;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AthenaDriver implements Driver {
    public static final String JDBC_SUB_PROTOCOL = "athena";

    private static final Pattern URL_PATTERN = Pattern.compile("^jdbc:" + JDBC_SUB_PROTOCOL + "://(.+)$");

    private final AwsClientFactory clientFactory;
    private final Map<String, String> env;

    public AthenaDriver() {
        this(new AwsClientFactory(), System.getenv());
    }

    AthenaDriver(AwsClientFactory clientFactory, Map<String, String> env) {
        this.clientFactory = clientFactory;
        this.env = env;
    }

    private Region determineRegion(Properties properties) {
        if (properties.containsKey("AWS_REGION")) {
            return Region.of(properties.getProperty("AWS_REGION"));
        } else if (env.containsKey("AWS_REGION")) {
            return Region.of(env.get("AWS_REGION"));
        } else if (env.containsKey("AWS_DEFAULT_REGION")) {
            return Region.of(env.get("AWS_DEFAULT_REGION"));
        } else {
            return null;
        }
    }

    @Override
    public Connection connect(String url, Properties connectionProperties) throws SQLException {
        Matcher m = matchUrl(url);
        if (m.matches()) {
            String databaseName = m.group(1);
            Region region = determineRegion(connectionProperties);
            String workGroup = connectionProperties.getProperty("WORK_GROUP");
            String outputLocation = connectionProperties.getProperty("OUTPUT_LOCATION");
            ConnectionConfiguration configuration = new ConnectionConfiguration(databaseName, workGroup, outputLocation, Duration.ofMinutes(1));
            return new AthenaConnection(clientFactory.createAthenaClient(region), configuration);
        } else {
            return null;
        }
    }

    private Matcher matchUrl(String url) {
        return URL_PATTERN.matcher(url);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return matchUrl(url).matches();
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
        return 0;
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
