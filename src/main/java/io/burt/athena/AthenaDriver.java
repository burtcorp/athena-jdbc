package io.burt.athena;

import software.amazon.awssdk.regions.Region;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AthenaDriver implements Driver {
    private static Pattern URL_PATTERN = Pattern.compile("^jdbc:awsathena://(.+)$");

    private final AwsClientFactory clientFactory;

    public AthenaDriver() {
        this(new AwsClientFactory());
    }

    AthenaDriver(AwsClientFactory clientFactory) {
        this.clientFactory = clientFactory;
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        Matcher m = matchUrl(url);
        if (m.matches()) {
            String databaseName = m.group(1);
            Region region = Region.of(info.getProperty("AWS_REGION"));
            String workGroup = info.getProperty("WORK_GROUP");
            String outputLocation = info.getProperty("OUTPUT_LOCATION");
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
