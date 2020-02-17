package io.burt.athena;

import java.io.IOException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class AthenaDriverInfo {
    private static final String driverVersion;
    private static final int driverMajorVersion;
    private static final int driverMinorVersion;

    static {
        final Properties properties = new Properties();
        try {
            properties.load(AthenaDatabaseMetaData.class.getClassLoader().getResourceAsStream("project.properties"));
        } catch (IOException e) {
            throw new RuntimeException("Could not load driver version: " + e.getMessage(), e);
        }
        driverVersion = properties.getProperty("project.version");
        final Pattern versionComponentsPattern = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)(?:-\\w+)?");
        final Matcher matcher = versionComponentsPattern.matcher(driverVersion);
        if (matcher.matches()) {
            driverMajorVersion = Integer.parseInt(matcher.group(1));
            driverMinorVersion = Integer.parseInt(matcher.group(2));
        } else {
            throw new RuntimeException("Could not parse driver version: " + driverVersion);
        }
    }

    static String getDriverVersion() {
        return driverVersion;
    }

    static int getDriverMajorVersion() {
        return driverMajorVersion;
    }

    static int getDriverMinorVersion() {
        return driverMinorVersion;
    }
}
