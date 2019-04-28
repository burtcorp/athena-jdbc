package io.burt.athena;

import java.time.Duration;

public class ConnectionConfiguration {
    private final String databaseName;
    private final String workGroupName;
    private final String outputLocation;
    private final Duration timeout;

    public ConnectionConfiguration(String databaseName, String workGroupName, String outputLocation, Duration timeout) {
        this.databaseName = databaseName;
        this.workGroupName = workGroupName;
        this.outputLocation = outputLocation;
        this.timeout = timeout;
    }

    public String databaseName() {
        return databaseName;
    }

    public String workGroupName() {
        return workGroupName;
    }

    public String outputLocation() {
        return outputLocation;
    }

    public Duration apiCallTimeout() {
        return timeout;
    }

    public ConnectionConfiguration withDatabaseName(String databaseName) {
        return new ConnectionConfiguration(databaseName, workGroupName, outputLocation, timeout);
    }

    public ConnectionConfiguration withTimeout(Duration timeout) {
        return new ConnectionConfiguration(databaseName, workGroupName, outputLocation, timeout);
    }
}
