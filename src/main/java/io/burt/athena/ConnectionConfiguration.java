package io.burt.athena;

import java.time.Duration;

class ConnectionConfiguration {
    private final String databaseName;
    private final String workGroupName;
    private final String outputLocation;
    private final Duration timeout;

    ConnectionConfiguration(String databaseName, String workGroupName, String outputLocation, Duration timeout) {
        this.databaseName = databaseName;
        this.workGroupName = workGroupName;
        this.outputLocation = outputLocation;
        this.timeout = timeout;
    }

    String databaseName() {
        return databaseName;
    }

    String workGroupName() {
        return workGroupName;
    }

    String outputLocation() {
        return outputLocation;
    }

    Duration apiCallTimeout() {
        return timeout;
    }

    ConnectionConfiguration withDatabaseName(String databaseName) {
        return new ConnectionConfiguration(databaseName, workGroupName, outputLocation, timeout);
    }

    ConnectionConfiguration withTimeout(Duration timeout) {
        return new ConnectionConfiguration(databaseName, workGroupName, outputLocation, timeout);
    }
}
