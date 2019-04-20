package io.burt.athena;

public class ConnectionConfiguration {
    private final String databaseName;
    private final String workGroupName;
    private final String outputLocation;

    public ConnectionConfiguration(String databaseName, String workGroupName, String outputLocation) {
        this.databaseName = databaseName;
        this.workGroupName = workGroupName;
        this.outputLocation = outputLocation;
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
}
