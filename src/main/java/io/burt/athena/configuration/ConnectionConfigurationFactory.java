package io.burt.athena.configuration;

import software.amazon.awssdk.regions.Region;

import java.time.Duration;

public class ConnectionConfigurationFactory {
    public ConnectionConfiguration createConnectionConfiguration(Region awsRegion, String databaseName, String workGroupName, String outputLocation, Duration networkTimeout, Duration queryTimeout, ResultLoadingStrategy resultLoadingStrategy) {
        return new ConcreteConnectionConfiguration(awsRegion, databaseName, workGroupName, outputLocation, networkTimeout, queryTimeout, resultLoadingStrategy);
    }
}

