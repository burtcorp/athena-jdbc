package io.burt.athena;

import io.burt.athena.polling.PollingStrategies;
import io.burt.athena.polling.PollingStrategy;
import io.burt.athena.result.PreloadingStandardResult;
import io.burt.athena.result.Result;
import io.burt.athena.result.StandardResult;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaAsyncClient;
import software.amazon.awssdk.services.athena.model.QueryExecution;

import java.time.Duration;

class ConnectionConfiguration {
    private final Region awsRegion;
    private final String databaseName;
    private final String workGroupName;
    private final String outputLocation;
    private final Duration timeout;

    private AthenaAsyncClient athenaClient;
    private PollingStrategy pollingStrategy;

    ConnectionConfiguration(Region awsRegion, String databaseName, String workGroupName, String outputLocation, Duration timeout) {
        this.awsRegion = awsRegion;
        this.databaseName = databaseName;
        this.workGroupName = workGroupName;
        this.outputLocation = outputLocation;
        this.timeout = timeout;
    }

    private ConnectionConfiguration(Region awsRegion, String databaseName, String workGroupName, String outputLocation, Duration timeout, AthenaAsyncClient athenaClient, PollingStrategy pollingStrategy) {
        this(awsRegion, databaseName, workGroupName, outputLocation, timeout);
        this.athenaClient = athenaClient;
        this.pollingStrategy = pollingStrategy;
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

    AthenaAsyncClient athenaClient() {
        if (athenaClient == null) {
            athenaClient = AthenaAsyncClient.builder().region(awsRegion).build();
        }
        return athenaClient;
    }

    PollingStrategy pollingStrategy() {
        if (pollingStrategy == null) {
            pollingStrategy = PollingStrategies.backoff(Duration.ofMillis(10), Duration.ofSeconds(5));
        }
        return pollingStrategy;
    }

    ConnectionConfiguration withDatabaseName(String databaseName) {
        return new ConnectionConfiguration(awsRegion, databaseName, workGroupName, outputLocation, timeout, athenaClient, pollingStrategy);
    }

    ConnectionConfiguration withTimeout(Duration timeout) {
        return new ConnectionConfiguration(awsRegion, databaseName, workGroupName, outputLocation, timeout, athenaClient, pollingStrategy);
    }

    Result createResult(QueryExecution queryExecution) {
        return new PreloadingStandardResult(athenaClient(), queryExecution, StandardResult.MAX_FETCH_SIZE, Duration.ofSeconds(10));
    }
}

class ConnectionConfigurationFactory {
    ConnectionConfiguration createConnectionConfiguration(Region awsRegion, String databaseName, String workGroupName, String outputLocation, Duration timeout) {
        return new ConnectionConfiguration(awsRegion, databaseName, workGroupName, outputLocation, timeout);
    }
}
