package io.burt.athena;

import io.burt.athena.polling.PollingStrategies;
import io.burt.athena.polling.PollingStrategy;
import io.burt.athena.result.PreloadingStandardResult;
import io.burt.athena.result.Result;
import io.burt.athena.result.S3Result;
import io.burt.athena.result.StandardResult;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaAsyncClient;
import software.amazon.awssdk.services.athena.model.QueryExecution;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.time.Duration;

public interface ConnectionConfiguration {
    enum ResultLoadingStrategy {
        GET_EXECUTION_RESULTS,
        S3
    }

    String databaseName();

    String workGroupName();

    String outputLocation();

    Duration apiCallTimeout();

    AthenaAsyncClient athenaClient();

    S3AsyncClient s3Client();

    PollingStrategy pollingStrategy();

    ConnectionConfiguration withDatabaseName(String databaseName);

    ConnectionConfiguration withTimeout(Duration timeout);

    Result createResult(QueryExecution queryExecution);
}

class ConcreteConnectionConfiguration implements ConnectionConfiguration {
    private final Region awsRegion;
    private final String databaseName;
    private final String workGroupName;
    private final String outputLocation;
    private final Duration timeout;
    private final ResultLoadingStrategy resultLoadingStrategy;

    private AthenaAsyncClient athenaClient;
    private S3AsyncClient s3Client;
    private PollingStrategy pollingStrategy;

    ConcreteConnectionConfiguration(Region awsRegion, String databaseName, String workGroupName, String outputLocation, Duration timeout, ResultLoadingStrategy resultLoadingStrategy) {
        this.awsRegion = awsRegion;
        this.databaseName = databaseName;
        this.workGroupName = workGroupName;
        this.outputLocation = outputLocation;
        this.timeout = timeout;
        this.resultLoadingStrategy = resultLoadingStrategy;
    }

    private ConcreteConnectionConfiguration(Region awsRegion, String databaseName, String workGroupName, String outputLocation, Duration timeout, ResultLoadingStrategy resultLoadingStrategy, AthenaAsyncClient athenaClient, S3AsyncClient s3Client, PollingStrategy pollingStrategy) {
        this(awsRegion, databaseName, workGroupName, outputLocation, timeout, resultLoadingStrategy);
        this.athenaClient = athenaClient;
        this.s3Client = s3Client;
        this.pollingStrategy = pollingStrategy;
    }

    @Override
    public String databaseName() {
        return databaseName;
    }

    @Override
    public String workGroupName() {
        return workGroupName;
    }

    @Override
    public String outputLocation() {
        return outputLocation;
    }

    @Override
    public Duration apiCallTimeout() {
        return timeout;
    }

    @Override
    public AthenaAsyncClient athenaClient() {
        if (athenaClient == null) {
            athenaClient = AthenaAsyncClient.builder().region(awsRegion).build();
        }
        return athenaClient;
    }

    @Override
    public S3AsyncClient s3Client() {
        if (s3Client == null) {
            s3Client = S3AsyncClient.builder().region(awsRegion).build();
        }
        return s3Client;
    }

    @Override
    public PollingStrategy pollingStrategy() {
        if (pollingStrategy == null) {
            pollingStrategy = PollingStrategies.backoff(Duration.ofMillis(10), Duration.ofSeconds(5));
        }
        return pollingStrategy;
    }

    @Override
    public ConnectionConfiguration withDatabaseName(String databaseName) {
        return new ConcreteConnectionConfiguration(awsRegion, databaseName, workGroupName, outputLocation, timeout, resultLoadingStrategy, athenaClient, s3Client, pollingStrategy);
    }

    @Override
    public ConnectionConfiguration withTimeout(Duration timeout) {
        return new ConcreteConnectionConfiguration(awsRegion, databaseName, workGroupName, outputLocation, timeout, resultLoadingStrategy, athenaClient, s3Client, pollingStrategy);
    }

    @Override
    public Result createResult(QueryExecution queryExecution) {
        if (resultLoadingStrategy == ResultLoadingStrategy.GET_EXECUTION_RESULTS) {
            return new PreloadingStandardResult(athenaClient(), queryExecution, StandardResult.MAX_FETCH_SIZE, Duration.ofSeconds(10));
        } else if (resultLoadingStrategy == ResultLoadingStrategy.S3) {
            return new S3Result(s3Client(), queryExecution, Duration.ofSeconds(10));
        } else {
            throw new IllegalStateException(String.format("No such result loading strategy: %s", queryExecution));
        }
    }
}

class ConnectionConfigurationFactory {
    ConnectionConfiguration createConnectionConfiguration(Region awsRegion, String databaseName, String workGroupName, String outputLocation, Duration timeout, ConnectionConfiguration.ResultLoadingStrategy resultLoadingStrategy) {
        return new ConcreteConnectionConfiguration(awsRegion, databaseName, workGroupName, outputLocation, timeout, resultLoadingStrategy);
    }
}
