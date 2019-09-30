package io.burt.athena.support;

import io.burt.athena.configuration.ConnectionConfiguration;
import io.burt.athena.polling.PollingStrategy;
import io.burt.athena.result.Result;
import software.amazon.awssdk.services.athena.AthenaAsyncClient;
import software.amazon.awssdk.services.athena.model.QueryExecution;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.time.Duration;
import java.util.function.Function;
import java.util.function.Supplier;

public class ConfigurableConnectionConfiguration implements ConnectionConfiguration {
    private final String databaseName;
    private final String workGroupName;
    private final String outputLocation;
    private final Duration networkTimeout;
    private final Duration queryTimeout;
    private final Supplier<AthenaAsyncClient> athenaClientFactory;
    private final Supplier<S3AsyncClient> s3ClientFactory;
    private final Supplier<PollingStrategy> pollingStrategyFactory;
    private final Function<QueryExecution, Result> resultFactory;

    public ConfigurableConnectionConfiguration(String databaseName, String workGroupName, String outputLocation, Duration networkTimeout, Duration queryTimeout, Supplier<AthenaAsyncClient> athenaClientFactory, Supplier<S3AsyncClient> s3ClientFactory, Supplier<PollingStrategy> pollingStrategyFactory, Function<QueryExecution, Result> resultFactory) {
        this.databaseName = databaseName;
        this.workGroupName = workGroupName;
        this.outputLocation = outputLocation;
        this.networkTimeout = networkTimeout;
        this.queryTimeout = queryTimeout;
        this.athenaClientFactory = athenaClientFactory;
        this.s3ClientFactory = s3ClientFactory;
        this.pollingStrategyFactory = pollingStrategyFactory;
        this.resultFactory = resultFactory;
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
    public Duration networkTimeout() {
        return networkTimeout;
    }

    @Override
    public Duration queryTimeout() { return queryTimeout; }

    @Override
    public AthenaAsyncClient athenaClient() {
        return athenaClientFactory.get();
    }

    @Override
    public S3AsyncClient s3Client() {
        return s3ClientFactory.get();
    }

    @Override
    public PollingStrategy pollingStrategy() {
        return pollingStrategyFactory.get();
    }

    @Override
    public ConnectionConfiguration withDatabaseName(String newDatabaseName) {
        return new ConfigurableConnectionConfiguration(newDatabaseName, workGroupName, outputLocation, networkTimeout, queryTimeout, athenaClientFactory, s3ClientFactory, pollingStrategyFactory, resultFactory);
    }

    @Override
    public ConnectionConfiguration withNetworkTimeout(Duration newNetworkTimeout) {
        return new ConfigurableConnectionConfiguration(databaseName, workGroupName, outputLocation, newNetworkTimeout, queryTimeout, athenaClientFactory, s3ClientFactory, pollingStrategyFactory, resultFactory);
    }

    @Override
    public ConnectionConfiguration withQueryTimeout(Duration newQueryTimeout) {
        return new ConfigurableConnectionConfiguration(databaseName, workGroupName, outputLocation, networkTimeout, newQueryTimeout, athenaClientFactory, s3ClientFactory, pollingStrategyFactory, resultFactory);
    }

    @Override
    public Result createResult(QueryExecution queryExecution) {
        return resultFactory.apply(queryExecution);
    }

    @Override
    public void close() {
    }
}
