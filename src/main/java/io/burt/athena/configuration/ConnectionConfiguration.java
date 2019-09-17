package io.burt.athena.configuration;

import io.burt.athena.polling.PollingStrategy;
import io.burt.athena.result.Result;
import software.amazon.awssdk.services.athena.AthenaAsyncClient;
import software.amazon.awssdk.services.athena.model.QueryExecution;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.time.Duration;

public interface ConnectionConfiguration extends AutoCloseable {
    String databaseName();

    String workGroupName();

    String outputLocation();

    Duration networkTimeout();

    Duration queryTimeout();

    AthenaAsyncClient athenaClient();

    S3AsyncClient s3Client();

    PollingStrategy pollingStrategy();

    ConnectionConfiguration withDatabaseName(String databaseName);

    ConnectionConfiguration withNetworkTimeout(Duration timeout);

    ConnectionConfiguration withQueryTimeout(Duration timeout);

    Result createResult(QueryExecution queryExecution);
}
