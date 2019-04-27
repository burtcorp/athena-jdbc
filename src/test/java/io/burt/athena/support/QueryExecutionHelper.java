package io.burt.athena.support;

import software.amazon.awssdk.services.athena.AthenaAsyncClient;
import software.amazon.awssdk.services.athena.model.ColumnInfo;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.GetQueryResultsRequest;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.athena.model.QueryExecutionState;
import software.amazon.awssdk.services.athena.model.Row;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionResponse;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class QueryExecutionHelper implements AthenaAsyncClient {
    private final List<StartQueryExecutionRequest> startQueryRequests;
    private final List<GetQueryExecutionRequest> getQueryExecutionRequests;
    private final List<GetQueryResultsRequest> getQueryResultsRequests;
    private final Queue<StartQueryExecutionResponse> startQueryExecutionResponseQueue;
    private final Queue<GetQueryExecutionResponse> getQueryExecutionResponseQueue;
    private Duration startQueryExecutionDelay;
    private Duration getQueryExecutionDelay;
    private boolean open;

    public QueryExecutionHelper() {
        this.startQueryRequests = new LinkedList<>();
        this.getQueryExecutionRequests = new LinkedList<>();
        this.getQueryResultsRequests = new LinkedList<>();
        this.startQueryExecutionResponseQueue = new LinkedList<>();
        this.getQueryExecutionResponseQueue = new LinkedList<>();
        this.startQueryExecutionDelay = Duration.ZERO;
        this.getQueryExecutionDelay = Duration.ZERO;
        this.open = true;
    }

    public boolean isClosed() {
        return !open;
    }

    public void delayStartQueryExecutionResponses(Duration delay) {
        startQueryExecutionDelay = delay;
    }

    public void delayGetQueryExecutionResponses(Duration delay) {
        getQueryExecutionDelay = delay;
    }

    public List<StartQueryExecutionRequest> startQueryRequests() {
        return startQueryRequests;
    }

    public List<GetQueryExecutionRequest> getQueryExecutionRequests() {
        return getQueryExecutionRequests;
    }

    public List<GetQueryResultsRequest> getQueryResultsRequests() {
        return getQueryResultsRequests;
    }

    public void queueStartQueryResponse(String queryExecutionId) {
        queueStartQueryResponse(b -> b.queryExecutionId(queryExecutionId));
    }

    public void queueStartQueryResponse(Consumer<StartQueryExecutionResponse.Builder> responseBuilderConsumer) {
        StartQueryExecutionResponse.Builder builder = StartQueryExecutionResponse.builder();
        responseBuilderConsumer.accept(builder);
        startQueryExecutionResponseQueue.add(builder.build());
    }

    public void queueGetQueryExecutionResponse(QueryExecutionState state) {
        queueGetQueryExecutionResponse(b -> b.queryExecution(bb -> bb.status(bbb -> bbb.state(state))));
    }

    public void queueGetQueryExecutionResponse(QueryExecutionState state, String stateChangeReason) {
        queueGetQueryExecutionResponse(b -> b.queryExecution(bb -> bb.status(bbb -> bbb.state(state).stateChangeReason(stateChangeReason))));
    }

    public void queueGetQueryExecutionResponse(Consumer<GetQueryExecutionResponse.Builder> responseBuilderConsumer) {
        GetQueryExecutionResponse.Builder builder = GetQueryExecutionResponse.builder();
        responseBuilderConsumer.accept(builder);
        getQueryExecutionResponseQueue.add(builder.build());
    }

    public void clearGetQueryExecutionResponseQueue() {
        getQueryExecutionResponseQueue.clear();
    }

    private <T> CompletableFuture<T> maybeDelayResponse(CompletableFuture<T> future, Duration delay) {
        if (delay.isZero()) {
            return future;
        } else {
            return future.thenApplyAsync(r -> {
                try {
                    Thread.sleep(startQueryExecutionDelay.toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return r;
            });
        }
    }

    @Override
    public CompletableFuture<StartQueryExecutionResponse> startQueryExecution(Consumer<StartQueryExecutionRequest.Builder> requestBuilderConsumer) {
        StartQueryExecutionRequest.Builder builder = StartQueryExecutionRequest.builder();
        requestBuilderConsumer.accept(builder);
        StartQueryExecutionRequest request = builder.build();
        startQueryRequests.add(request);
        StartQueryExecutionResponse response = startQueryExecutionResponseQueue.remove();
        CompletableFuture<StartQueryExecutionResponse> future = CompletableFuture.completedFuture(response);
        return maybeDelayResponse(future, startQueryExecutionDelay);
    }

    @Override
    public CompletableFuture<GetQueryExecutionResponse> getQueryExecution(Consumer<GetQueryExecutionRequest.Builder> requestBuilderConsumer) {
        GetQueryExecutionRequest.Builder builder = GetQueryExecutionRequest.builder();
        requestBuilderConsumer.accept(builder);
        GetQueryExecutionRequest request = builder.build();
        getQueryExecutionRequests.add(request);
        GetQueryExecutionResponse responsePrototype = getQueryExecutionResponseQueue.remove();
        GetQueryExecutionResponse response = responsePrototype.toBuilder().queryExecution(responsePrototype.queryExecution().toBuilder().queryExecutionId(request.queryExecutionId()).build()).build();
        CompletableFuture<GetQueryExecutionResponse> future = CompletableFuture.completedFuture(response);
        return maybeDelayResponse(future, getQueryExecutionDelay);
    }

    @Override
    public CompletableFuture<GetQueryResultsResponse> getQueryResults(Consumer<GetQueryResultsRequest.Builder> requestBuilderConsumer) {
        GetQueryResultsRequest.Builder builder = GetQueryResultsRequest.builder();
        requestBuilderConsumer.accept(builder);
        GetQueryResultsRequest request = builder.build();
        getQueryResultsRequests.add(request);
        GetQueryResultsResponse response = GetQueryResultsResponse.builder().resultSet(b -> {
            b.rows(new Row[0]);
            b.resultSetMetadata(bb -> {
                bb.columnInfo(new ColumnInfo[0]);
            });
        }).build();
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public String serviceName() {
        return null;
    }

    @Override
    public void close() {
        open = false;
    }
}
