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
import software.amazon.awssdk.services.athena.model.StopQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.StopQueryExecutionResponse;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class QueryExecutionHelper implements AthenaAsyncClient {
    private final List<StartQueryExecutionRequest> startQueryRequests;
    private final List<GetQueryExecutionRequest> getQueryExecutionRequests;
    private final List<GetQueryResultsRequest> getQueryResultsRequests;
    private final List<StopQueryExecutionRequest> stopQueryExecutionRequests;
    private final Queue<StartQueryExecutionResponse> startQueryExecutionResponseQueue;
    private final Queue<GetQueryExecutionResponse> getQueryExecutionResponseQueue;
    private final Queue<Exception> startQueryExecutionExceptionQueue;
    private final Queue<Exception> getQueryExecutionExceptionQueue;
    private Duration startQueryExecutionDelay;
    private Duration getQueryExecutionDelay;
    private Duration getQueryResultsDelay;
    private Lock getQueryExecutionBlocker;
    private boolean open;
    private TestClock clock;

    public QueryExecutionHelper() {
        this(new TestClock());
    }

    public QueryExecutionHelper(TestClock clock) {
        this.clock = clock;
        this.startQueryRequests = new LinkedList<>();
        this.getQueryExecutionRequests = new LinkedList<>();
        this.getQueryResultsRequests = new LinkedList<>();
        this.stopQueryExecutionRequests = new LinkedList<>();
        this.startQueryExecutionResponseQueue = new LinkedList<>();
        this.getQueryExecutionResponseQueue = new LinkedList<>();
        this.startQueryExecutionExceptionQueue = new LinkedList<>();
        this.getQueryExecutionExceptionQueue = new LinkedList<>();
        this.startQueryExecutionDelay = Duration.ZERO;
        this.getQueryExecutionDelay = Duration.ZERO;
        this.getQueryResultsDelay = Duration.ZERO;
        this.getQueryExecutionBlocker = new ReentrantLock();
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

    public void delayGetQueryResultsResponses(Duration delay) {
        getQueryResultsDelay = delay;
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

    public List<StopQueryExecutionRequest> stopQueryExecutionRequests() {
        return stopQueryExecutionRequests;
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
        queueGetQueryExecutionResponse(b -> b.queryExecution(bb -> bb.status(bbb -> bbb.state(state)).resultConfiguration(bbb -> bbb.outputLocation("s3://dummy/location.csv"))));
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

    public void blockGetQueryExecutionResponse() {
        getQueryExecutionBlocker.lock();
    }

    public void unblockGetQueryExecutionResponse() {
        getQueryExecutionBlocker.unlock();
    }

    public void queueStartQueryExecutionException(Exception e) {
        startQueryExecutionExceptionQueue.add(e);
    }

    public void queueGetQueryExecutionException(Exception e) {
        getQueryExecutionExceptionQueue.add(e);
    }

    private <T> CompletableFuture<T> maybeDelayResponse(CompletableFuture<T> future, Duration delay) {
        if (delay.isZero()) {
            return future;
        } else {
            return TestDelayedCompletableFuture.create(future, delay, clock);
        }
    }

    private <T> CompletableFuture<T> maybeFailResponse(CompletableFuture<T> future, Queue<Exception> exceptions) {
        if (exceptions.isEmpty()) {
            return future;
        } else {
            CompletableFuture<T> failedFuture = new CompletableFuture<T>();
            failedFuture.completeExceptionally(exceptions.remove());
            return failedFuture;
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
        return maybeDelayResponse(maybeFailResponse(future, startQueryExecutionExceptionQueue), startQueryExecutionDelay);
    }

    @Override
    public CompletableFuture<GetQueryExecutionResponse> getQueryExecution(Consumer<GetQueryExecutionRequest.Builder> requestBuilderConsumer) {
        GetQueryExecutionRequest.Builder builder = GetQueryExecutionRequest.builder();
        requestBuilderConsumer.accept(builder);
        GetQueryExecutionRequest request = builder.build();
        getQueryExecutionRequests.add(request);
        GetQueryExecutionResponse responsePrototype = getQueryExecutionResponseQueue.remove();
        GetQueryExecutionResponse response = responsePrototype.toBuilder().queryExecution(responsePrototype.queryExecution().toBuilder().queryExecutionId(request.queryExecutionId()).build()).build();
        try {
            getQueryExecutionBlocker.lock();
            CompletableFuture<GetQueryExecutionResponse> future = CompletableFuture.completedFuture(response);
            return maybeDelayResponse(maybeFailResponse(future, getQueryExecutionExceptionQueue), getQueryExecutionDelay);
        } finally {
            getQueryExecutionBlocker.unlock();
        }
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
        CompletableFuture<GetQueryResultsResponse> future = CompletableFuture.completedFuture(response);
        return maybeDelayResponse(future, getQueryResultsDelay);
    }

    @Override
    public CompletableFuture<StopQueryExecutionResponse> stopQueryExecution(Consumer<StopQueryExecutionRequest.Builder> requestBuilderConsumer) {
        StopQueryExecutionRequest.Builder builder = StopQueryExecutionRequest.builder();
        requestBuilderConsumer.accept(builder);
        StopQueryExecutionRequest request = builder.build();
        stopQueryExecutionRequests.add(request);
        StopQueryExecutionResponse response = StopQueryExecutionResponse.builder().build();
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
