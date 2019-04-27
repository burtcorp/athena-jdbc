package io.burt.athena.support;

import software.amazon.awssdk.services.athena.AthenaAsyncClient;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionResponse;
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
    private final Queue<StartQueryExecutionResponse> startQueryExecutionResponseQueue;
    private final Queue<GetQueryExecutionResponse> getQueryExecutionResponseQueue;
    private Duration startQueryExecutionDelay;
    private Duration getQueryExecutionDelay;
    private boolean open;

    public QueryExecutionHelper() {
        this.startQueryRequests = new LinkedList<>();
        this.getQueryExecutionRequests = new LinkedList<>();
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

    public void queueStartQueryResponse(Consumer<StartQueryExecutionResponse.Builder> responseBuilderConsumer) {
        StartQueryExecutionResponse.Builder builder = StartQueryExecutionResponse.builder();
        responseBuilderConsumer.accept(builder);
        startQueryExecutionResponseQueue.add(builder.build());
    }

    public void queueGetQueryExecutionResponse(Consumer<GetQueryExecutionResponse.Builder> responseBuilderConsumer) {
        GetQueryExecutionResponse.Builder builder = GetQueryExecutionResponse.builder();
        responseBuilderConsumer.accept(builder);
        getQueryExecutionResponseQueue.add(builder.build());
    }

    @Override
    public CompletableFuture<StartQueryExecutionResponse> startQueryExecution(Consumer<StartQueryExecutionRequest.Builder> requestBuilderConsumer) {
        StartQueryExecutionRequest.Builder builder = StartQueryExecutionRequest.builder();
        requestBuilderConsumer.accept(builder);
        StartQueryExecutionRequest request = builder.build();
        startQueryRequests.add(request);
        StartQueryExecutionResponse response = startQueryExecutionResponseQueue.remove();
        CompletableFuture<StartQueryExecutionResponse> future;
        if (startQueryExecutionDelay.isZero()) {
            future = CompletableFuture.completedFuture(response);
        } else {
            future = CompletableFuture.supplyAsync(() -> {
                try {
                    Thread.sleep(startQueryExecutionDelay.toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return response;
            });
        }
        return future;
    }

    @Override
    public CompletableFuture<GetQueryExecutionResponse> getQueryExecution(Consumer<GetQueryExecutionRequest.Builder> requestBuilderConsumer) {
        GetQueryExecutionRequest.Builder builder = GetQueryExecutionRequest.builder();
        requestBuilderConsumer.accept(builder);
        GetQueryExecutionRequest request = builder.build();
        getQueryExecutionRequests.add(request);
        GetQueryExecutionResponse response = getQueryExecutionResponseQueue.remove();
        CompletableFuture<GetQueryExecutionResponse> future;
        if (getQueryExecutionDelay.isZero()) {
            future = CompletableFuture.completedFuture(response);
        } else {
            future = CompletableFuture.supplyAsync(() -> {
                try {
                    Thread.sleep(getQueryExecutionDelay.toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return response;
            });
        }
        return future;
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
