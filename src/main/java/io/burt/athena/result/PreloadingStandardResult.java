package io.burt.athena.result;

import software.amazon.awssdk.services.athena.AthenaAsyncClient;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.athena.model.QueryExecution;

import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class PreloadingStandardResult extends StandardResult {
    private CompletableFuture<GetQueryResultsResponse> pendingResult;

    public PreloadingStandardResult(AthenaAsyncClient athenaClient, QueryExecution queryExecution, int fetchSize, Duration timeout) {
        super(athenaClient, queryExecution, fetchSize, timeout);
        this.pendingResult = null;
    }

    @Override
    protected boolean shouldLoadNextPage() throws SQLException {
        return (getRowNumber() == 0 && currentRows == null) || (pendingResult != null && !currentRows.hasNext());
    }

    @Override
    protected GetQueryResultsResponse loadNextPage() throws InterruptedException, TimeoutException, ExecutionException {
        CompletableFuture<GetQueryResultsResponse> loadingPage;
        if (pendingResult == null) {
            loadingPage = loadPage();
        } else {
            loadingPage = pendingResult;
            pendingResult = null;
        }
        GetQueryResultsResponse response = loadingPage.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        if (response.nextToken() != null) {
            pendingResult = loadPage(response.nextToken());
        }
        return response;
    }

    @Override
    public ResultPosition getPosition() throws SQLException {
        if (pendingResult == null && currentRows != null && currentRow != null && !currentRows.hasNext()) {
            return ResultPosition.LAST;
        } else if (pendingResult == null && currentRows != null && currentRow == null) {
            return ResultPosition.AFTER_LAST;
        } else {
            return super.getPosition();
        }
    }
}
