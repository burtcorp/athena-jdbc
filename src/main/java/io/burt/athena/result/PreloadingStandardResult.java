package io.burt.athena.result;

import io.burt.athena.AthenaResultSetMetaData;
import software.amazon.awssdk.services.athena.AthenaAsyncClient;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class PreloadingStandardResult extends StandardResult {
    private CompletableFuture<GetQueryResultsResponse> pendingResult;

    public PreloadingStandardResult(AthenaAsyncClient athenaClient, String queryExecutionId, int fetchSize, Duration timeout) {
        super(athenaClient, queryExecutionId, fetchSize, timeout);
        this.pendingResult = null;
    }

    protected void ensureResults() throws SQLException, InterruptedException {
        if ((rowNumber == 0 && currentRows == null) || (pendingResult != null && !currentRows.hasNext())) {
            try {
                GetQueryResultsResponse response;
                if (pendingResult == null) {
                    response = loadPage().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
                } else {
                    response = pendingResult.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
                    pendingResult = null;
                }
                if (response.nextToken() != null) {
                    pendingResult = loadPage(response.nextToken());
                }
                resultSetMetaData = new AthenaResultSetMetaData(response.resultSet().resultSetMetadata());
                currentRows = response.resultSet().rows().iterator();
                if (rowNumber == 0 && currentRows.hasNext()) {
                    currentRows.next();
                }
            } catch (TimeoutException ie) {
                throw new SQLTimeoutException(ie);
            } catch (ExecutionException ee) {
                throw new SQLException(ee);
            }
        }
    }

    @Override
    public boolean isLast() throws SQLException {
        return pendingResult == null && currentRows != null && currentRow != null && !currentRows.hasNext();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return pendingResult == null && currentRows != null && currentRow == null;
    }
}
