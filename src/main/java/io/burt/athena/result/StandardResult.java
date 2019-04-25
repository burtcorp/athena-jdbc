package io.burt.athena.result;

import io.burt.athena.AthenaResultSetMetaData;
import software.amazon.awssdk.services.athena.AthenaAsyncClient;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.athena.model.Row;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class StandardResult implements Result {
    public static final int MAX_FETCH_SIZE = 1000;

    private int fetchSize;

    protected final String queryExecutionId;
    protected final AthenaAsyncClient athenaClient;

    protected AthenaResultSetMetaData resultSetMetaData;
    protected String nextToken;
    protected int rowNumber;
    protected Iterator<Row> currentRows;
    protected Row currentRow;

    public StandardResult(AthenaAsyncClient athenaClient, String queryExecutionId, int fetchSize) {
        this.athenaClient = athenaClient;
        this.queryExecutionId = queryExecutionId;
        this.fetchSize = fetchSize;
        this.rowNumber = 0;
        this.nextToken = null;
        this.currentRows = null;
        this.currentRow = null;
        this.resultSetMetaData = null;
    }

    protected void ensureResults() throws SQLException {
        if ((rowNumber == 0 && currentRows == null) || (nextToken != null && !currentRows.hasNext())) {
            try {
                GetQueryResultsResponse response = loadPage().get(1, TimeUnit.MINUTES);
                nextToken = response.nextToken();
                resultSetMetaData = new AthenaResultSetMetaData(response.resultSet().resultSetMetadata());
                currentRows = response.resultSet().rows().iterator();
                if (rowNumber == 0 && currentRows.hasNext()) {
                    currentRows.next();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } catch (TimeoutException ie) {
                throw new SQLTimeoutException(ie);
            } catch (ExecutionException ee) {
                throw new SQLException(ee);
            }
        }
    }

    protected CompletableFuture<GetQueryResultsResponse> loadPage() {
        return loadPage(nextToken);
    }

    protected CompletableFuture<GetQueryResultsResponse> loadPage(String nextToken) {
        return athenaClient.getQueryResults(builder -> {
            builder.nextToken(nextToken);
            builder.queryExecutionId(queryExecutionId);
            builder.maxResults(fetchSize);
        });
    }

    @Override
    public int fetchSize() throws SQLException {
        return fetchSize;
    }

    @Override
    public void updateFetchSize(int newFetchSize) throws SQLException {
        if (newFetchSize > MAX_FETCH_SIZE) {
            throw new SQLException(String.format("Fetch size too large (got %d, max is %d)", newFetchSize, MAX_FETCH_SIZE));
        } else {
            fetchSize = newFetchSize;
        }
    }

    @Override
    public AthenaResultSetMetaData metaData() throws SQLException {
        ensureResults();
        return resultSetMetaData;
    }

    @Override
    public int rowNumber() throws SQLException {
        return rowNumber;
    }

    @Override
    public boolean next() throws SQLException {
        ensureResults();
        rowNumber++;
        if (currentRows.hasNext()) {
            currentRow = currentRows.next();
        } else {
            currentRow = null;
        }
        return currentRow != null;
    }

    @Override
    public String stringValue(int columnIndex) throws SQLException {
        return currentRow.data().get(columnIndex - 1).varCharValue();
    }

    @Override
    public boolean isLast() throws SQLException {
        return nextToken == null && currentRows != null && currentRow != null && !currentRows.hasNext();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return nextToken == null && currentRows != null && currentRow == null;
    }
}
