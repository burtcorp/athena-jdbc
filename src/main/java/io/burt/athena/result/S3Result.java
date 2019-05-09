package io.burt.athena.result;

import io.burt.athena.AthenaResultSetMetaData;
import io.burt.athena.result.csv.VeryBasicCsvParser;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.athena.AthenaAsyncClient;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.athena.model.QueryExecution;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class S3Result implements Result {
    private static final Pattern S3_URI_PATTERN = Pattern.compile("^s3://([^/]+)/(.+)$");

    private final QueryExecution queryExecution;

    private AthenaAsyncClient athenaClient;
    private S3Client s3Client;
    private Duration timeout;
    private AthenaResultSetMetaData resultSetMetaData;
    private ResponseInputStream<GetObjectResponse> responseStream;
    private VeryBasicCsvParser csvParser;
    private String[] currentRow;
    private int rowNumber;

    public S3Result(AthenaAsyncClient athenaClient, S3Client s3Client, QueryExecution queryExecution, Duration timeout) {
        this.athenaClient = athenaClient;
        this.s3Client = s3Client;
        this.queryExecution = queryExecution;
        this.timeout = timeout;
        this.resultSetMetaData = null;
        this.responseStream = null;
        this.csvParser = null;
        this.currentRow = null;
        this.rowNumber = 0;
    }

    @Override
    public int getFetchSize() {
        return -1;
    }

    @Override
    public void setFetchSize(int newFetchSize) {
    }

    @Override
    public AthenaResultSetMetaData getMetaData() throws SQLException {
        if (resultSetMetaData == null) {
            try {
                GetQueryResultsResponse response = athenaClient.getQueryResults(builder -> {
                    builder.queryExecutionId(queryExecution.queryExecutionId());
                    builder.maxResults(1);
                }).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
                resultSetMetaData = new AthenaResultSetMetaData(queryExecution, response.resultSet().resultSetMetadata());
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return null;
            } catch (TimeoutException ie) {
                throw new SQLTimeoutException(ie);
            } catch (ExecutionException ee) {
                throw new SQLException(ee.getCause());
            }
        }
        return resultSetMetaData;
    }

    @Override
    public int getRowNumber() {
        return rowNumber;
    }

    @Override
    public boolean next() throws SQLException {
        if (responseStream == null) {
            AthenaResultSetMetaData md = getMetaData();
            Matcher matcher = S3_URI_PATTERN.matcher(md.getOutputLocation());
            matcher.matches();
            String bucketName = matcher.group(1);
            String key = matcher.group(2);
            responseStream = s3Client.getObject(b -> b.bucket(bucketName).key(key));
            csvParser = new VeryBasicCsvParser(new BufferedReader(new InputStreamReader(responseStream)), md.getColumnCount());
            csvParser.next();
            rowNumber = 0;
        }
        currentRow = csvParser.next();
        if (currentRow == null) {
            return false;
        } else {
            rowNumber++;
            return true;
        }
    }

    @Override
    public String getString(int columnIndex) {
        return currentRow[columnIndex - 1];
    }

    @Override
    public ResultPosition getPosition() {
        if (getRowNumber() == 0) {
            return ResultPosition.BEFORE_FIRST;
        } else if (getRowNumber() == 1) {
            return ResultPosition.FIRST;
        } else if (csvParser.hasNext()) {
            return ResultPosition.MIDDLE;
        } else if (currentRow == null) {
            return ResultPosition.AFTER_LAST;
        } else {
            return ResultPosition.LAST;
        }
    }

    @Override
    public void close() {
        s3Client = null;
        athenaClient = null;
        resultSetMetaData = null;
    }
}
