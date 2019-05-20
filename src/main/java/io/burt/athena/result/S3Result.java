package io.burt.athena.result;

import io.burt.athena.AthenaResultSetMetaData;
import io.burt.athena.result.csv.VeryBasicCsvParser;
import io.burt.athena.result.s3.ByteBufferResponseTransformer;
import io.burt.athena.result.s3.InputStreamResponseTransformer;
import software.amazon.awssdk.services.athena.model.QueryExecution;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class S3Result implements Result {
    private static final Pattern S3_URI_PATTERN = Pattern.compile("^s3://([^/]+)/(.+)$");

    private final QueryExecution queryExecution;
    private final S3AsyncClient s3Client;
    private final String bucketName;
    private final String key;
    private final Duration timeout;

    private AthenaResultSetMetaData resultSetMetaData;
    private Iterator<String[]> csvParser;
    private String[] currentRow;
    private int rowNumber;

    public S3Result(S3AsyncClient s3Client, QueryExecution queryExecution, Duration timeout) {
        this.s3Client = s3Client;
        this.queryExecution = queryExecution;
        this.timeout = timeout;
        this.resultSetMetaData = null;
        this.csvParser = null;
        this.currentRow = null;
        this.rowNumber = 0;
        Matcher matcher = S3_URI_PATTERN.matcher(queryExecution.resultConfiguration().outputLocation());
        if (matcher.matches()) {
            this.bucketName = matcher.group(1);
            this.key = matcher.group(2);
        } else {
            throw new IllegalArgumentException(String.format("The output location \"%s\" is malformed", queryExecution.resultConfiguration().outputLocation()));
        }
    }

    @Override
    public int getFetchSize() {
        return -1;
    }

    @Override
    public void setFetchSize(int newFetchSize) {
    }

    private void start() throws ExecutionException, TimeoutException, InterruptedException {
        AthenaMetaDataParser metaDataParser = new AthenaMetaDataParser(queryExecution);
        CompletableFuture<AthenaResultSetMetaData> metadataFuture = s3Client.getObject(b -> b.bucket(bucketName).key(key + ".metadata"), new ByteBufferResponseTransformer()).thenApply(metaDataParser::parse);
        CompletableFuture<InputStream> responseStreamFuture = s3Client.getObject(b -> b.bucket(bucketName).key(key), new InputStreamResponseTransformer());
        CompletableFuture<Iterator<String[]>> combinedFuture = metadataFuture.thenCombine(responseStreamFuture, (metaData, responseStream) -> new VeryBasicCsvParser(new BufferedReader(new InputStreamReader(responseStream)), metaData.getColumnCount()));
        csvParser = combinedFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        csvParser.next();
        rowNumber = 0;
        resultSetMetaData = metadataFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public AthenaResultSetMetaData getMetaData() throws SQLException {
        if (resultSetMetaData == null) {
            try {
                start();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            } catch (ExecutionException e) {
                throw new SQLException(e.getCause());
            } catch (TimeoutException e) {
                throw new SQLTimeoutException(e);
            } catch (NoSuchKeyException e) {
                throw new SQLException(e);
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
        if (resultSetMetaData == null) {
            try {
                start();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } catch (ExecutionException e) {
                throw new SQLException(e.getCause());
            } catch (TimeoutException e) {
                throw new SQLTimeoutException(e);
            } catch (NoSuchKeyException e) {
                throw new SQLException(e);
            }
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
    }
}
