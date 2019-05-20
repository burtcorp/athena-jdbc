package io.burt.athena.result;

import io.burt.athena.AthenaResultSetMetaData;
import io.burt.athena.result.csv.VeryBasicCsvParser;
import io.burt.athena.result.protobuf.VeryBasicProtobufParser;
import io.burt.athena.result.s3.ByteBufferResponseTransformer;
import io.burt.athena.result.s3.InputStreamResponseTransformer;
import software.amazon.awssdk.services.athena.model.ColumnInfo;
import software.amazon.awssdk.services.athena.model.ColumnNullable;
import software.amazon.awssdk.services.athena.model.QueryExecution;
import software.amazon.awssdk.services.athena.model.ResultSetMetadata;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class S3Result implements Result {
    private static final Pattern S3_URI_PATTERN = Pattern.compile("^s3://([^/]+)/(.+)$");

    private final QueryExecution queryExecution;
    private final S3AsyncClient s3Client;
    private final String bucketName;
    private final String key;

    private AthenaResultSetMetaData resultSetMetaData;
    private Iterator<String[]> csvParser;
    private String[] currentRow;
    private int rowNumber;

    public S3Result(S3AsyncClient s3Client, QueryExecution queryExecution, Duration timeout) {
        this.s3Client = s3Client;
        this.queryExecution = queryExecution;
        this.resultSetMetaData = null;
        this.csvParser = null;
        this.currentRow = null;
        this.rowNumber = 0;
        Matcher matcher = S3_URI_PATTERN.matcher(queryExecution.resultConfiguration().outputLocation());
        matcher.matches();
        this.bucketName = matcher.group(1);
        this.key = matcher.group(2);
    }

    @Override
    public int getFetchSize() {
        return -1;
    }

    @Override
    public void setFetchSize(int newFetchSize) {
    }

    private void start() throws ExecutionException, InterruptedException {
        CompletableFuture<AthenaResultSetMetaData> metadataFuture = s3Client.getObject(b -> b.bucket(bucketName).key(key + ".metadata"), new ByteBufferResponseTransformer()).thenApply(this::parseResultSetMetadata);
        CompletableFuture<InputStream> responseStreamFuture = s3Client.getObject(b -> b.bucket(bucketName).key(key), new InputStreamResponseTransformer());
        CompletableFuture<Iterator<String[]>> combinedFuture = metadataFuture.thenCombine(responseStreamFuture, (metaData, responseStream) -> new VeryBasicCsvParser(new BufferedReader(new InputStreamReader(responseStream)), metaData.getColumnCount()));
        csvParser = combinedFuture.get();
        csvParser.next();
        rowNumber = 0;
        resultSetMetaData = metadataFuture.get();
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
            }
        }
        return resultSetMetaData;
    }

    private AthenaResultSetMetaData parseResultSetMetadata(ByteBuffer buffer) {
        VeryBasicProtobufParser parser = new VeryBasicProtobufParser();
        List<VeryBasicProtobufParser.Field> fields = parser.parse(buffer);
        List<ColumnInfo> columns = new LinkedList<>();
        for (VeryBasicProtobufParser.Field field : fields) {
            if (field.getNumber() == 4) {
                byte[] contents = ((VeryBasicProtobufParser.BinaryField) field).getContents();
                List<VeryBasicProtobufParser.Field> parse = parser.parse(contents);
                ColumnInfo columnInfo = fieldsToColumn(parse);
                columns.add(columnInfo);
            }
        }
        return new AthenaResultSetMetaData(queryExecution, ResultSetMetadata.builder().columnInfo(columns).build());
    }

    private String fieldToString(VeryBasicProtobufParser.Field field) {
        return new String(((VeryBasicProtobufParser.BinaryField) field).getContents(), StandardCharsets.UTF_8);
    }

    private int fieldToInt(VeryBasicProtobufParser.Field field) {
        return (int) ((VeryBasicProtobufParser.IntegerField) field).getValue();
    }

    private ColumnInfo fieldsToColumn(List<VeryBasicProtobufParser.Field> fields) {
        ColumnInfo.Builder builder = ColumnInfo.builder();
        for (VeryBasicProtobufParser.Field field : fields) {
            switch (field.getNumber()) {
                case 1:
                    builder.catalogName(fieldToString(field));
                    break;
                case 4:
                    builder.name(fieldToString(field));
                    break;
                case 5:
                    builder.label(fieldToString(field));
                    break;
                case 6:
                    builder.type(fieldToString(field));
                    break;
                case 7:
                    builder.precision(fieldToInt(field));
                    break;
                case 8:
                    builder.scale(fieldToInt(field));
                    break;
                case 9:
                    int v = fieldToInt(field);
                    if (v == 1) {
                        builder.nullable(ColumnNullable.NOT_NULL);
                    } else if (v == 2) {
                        builder.nullable(ColumnNullable.NULLABLE);
                    } else {
                        builder.nullable(ColumnNullable.UNKNOWN);
                    }
                    break;
                case 10:
                    builder.caseSensitive(fieldToInt(field) == 1);
                    break;
            }
        }
        return builder.build();
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
