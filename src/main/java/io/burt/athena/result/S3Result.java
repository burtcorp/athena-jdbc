package io.burt.athena.result;

import io.burt.athena.AthenaResultSetMetaData;
import io.burt.athena.result.csv.VeryBasicCsvParser;
import io.burt.athena.result.protobuf.VeryBasicProtobufParser;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.athena.model.ColumnInfo;
import software.amazon.awssdk.services.athena.model.ColumnNullable;
import software.amazon.awssdk.services.athena.model.QueryExecution;
import software.amazon.awssdk.services.athena.model.ResultSetMetadata;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class S3Result implements Result {
    private static final Pattern S3_URI_PATTERN = Pattern.compile("^s3://([^/]+)/(.+)$");

    private final QueryExecution queryExecution;
    private final S3Client s3Client;
    private final String bucketName;
    private final String key;

    private AthenaResultSetMetaData resultSetMetaData;
    private ResponseInputStream<GetObjectResponse> responseStream;
    private VeryBasicCsvParser csvParser;
    private String[] currentRow;
    private int rowNumber;

    public S3Result(S3Client s3Client, QueryExecution queryExecution, Duration timeout) {
        this.s3Client = s3Client;
        this.queryExecution = queryExecution;
        this.resultSetMetaData = null;
        this.responseStream = null;
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

    @Override
    public AthenaResultSetMetaData getMetaData() throws SQLException {
        if (resultSetMetaData == null) {
            InputStream stream = s3Client.getObject(b -> b.bucket(bucketName).key(key + ".metadata"));
            VeryBasicProtobufParser parser = new VeryBasicProtobufParser();
            try {
                List<ColumnInfo> columns = new LinkedList<>();
                for (VeryBasicProtobufParser.Field field : parser.parse(stream)) {
                    if (field.getNumber() == 4) {
                        byte[] contents = ((VeryBasicProtobufParser.BinaryField) field).getContents();
                        List<VeryBasicProtobufParser.Field> parse = parser.parse(contents);
                        ColumnInfo columnInfo = fieldsToColumn(parse);
                        columns.add(columnInfo);
                    }
                }
                resultSetMetaData = new AthenaResultSetMetaData(queryExecution, ResultSetMetadata.builder().columnInfo(columns).build());
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }
        return resultSetMetaData;
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
        if (responseStream == null) {
            getMetaData();
            responseStream = s3Client.getObject(b -> b.bucket(bucketName).key(key));
            csvParser = new VeryBasicCsvParser(new BufferedReader(new InputStreamReader(responseStream)), resultSetMetaData.getColumnCount());
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
    }
}
