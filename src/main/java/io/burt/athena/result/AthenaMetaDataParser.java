package io.burt.athena.result;

import io.burt.athena.AthenaResultSetMetaData;
import io.burt.athena.result.protobuf.BinaryField;
import io.burt.athena.result.protobuf.Field;
import io.burt.athena.result.protobuf.IntegerField;
import io.burt.athena.result.protobuf.VeryBasicProtobufParser;
import software.amazon.awssdk.services.athena.model.ColumnInfo;
import software.amazon.awssdk.services.athena.model.ColumnNullable;
import software.amazon.awssdk.services.athena.model.QueryExecution;
import software.amazon.awssdk.services.athena.model.ResultSetMetadata;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

public class AthenaMetaDataParser {
    private static final int COLUMNS_FIELD = 4;
    private static final int CATALOG_NAME_FIELD = 1;
    private static final int NAME_FIELD = 4;
    private static final int LABEL_FIELD = 5;
    private static final int TYPE_FIELD = 6;
    private static final int PRECISION_FIELD = 7;
    private static final int SCALE_FIELD = 8;
    private static final int NULLABLE_FIELD = 9;
    private static final int CASE_SENSITIVE_FIELD = 10;

    private final QueryExecution queryExecution;

    public AthenaMetaDataParser(QueryExecution queryExecution) {
        this.queryExecution = queryExecution;
    }

    public AthenaResultSetMetaData parse(ByteBuffer buffer) {
        VeryBasicProtobufParser parser = new VeryBasicProtobufParser();
        List<Field> fields = parser.parse(buffer);
        List<ColumnInfo> columns = new LinkedList<>();
        for (Field field : fields) {
            if (field.getNumber() == COLUMNS_FIELD) {
                byte[] contents = ((BinaryField) field).getContents();
                List<Field> parse = parser.parse(contents);
                ColumnInfo columnInfo = fieldsToColumn(parse);
                columns.add(columnInfo);
            }
        }
        return new AthenaResultSetMetaData(queryExecution, ResultSetMetadata.builder().columnInfo(columns).build());
    }

    private String fieldToString(Field field) {
        return new String(((BinaryField) field).getContents(), StandardCharsets.UTF_8);
    }

    private int fieldToInt(Field field) {
        return (int) ((IntegerField) field).getValue();
    }

    private ColumnInfo fieldsToColumn(List<Field> fields) {
        ColumnInfo.Builder builder = ColumnInfo.builder();
        for (Field field : fields) {
            switch (field.getNumber()) {
                case CATALOG_NAME_FIELD:
                    builder.catalogName(fieldToString(field));
                    break;
                case NAME_FIELD:
                    builder.name(fieldToString(field));
                    break;
                case LABEL_FIELD:
                    builder.label(fieldToString(field));
                    break;
                case TYPE_FIELD:
                    builder.type(fieldToString(field));
                    break;
                case PRECISION_FIELD:
                    builder.precision(fieldToInt(field));
                    break;
                case SCALE_FIELD:
                    builder.scale(fieldToInt(field));
                    break;
                case NULLABLE_FIELD:
                    int v = fieldToInt(field);
                    if (v == 1) {
                        builder.nullable(ColumnNullable.NOT_NULL);
                    } else if (v == 2) {
                        builder.nullable(ColumnNullable.NULLABLE);
                    } else {
                        builder.nullable(ColumnNullable.UNKNOWN);
                    }
                    break;
                case CASE_SENSITIVE_FIELD:
                    builder.caseSensitive(fieldToInt(field) == 1);
                    break;
            }
        }
        return builder.build();
    }
}
