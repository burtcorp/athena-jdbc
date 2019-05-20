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
    private final QueryExecution queryExecution;

    public AthenaMetaDataParser(QueryExecution queryExecution) {
        this.queryExecution = queryExecution;
    }

    public AthenaResultSetMetaData parse(ByteBuffer buffer) {
        VeryBasicProtobufParser parser = new VeryBasicProtobufParser();
        List<Field> fields = parser.parse(buffer);
        List<ColumnInfo> columns = new LinkedList<>();
            if (field.getNumber() == 4) {
        for (Field field : fields) {
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

}
