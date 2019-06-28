package io.burt.athena.result;

import io.burt.athena.AthenaResultSetMetaData;
import io.burt.athena.support.TestNameGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.athena.model.ColumnInfo;
import software.amazon.awssdk.services.athena.model.ColumnNullable;
import software.amazon.awssdk.services.athena.model.QueryExecution;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSetMetaData;
import java.sql.Types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(TestNameGenerator.class)
class AthenaMetaDataParserTest {
    private AthenaMetaDataParser parser;

    private void putString(ByteBuffer buffer, int n, String str) {
        if (str != null) {
            buffer.put((byte) (n << 3 | 2));
            buffer.put((byte) str.length());
            buffer.put(str.getBytes(StandardCharsets.UTF_8));
        }
    }

    private void putInteger(ByteBuffer buffer, int n, Integer i) {
        if (i != null) {
            buffer.put((byte) (n << 3));
            buffer.put(i.byteValue());
        }
    }

    private ByteBuffer createMetaData(ColumnInfo... columns) {
        ByteBuffer outerBuffer = ByteBuffer.allocate(1 << 16);
        outerBuffer.put((byte) (1 << 3 | 2));
        outerBuffer.put((byte) 5);
        outerBuffer.put("fnord".getBytes());
        for (ColumnInfo column : columns) {
            ByteBuffer innerBuffer = ByteBuffer.allocate(1 << 12);
            putString(innerBuffer, 1, column.catalogName());
            putString(innerBuffer, 4, column.name());
            putString(innerBuffer, 5, column.label());
            putString(innerBuffer, 6, column.type());
            putInteger(innerBuffer, 7, column.precision());
            putInteger(innerBuffer, 8, column.scale());
            putInteger(innerBuffer, 9, column.nullable() == null ? null : column.nullable().ordinal() + 1);
            putInteger(innerBuffer, 10, column.caseSensitive() == null ? null : (column.caseSensitive() ? 1 : 0));
            ((ByteBuffer) innerBuffer).flip();
            outerBuffer.put((byte) (4 << 3 | 2));
            outerBuffer.put((byte) innerBuffer.remaining());
            outerBuffer.put(innerBuffer);
        }
        ((ByteBuffer) outerBuffer).flip();
        return outerBuffer;
    }

    @BeforeEach
    void setUp() {
        parser = new AthenaMetaDataParser(QueryExecution.builder().build());
    }

    @Nested
    class Parse {
        @Nested
        class WithNoColumns {
            @Test
            void returnsNoColumns() {
                AthenaResultSetMetaData metaData = parser.parse(createMetaData());
                assertEquals(0, metaData.getColumnCount());
            }
        }

        @Nested
        class WithOneColumn {
            @Test
            void returnsOneColumn() {
                AthenaResultSetMetaData metaData = parser.parse(createMetaData(ColumnInfo.builder().label("col_label").name("col_name").type("varchar").build()));
                assertEquals(1, metaData.getColumnCount());
                assertEquals("col_label", metaData.getColumnLabel(1));
                assertEquals("col_name", metaData.getColumnName(1));
                assertEquals(Types.VARCHAR, metaData.getColumnType(1));
            }
        }

        @Nested
        class WithManyColumns {
            @Test
            void parsesMetaDataWithManyColumns() {
                AthenaResultSetMetaData metaData = parser.parse(createMetaData(
                        ColumnInfo.builder().label("col1_label").name("col1_name").type("varchar").build(),
                        ColumnInfo.builder().label("col2_label").name("col2_name").type("bigint").build(),
                        ColumnInfo.builder().label("col3_label").name("col3_name").type("boolean").build(),
                        ColumnInfo.builder().label("col4_label").name("col4_name").type("date").build()
                ));
                assertEquals(4, metaData.getColumnCount());
                assertEquals("col1_label", metaData.getColumnLabel(1));
                assertEquals("col1_name", metaData.getColumnName(1));
                assertEquals(Types.VARCHAR, metaData.getColumnType(1));
                assertEquals("col2_label", metaData.getColumnLabel(2));
                assertEquals("col2_name", metaData.getColumnName(2));
                assertEquals(Types.BIGINT, metaData.getColumnType(2));
                assertEquals("col3_label", metaData.getColumnLabel(3));
                assertEquals("col3_name", metaData.getColumnName(3));
                assertEquals(Types.BOOLEAN, metaData.getColumnType(3));
                assertEquals("col4_label", metaData.getColumnLabel(4));
                assertEquals("col4_name", metaData.getColumnName(4));
                assertEquals(Types.DATE, metaData.getColumnType(4));
            }
        }

        @Nested
        class WithACatalogName {
            @Test
            void returnsColumnsWithACatalogName() {
                AthenaResultSetMetaData metaData = parser.parse(createMetaData(ColumnInfo.builder().catalogName("catalog1").build()));
                assertEquals("catalog1", metaData.getCatalogName(1));
            }
        }

        @Nested
        class WithPrecisionAndScale {
            @Test
            void returnsColumnsWithPrecisionAndScale() {
                AthenaResultSetMetaData metaData = parser.parse(createMetaData(ColumnInfo.builder().precision(3).scale(4).build()));
                assertEquals(3, metaData.getPrecision(1));
                assertEquals(4, metaData.getScale(1));
            }
        }

        @Nested
        class WithNullability {
            @Test
            void returnsColumnsWithPrecisionAndScale() {
                AthenaResultSetMetaData metaData = parser.parse(createMetaData(
                        ColumnInfo.builder().nullable(ColumnNullable.NULLABLE).build(),
                        ColumnInfo.builder().nullable(ColumnNullable.NOT_NULL).build(),
                        ColumnInfo.builder().nullable(ColumnNullable.UNKNOWN).build()
                ));
                assertEquals(ResultSetMetaData.columnNullable, metaData.isNullable(1));
                assertEquals(ResultSetMetaData.columnNoNulls, metaData.isNullable(2));
                assertEquals(ResultSetMetaData.columnNullableUnknown, metaData.isNullable(3));
            }
        }

        @Nested
        class WithCaseSensitivity {
            @Test
            void returnsColumnsWithPrecisionAndScale() {
                AthenaResultSetMetaData metaData = parser.parse(createMetaData(
                        ColumnInfo.builder().caseSensitive(true).build(),
                        ColumnInfo.builder().caseSensitive(false).build()
                ));
                assertTrue(metaData.isCaseSensitive(1));
                assertFalse(metaData.isCaseSensitive(2));
            }
        }
    }
}