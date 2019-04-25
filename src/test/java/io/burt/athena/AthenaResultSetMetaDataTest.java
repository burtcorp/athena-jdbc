package io.burt.athena;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.athena.model.ColumnInfo;
import software.amazon.awssdk.services.athena.model.ColumnNullable;
import software.amazon.awssdk.services.athena.model.ResultSetMetadata;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class AthenaResultSetMetaDataTest {
    private ResultSetMetaData metaData;

    @BeforeEach
    void setUp() {
        metaData = createMetaData(
                createColumn(cb -> cb.label("col1_label").name("col1_name").type("varchar").tableName("table1").schemaName("schema1").catalogName("catalog1").precision(0).scale(0)),
                createColumn(cb -> cb.label("col2_label").name("col2_name").type("bigint").tableName("table1").schemaName("schema1").catalogName("catalog1").precision(17).scale(3)),
                createColumn(cb -> cb.label("col3_label").name("col3_name").type("varchar").tableName("table2").schemaName("schema2").catalogName("catalog2").precision(0).scale(0))
        );
    }

    private ColumnInfo createColumn(Consumer<ColumnInfo.Builder> factory) {
        ColumnInfo.Builder builder = ColumnInfo.builder();
        factory.accept(builder);
        return builder.build();
    }

    private ResultSetMetaData createMetaData(Consumer<ColumnInfo.Builder> factory) {
        return createMetaData(createColumn(factory));
    }

    private ResultSetMetaData createMetaData(ColumnInfo... columnInfos) {
        return new AthenaResultSetMetaData(ResultSetMetadata.builder().columnInfo(columnInfos).build());
    }

    @Nested
    class GetColumnCount {
        @Test
        void returnsTheNumberOfColumns() throws Exception {
            assertEquals(3, metaData.getColumnCount());
        }
    }

    @Nested
    class GetColumnLabel {
        @Test
        void returnsTheColumnLabel() throws Exception {
            assertEquals("col1_label", metaData.getColumnLabel(1));
            assertEquals("col3_label", metaData.getColumnLabel(3));
        }

        @Nested
        class WhenOutOfBounds {
            @Test
            void throwsOutOfBoundsException() {
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.getColumnLabel(-1));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.getColumnLabel(0));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.getColumnLabel(metaData.getColumnCount() + 1));
            }
        }
    }

    @Nested
    class GetColumnName {
        @Test
        void returnsTheColumnName() throws Exception {
            assertEquals("col1_name", metaData.getColumnName(1));
            assertEquals("col3_name", metaData.getColumnName(3));
        }

        @Nested
        class WhenOutOfBounds {
            @Test
            void throwsOutOfBoundsException() {
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.getColumnName(-1));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.getColumnName(0));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.getColumnName(metaData.getColumnCount() + 1));
            }
        }
    }

    @Nested
    class GetTableName {
        @Test
        void returnsTheTableName() throws Exception {
            assertEquals("table1", metaData.getTableName(1));
            assertEquals("table2", metaData.getTableName(3));
        }

        @Nested
        class WhenOutOfBounds {
            @Test
            void throwsOutOfBoundsException() {
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.getTableName(-1));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.getTableName(0));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.getTableName(metaData.getColumnCount() + 1));
            }
        }
    }

    @Nested
    class GetSchemaName {
        @Test
        void returnsTheTableName() throws Exception {
            assertEquals("schema1", metaData.getSchemaName(1));
            assertEquals("schema2", metaData.getSchemaName(3));
        }


        @Nested
        class WhenOutOfBounds {
            @Test
            void throwsOutOfBoundsException() {
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.getSchemaName(-1));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.getSchemaName(0));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.getSchemaName(metaData.getColumnCount() + 1));
            }
        }
    }

    @Nested
    class GetCatalogName {
        @Test
        void returnsTheTableName() throws Exception {
            assertEquals("catalog1", metaData.getCatalogName(1));
            assertEquals("catalog2", metaData.getCatalogName(3));
        }

        @Nested
        class WhenOutOfBounds {
            @Test
            void throwsOutOfBoundsException() {
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.getCatalogName(-1));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.getCatalogName(0));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.getCatalogName(metaData.getColumnCount() + 1));
            }
        }
    }

    @Nested
    class GetPrecision {
        @Test
        void returnsThePrecision() throws Exception {
            assertEquals(0, metaData.getPrecision(1));
            assertEquals(17, metaData.getPrecision(2));
        }

        @Nested
        class WhenOutOfBounds {
            @Test
            void throwsOutOfBoundsException() {
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.getPrecision(-1));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.getPrecision(0));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.getPrecision(metaData.getColumnCount() + 1));
            }
        }
    }

    @Nested
    class GetScale {
        @Test
        void returnsTheScale() throws Exception {
            assertEquals(0, metaData.getScale(1));
            assertEquals(3, metaData.getScale(2));
        }

        @Nested
        class WhenOutOfBounds {
            @Test
            void throwsOutOfBoundsException() {
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.getScale(-1));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.getScale(0));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.getScale(metaData.getColumnCount() + 1));
            }
        }
    }

    @Nested
    class GetColumnType {
        @Test
        void returnsTINYINTForTinyint() throws Exception {
            ResultSetMetaData md = createMetaData(cib -> cib.type("tinyint"));
            assertEquals(Types.TINYINT, md.getColumnType(1));
        }

        @Test
        void returnsSMALLINTForSmallint() throws Exception {
            ResultSetMetaData md = createMetaData(cib -> cib.type("smallint"));
            assertEquals(Types.SMALLINT, md.getColumnType(1));
        }

        @Test
        void returnsINTEGERForInteger() throws Exception {
            ResultSetMetaData md = createMetaData(cib -> cib.type("integer"));
            assertEquals(Types.INTEGER, md.getColumnType(1));
        }

        @Test
        void returnsBIGINTForBigint() throws Exception {
            ResultSetMetaData md = createMetaData(cib -> cib.type("bigint"));
            assertEquals(Types.BIGINT, md.getColumnType(1));
        }

        @Test
        void returnsFLOATForFloat() throws Exception {
            ResultSetMetaData md = createMetaData(cib -> cib.type("float"));
            assertEquals(Types.FLOAT, md.getColumnType(1));
        }

        @Test
        void returnsDOUBLEForDouble() throws Exception {
            ResultSetMetaData md = createMetaData(cib -> cib.type("double"));
            assertEquals(Types.DOUBLE, md.getColumnType(1));
        }

        @Test
        void returnsDECIMALForDecimal() throws Exception {
            ResultSetMetaData md = createMetaData(cib -> cib.type("decimal"));
            assertEquals(Types.DECIMAL, md.getColumnType(1));
        }

        @Test
        void returnsBOOLEANForBoolean() throws Exception {
            ResultSetMetaData md = createMetaData(cib -> cib.type("boolean"));
            assertEquals(Types.BOOLEAN, md.getColumnType(1));
        }

        @Test
        void returnsCHARForChar() throws Exception {
            ResultSetMetaData md = createMetaData(cib -> cib.type("char"));
            assertEquals(Types.CHAR, md.getColumnType(1));
        }

        @Test
        void returnsVARCHARForVarchar() throws Exception {
            ResultSetMetaData md = createMetaData(cib -> cib.type("varchar"));
            assertEquals(Types.VARCHAR, md.getColumnType(1));
        }

        @Test
        void returnsVARBINARYForVarbinary() throws Exception {
            ResultSetMetaData md = createMetaData(cib -> cib.type("varbinary"));
            assertEquals(Types.VARBINARY, md.getColumnType(1));
        }

        @Test
        void returnsVARCHARForJson() throws Exception {
            ResultSetMetaData md = createMetaData(cib -> cib.type("json"));
            assertEquals(Types.VARCHAR, md.getColumnType(1));
        }

        @Test
        void returnsDATEForDate() throws Exception {
            ResultSetMetaData md = createMetaData(cib -> cib.type("date"));
            assertEquals(Types.DATE, md.getColumnType(1));
        }

        @Test
        void returnsTIMEForTime() throws Exception {
            ResultSetMetaData md = createMetaData(cib -> cib.type("time"));
            assertEquals(Types.TIME, md.getColumnType(1));
        }

        @Test
        void returnsTIME_WITH_TIMEZONEForTimeWithTimeZone() throws Exception {
            ResultSetMetaData md = createMetaData(cib -> cib.type("time with time zone"));
            assertEquals(Types.TIME_WITH_TIMEZONE, md.getColumnType(1));
        }

        @Test
        void returnsTIMESTAMPForTimestamp() throws Exception {
            ResultSetMetaData md = createMetaData(cib -> cib.type("timestamp"));
            assertEquals(Types.TIMESTAMP, md.getColumnType(1));
        }

        @Test
        void returnsTIMESTAMP_WITH_TIMEZONEForTimestampWithTimeZone() throws Exception {
            ResultSetMetaData md = createMetaData(cib -> cib.type("timestamp with time zone"));
            assertEquals(Types.TIMESTAMP_WITH_TIMEZONE, md.getColumnType(1));
        }

        @Test
        void returnsBIGINTForIntervalDayToSecond() throws Exception {
            ResultSetMetaData md = createMetaData(cib -> cib.type("interval day to second"));
            assertEquals(Types.VARCHAR, md.getColumnType(1));
        }

        @Test
        void returnsBIGINTForIntervalYearToMonth() throws Exception {
            ResultSetMetaData md = createMetaData(cib -> cib.type("interval year to month"));
            assertEquals(Types.VARCHAR, md.getColumnType(1));
        }

        @Test
        void returnsARRAYForArray() throws Exception {
            ResultSetMetaData md = createMetaData(cib -> cib.type("array"));
            assertEquals(Types.ARRAY, md.getColumnType(1));
        }

        @Test
        void returnsSTRUCTForMap() throws Exception {
            ResultSetMetaData md = createMetaData(cib -> cib.type("map"));
            assertEquals(Types.STRUCT, md.getColumnType(1));
        }

        @Test
        void returnsSTRUCTForRow() throws Exception {
            ResultSetMetaData md = createMetaData(cib -> cib.type("row"));
            assertEquals(Types.STRUCT, md.getColumnType(1));
        }

        @Test
        void returnsOTHERForAnyOtherType() throws Exception {
            ResultSetMetaData md = createMetaData(cib -> cib.type("fnord"));
            assertEquals(Types.OTHER, md.getColumnType(1));
        }

        @Nested
        class WhenOutOfBounds {
            @Test
            void throwsOutOfBoundsException() {
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.getColumnType(-1));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.getColumnType(0));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.getColumnType(metaData.getColumnCount() + 1));
            }
        }
    }

    @Nested
    class GetColumnTypeName {
        @Test
        void returnsTheAthenaName() throws Exception {
            ResultSetMetaData md = createMetaData(
                    createColumn(cib -> cib.type("varchar")),
                    createColumn(cib -> cib.type("date")),
                    createColumn(cib -> cib.type("bigint"))
            );
            assertEquals("varchar", md.getColumnTypeName(1));
            assertEquals("date", md.getColumnTypeName(2));
            assertEquals("bigint", md.getColumnTypeName(3));
        }

        @Nested
        class WhenOutOfBounds {
            @Test
            void throwsOutOfBoundsException() {
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.getColumnTypeName(-1));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.getColumnTypeName(0));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.getColumnTypeName(metaData.getColumnCount() + 1));
            }
        }
    }

    @Nested
    class GetColumnClassName {
        @Test
        void returnsStringForVarchar() throws Exception {
            assertEquals("java.lang.String", metaData.getColumnClassName(1));
        }

        @Test
        void returnsObjectByDefault() throws Exception {
            assertEquals("java.lang.Object", metaData.getColumnClassName(2));
        }

        @Nested
        class WhenOutOfBounds {
            @Test
            void throwsOutOfBoundsException() {
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.getColumnClassName(-1));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.getColumnClassName(0));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.getColumnClassName(metaData.getColumnCount() + 1));
            }
        }
    }

    @Nested
    class GetColumnDisplaySize {
        @Test
        void isNotSupported() {
            assertThrows(SQLFeatureNotSupportedException.class, () -> metaData.getColumnDisplaySize(1));
        }
    }

    @Nested
    class Unwrap {
        @Test
        void returnsTypedInstance() throws SQLException {
            AthenaResultSetMetaData arsmd = metaData.unwrap(AthenaResultSetMetaData.class);
            assertNotNull(arsmd);
        }

        @Test
        void throwsWhenAskedToUnwrapClassItIsNotWrapperFor() {
            assertThrows(SQLException.class, () -> metaData.unwrap(String.class));
        }
    }

    @Nested
    class IsWrapperFor {
        @Test
        void isWrapperForAthenaResultSetMetadata() throws Exception {
            assertTrue(metaData.isWrapperFor(AthenaResultSetMetaData.class));
        }

        @Test
        void isWrapperForObject() throws Exception {
            assertTrue(metaData.isWrapperFor(Object.class));
        }

        @Test
        void isNotWrapperForOtherClasses() throws Exception {
            assertFalse(metaData.isWrapperFor(String.class));
        }
    }

    @Nested
    class IsAutoIncrement {
        @Test
        void alwaysReturnsFalse() throws Exception {
            assertFalse(metaData.isAutoIncrement(1));
            assertFalse(metaData.isAutoIncrement(2));
        }

        @Nested
        class WhenOutOfBounds {
            @Test
            void throwsOutOfBoundsException() {
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.isAutoIncrement(-1));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.isAutoIncrement(0));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.isAutoIncrement(metaData.getColumnCount() + 1));
            }
        }
    }

    @Nested
    class IsCaseSensitive {
        @Test
        void returnsTheCaseSensitivityOfTheColumn() throws Exception {
            ResultSetMetaData md = createMetaData(
                    createColumn(cib -> cib.caseSensitive(false)),
                    createColumn(cib -> cib.caseSensitive(true))
            );
            assertFalse(md.isCaseSensitive(1));
            assertTrue(md.isCaseSensitive(2));
        }

        @Nested
        class WhenOutOfBounds {
            @Test
            void throwsOutOfBoundsException() {
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.isCaseSensitive(-1));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.isCaseSensitive(0));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.isCaseSensitive(metaData.getColumnCount() + 1));
            }
        }

    }

    @Nested
    class IsSearchable {
        @Test
        void alwaysReturnsTrue() throws Exception {
            assertTrue(metaData.isSearchable(1));
            assertTrue(metaData.isSearchable(2));
        }

        @Nested
        class WhenOutOfBounds {
            @Test
            void throwsOutOfBoundsException() {
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.isSearchable(-1));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.isSearchable(0));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.isSearchable(metaData.getColumnCount() + 1));
            }
        }
    }

    @Nested
    class IsCurrency {
        @Test
        void alwaysReturnsFalse() throws Exception {
            assertFalse(metaData.isCurrency(1));
            assertFalse(metaData.isCurrency(2));
        }

        @Nested
        class WhenOutOfBounds {
            @Test
            void throwsOutOfBoundsException() {
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.isCurrency(-1));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.isCurrency(0));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.isCurrency(metaData.getColumnCount() + 1));
            }
        }
    }

    @Nested
    class IsNullable {
        @Test
        void returnsNoNullsWhenNotNullable() throws Exception {
            ResultSetMetaData md = createMetaData(cib -> cib.nullable(ColumnNullable.NOT_NULL));
            assertEquals(ResultSetMetaData.columnNoNulls, md.isNullable(1));
        }

        @Test
        void returnsNullableWhenNullable() throws Exception {
            ResultSetMetaData md = createMetaData(cib -> cib.nullable(ColumnNullable.NULLABLE));
            assertEquals(ResultSetMetaData.columnNullable, md.isNullable(1));
        }

        @Test
        void returnsUnknownWhenUnknown() throws Exception {
            ResultSetMetaData md = createMetaData(cib -> cib.nullable(ColumnNullable.UNKNOWN));
            assertEquals(ResultSetMetaData.columnNullableUnknown, md.isNullable(1));
        }

        @Nested
        class WhenOutOfBounds {
            @Test
            void throwsOutOfBoundsException() {
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.isNullable(-1));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.isNullable(0));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.isNullable(metaData.getColumnCount() + 1));
            }
        }
    }

    @Nested
    class IsSigned {
        @Test
        void returnsTrueForNumbers() throws Exception {
            List<String> numericTypes = Arrays.asList(
                    "tinyint",
                    "smallint",
                    "integer",
                    "bigint",
                    "float",
                    "double",
                    "decimal"
            );
            for (String type : numericTypes) {
                ResultSetMetaData md = createMetaData(cib -> cib.type(type));
                assertTrue(md.isSigned(1));
            }
        }

        @Test
        void returnsFalseForNonNumbers() throws Exception {
            List<String> nonNumericTypes = Arrays.asList(
                    "boolean",
                    "char",
                    "varchar",
                    "varbinary",
                    "json",
                    "date",
                    "time",
                    "timestamp",
                    "time with time zone",
                    "timestamp with time zone",
                    "interval day to second",
                    "interval year to month",
                    "array",
                    "map",
                    "row"
            );
            for (String type : nonNumericTypes) {
                ResultSetMetaData md = createMetaData(cib -> cib.type(type));
                assertFalse(md.isSigned(1));
            }
        }

        @Nested
        class WhenOutOfBounds {
            @Test
            void throwsOutOfBoundsException() {
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.isSigned(-1));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.isSigned(0));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.isSigned(metaData.getColumnCount() + 1));
            }
        }
    }

    @Nested
    class IsReadonly {
        @Test
        void alwaysReturnsTrue() throws Exception {
            assertTrue(metaData.isReadOnly(1));
            assertTrue(metaData.isReadOnly(2));
        }

        @Nested
        class WhenOutOfBounds {
            @Test
            void throwsOutOfBoundsException() {
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.isReadOnly(-1));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.isReadOnly(0));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.isReadOnly(metaData.getColumnCount() + 1));
            }
        }
    }

    @Nested
    class IsWritable {
        @Test
        void alwaysReturnsFalse() throws Exception {
            assertFalse(metaData.isWritable(1));
            assertFalse(metaData.isWritable(2));
        }

        @Nested
        class WhenOutOfBounds {
            @Test
            void throwsOutOfBoundsException() {
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.isWritable(-1));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.isWritable(0));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.isWritable(metaData.getColumnCount() + 1));
            }
        }
    }

    @Nested
    class IsDefinitelyWritable {
        @Test
        void alwaysReturnsFalse() throws Exception {
            assertFalse(metaData.isDefinitelyWritable(1));
            assertFalse(metaData.isDefinitelyWritable(2));
        }

        @Nested
        class WhenOutOfBounds {
            @Test
            void throwsOutOfBoundsException() {
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.isDefinitelyWritable(-1));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.isDefinitelyWritable(0));
                assertThrows(IndexOutOfBoundsException.class, () -> metaData.isDefinitelyWritable(metaData.getColumnCount() + 1));
            }
        }
    }
}
