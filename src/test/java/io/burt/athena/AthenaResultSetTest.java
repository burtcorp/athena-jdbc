package io.burt.athena;

import io.burt.athena.support.GetQueryResultsHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.athena.AthenaAsyncClient;
import software.amazon.awssdk.services.athena.model.ColumnInfo;
import software.amazon.awssdk.services.athena.model.GetQueryResultsRequest;
import software.amazon.awssdk.services.athena.model.Row;

import java.io.ByteArrayInputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.TimeZone;
import java.util.function.Consumer;

import static io.burt.athena.support.GetQueryResultsHelper.createColumn;
import static io.burt.athena.support.GetQueryResultsHelper.createRow;
import static io.burt.athena.support.GetQueryResultsHelper.createRowWithNull;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class AthenaResultSetTest {
    @Mock private AthenaStatement parentStatement;

    @Captor private ArgumentCaptor<Consumer<GetQueryResultsRequest.Builder>> getQueryResultsCaptor;

    private AthenaResultSet resultSet;
    private GetQueryResultsHelper queryResultsHelper;

    @BeforeEach
    void setUpResultSet() {
        queryResultsHelper = new GetQueryResultsHelper();
        resultSet = new AthenaResultSet(queryResultsHelper, parentStatement, "Q1234");
    }

    private void noRows() {
        queryResultsHelper.update(Arrays.asList(
                createColumn("col1", "string"),
                createColumn("col2", "integer")
        ), Collections.emptyList());
    }

    private void defaultRows() {
        queryResultsHelper.update(Arrays.asList(
                createColumn("col1", "string"),
                createColumn("col2", "integer")
        ), Arrays.asList(
                createRow("row1", "1"),
                createRow("row2", "2"),
                createRow("row3", "3")
        ));
    }

    @Nested
    class GetStatement {
        @Test
        void returnsTheParentStatement() throws Exception {
            assertSame(parentStatement, resultSet.getStatement());
        }
    }

    @Nested
    class QueryExecutionId {
        @Test
        void returnsTheQueryExecutionId() {
            assertEquals("Q1234", resultSet.queryExecutionId());
        }
    }

    @Nested
    class GetMetaData {
        @BeforeEach
        void setUp() {
            defaultRows();
        }

        @Test
        void returnsMetaData() throws Exception {
            resultSet.next();
            assertEquals("col1", resultSet.getMetaData().getColumnLabel(1));
        }

        @Test
        void loadsTheMetaDataIfNotLoaded() throws Exception {
            assertNotNull(resultSet.getMetaData());
            assertTrue(queryResultsHelper.requestCount() > 0);
        }

        @Test
        void doesNotLoadTheSamePageTwice() throws Exception {
            resultSet.getMetaData();
            resultSet.next();
            List<String> nextTokens = queryResultsHelper.nextTokens();
            assertEquals(new HashSet<>(nextTokens).size(), nextTokens.size());
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnError() throws Exception {
                resultSet.close();
                assertThrows(SQLException.class, () -> resultSet.getMetaData());
            }
        }
    }

    @Nested
    class Next {
        @BeforeEach
        void setUp() {
            noRows();
        }

        @Test
        void callsGetQueryResults() throws Exception {
            resultSet.next();
            GetQueryResultsRequest request = queryResultsHelper.resultsRequests().get(0);
            assertEquals("Q1234", request.queryExecutionId());
        }

        @Test
        void loadsTheMaxNumberOfRowsAllowed() throws Exception {
            resultSet.next();
            GetQueryResultsRequest request = queryResultsHelper.resultsRequests().get(0);
            assertEquals(1000, request.maxResults());
        }

        @Nested
        class WhenTheResultIsEmpty {
            @Test
            void returnsFalse() throws Exception {
                assertFalse(resultSet.next());
            }
        }

        @Nested
        class WhenTheResultHasRows {
            @BeforeEach
            void setUp() {
                defaultRows();
            }

            @Test
            void returnsTrue() throws Exception {
                assertTrue(resultSet.next());
            }

            @Test
            void skipsTheHeaderRow() throws Exception {
                resultSet.next();
                assertNotEquals("col1", resultSet.getString(1));
            }

            @Test
            void returnsEachRow() throws Exception {
                resultSet.next();
                assertEquals("row1", resultSet.getString(1));
                resultSet.next();
                assertEquals("row2", resultSet.getString(1));
                resultSet.next();
                assertEquals("row3", resultSet.getString(1));
            }

            @Nested
            class AndIsExhausted {
                @Test
                void returnsFalse() throws Exception {
                    assertTrue(resultSet.next());
                    assertTrue(resultSet.next());
                    assertTrue(resultSet.next());
                    assertFalse(resultSet.next());
                }
            }
        }

        @Nested
        class WhenTheResultHasManyPages {
            @BeforeEach
            void setUp() {
                queryResultsHelper.update(Arrays.asList(
                        createColumn("col1", "string"),
                        createColumn("col2", "integer")
                ), Arrays.asList(
                        createRow("row1", "1"),
                        createRow("row2", "2"),
                        createRow("row3", "3"),
                        createRow("row4", "4"),
                        createRow("row5", "5"),
                        createRow("row6", "6"),
                        createRow("row7", "7"),
                        createRow("row8", "8"),
                        createRow("row9", "9")
                ));
            }

            @Test
            void loadsAllPages() throws Exception {
                resultSet.setFetchSize(3);
                List<String> rows = new ArrayList<>(9);
                while (resultSet.next()) {
                    rows.add(resultSet.getString(2));
                }
                assertEquals(4, queryResultsHelper.requestCount());
                assertEquals(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9"), rows);
                assertEquals(Arrays.asList(null, "2", "3", "4"), queryResultsHelper.nextTokens());
            }

            @Test
            void stopsLoadingWhenThereAreNoMorePages() throws Exception {
                resultSet.setFetchSize(3);
                assertTrue(resultSet.relative(9));
                assertFalse(resultSet.next());
            }
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnError() throws Exception {
                resultSet.close();
                assertThrows(SQLException.class, () -> resultSet.next());
            }
        }
    }

    @Nested
    class IsBeforeFirst {
        @BeforeEach
        void setUp() {
            defaultRows();
        }

        @Test
        void returnsTrueBeforeNextIsCalled() throws Exception {
            assertTrue(resultSet.isBeforeFirst());
        }

        @Test
        void returnsFalseWhenNextHasBeenCalled() throws Exception {
            resultSet.next();
            assertFalse(resultSet.isBeforeFirst());
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnError() throws Exception {
                resultSet.close();
                assertThrows(SQLException.class, () -> resultSet.isBeforeFirst());
            }
        }
    }

    @Nested
    class IsFirst {
        @BeforeEach
        void setUp() {
            defaultRows();
        }

        @Test
        void returnsFalseBeforeNextIsCalled() throws Exception {
            assertFalse(resultSet.isFirst());
        }

        @Test
        void returnsTrueWhenNextHasBeenCalledOnce() throws Exception {
            resultSet.next();
            assertTrue(resultSet.isFirst());
        }

        @Test
        void returnsFalseWhenNextHasBeenCalledTwice() throws Exception {
            resultSet.next();
            resultSet.next();
            assertFalse(resultSet.isFirst());
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnError() throws Exception {
                resultSet.close();
                assertThrows(SQLException.class, () -> resultSet.isFirst());
            }
        }
    }

    @Nested
    class IsLast {
        @BeforeEach
        void setUp() {
            defaultRows();
        }

        @Test
        void returnsFalseBeforeNextIsCalled() throws Exception {
            assertFalse(resultSet.isLast());
        }

        @Test
        void returnsFalseWhenNotLast() throws Exception {
            resultSet.next();
            assertFalse(resultSet.isLast());
            resultSet.next();
            assertFalse(resultSet.isLast());
        }

        @Test
        void returnsTrueWhenOnLastRow() throws Exception {
            resultSet.relative(3);
            assertTrue(resultSet.isLast());
        }

        @Test
        void returnsFalseWhenAfterLastRow() throws Exception {
            resultSet.relative(4);
            assertFalse(resultSet.isLast());
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnError() throws Exception {
                resultSet.close();
                assertThrows(SQLException.class, () -> resultSet.isLast());
            }
        }
    }

    @Nested
    class IsAfterLast {
        @BeforeEach
        void setUp() {
            defaultRows();
        }

        @Test
        void returnsFalseBeforeNextIsCalled() throws Exception {
            assertFalse(resultSet.isAfterLast());
        }

        @Test
        void returnsFalseWhenNotAfterLast() throws Exception {
            resultSet.next();
            assertFalse(resultSet.isAfterLast());
            resultSet.next();
            assertFalse(resultSet.isAfterLast());
        }

        @Test
        void returnsFalseWhenOnLastRow() throws Exception {
            resultSet.relative(3);
            assertFalse(resultSet.isAfterLast());
        }

        @Test
        void returnsTrueWhenAfterLastRow() throws Exception {
            resultSet.relative(4);
            assertTrue(resultSet.isAfterLast());
            resultSet.next();
            assertTrue(resultSet.isAfterLast());
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnError() throws Exception {
                resultSet.close();
                assertThrows(SQLException.class, () -> resultSet.isAfterLast());
            }
        }
    }

    @Nested
    class GetRow {
        @BeforeEach
        void setUp() {
            defaultRows();
        }

        @Test
        void returnsZeroWhenBeforeFirstRow() throws Exception {
            assertEquals(0, resultSet.getRow());
        }

        @Test
        void returnsOneWhenOnFirstRow() throws Exception {
            resultSet.next();
            assertEquals(1, resultSet.getRow());
        }

        @Test
        void returnsTheRowNumber() throws Exception {
            resultSet.next();
            assertEquals(1, resultSet.getRow());
            resultSet.next();
            assertEquals(2, resultSet.getRow());
            resultSet.next();
            assertEquals(3, resultSet.getRow());
        }

        @Test
        void returnsZeroWhenAfterLastRow() throws Exception {
            resultSet.relative(4);
            assertEquals(0, resultSet.getRow());
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnError() throws Exception {
                resultSet.close();
                assertThrows(SQLException.class, () -> resultSet.getRow());
            }
        }
    }

    @Nested
    class First {
        @Test
        void isNotSupported() {
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.first());
        }
    }

    @Nested
    class BeforeFirst {
        @Test
        void isNotSupported() {
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.beforeFirst());
        }
    }

    @Nested
    class Last {
        @Test
        void isNotSupported() {
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.last());
        }
    }

    @Nested
    class AfterLast {
        @Test
        void isNotSupported() {
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.afterLast());
        }
    }

    @Nested
    class Absolute {
        @BeforeEach
        void setUp() {
            defaultRows();
        }

        @Test
        void movesForwardMultipleRows() throws Exception {
            resultSet.absolute(3);
            assertEquals("row3", resultSet.getString(1));
        }

        @Test
        void returnsTrueWhenNextWouldHaveReturnedTrue() throws Exception {
            assertTrue(resultSet.absolute(1));
            assertTrue(resultSet.absolute(3));
        }

        @Test
        void returnsFalseWhenNextWouldHaveReturnedFalse() throws Exception {
            assertFalse(resultSet.absolute(4));
            assertFalse(resultSet.absolute(10));
        }

        @Test
        void throwsWhenOffsetIsZero() {
            assertThrows(SQLException.class, () -> resultSet.absolute(0));
        }

        @Test
        void throwsWhenOffsetIsNegative() {
            assertThrows(SQLException.class, () -> resultSet.absolute(-3));
        }

        @Test
        void throwsWhenMovingBackwards() throws Exception {
            resultSet.absolute(3);
            assertThrows(SQLException.class, () -> resultSet.absolute(2));
        }
    }

    @Nested
    class Relative {
        @BeforeEach
        void setUp() {
            defaultRows();
        }

        @Test
        void movesForwardMultipleRows() throws Exception {
            resultSet.relative(3);
            assertEquals("row3", resultSet.getString(1));
        }

        @Test
        void returnsTrueWhenNextWouldHaveReturnedTrue() throws Exception {
            assertTrue(resultSet.relative(3));
        }

        @Test
        void returnsFalseWhenNextWouldHaveReturnedFalse() throws Exception {
            assertFalse(resultSet.relative(4));
            assertFalse(resultSet.relative(10));
        }

        @Test
        void throwsWhenOffsetIsZero() {
            assertThrows(SQLException.class, () -> resultSet.relative(0));
        }

        @Test
        void throwsWhenOffsetIsNegative() {
            assertThrows(SQLException.class, () -> resultSet.relative(-3));
        }
    }

    @Nested
    class Previous {
        @Test
        void isNotSupported() {
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.previous());
        }
    }

    @Nested
    class GetType {
        @Test
        void returnsForwardOnly() throws Exception {
            assertEquals(ResultSet.TYPE_FORWARD_ONLY, resultSet.getType());
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnError() throws Exception {
                resultSet.close();
                assertThrows(SQLException.class, () -> resultSet.getType());
            }
        }
    }

    @Nested
    class GetFetchDirection {
        @Test
        void returnsForward() throws Exception {
            assertEquals(ResultSet.FETCH_FORWARD, resultSet.getFetchDirection());
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnError() throws Exception {
                resultSet.close();
                assertThrows(SQLException.class, () -> resultSet.getFetchDirection());
            }
        }
    }

    @Nested
    class SetFetchDirection {
        @Test
        void allowsFetchForward() {
            assertDoesNotThrow(() -> resultSet.setFetchDirection(ResultSet.FETCH_FORWARD));
        }

        @Test
        void doesNotAllowAnyOtherDirection() {
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.setFetchDirection(ResultSet.FETCH_REVERSE));
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnError() throws Exception {
                resultSet.close();
                assertThrows(SQLException.class, () -> resultSet.setFetchSize(ResultSet.FETCH_FORWARD));
            }
        }
    }

    @Nested
    class GetConcurrency {
        @Test
        void returnsReadOnly() throws Exception {
            assertEquals(ResultSet.CONCUR_READ_ONLY, resultSet.getConcurrency());
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnError() throws Exception {
                resultSet.close();
                assertThrows(SQLException.class, () -> resultSet.getConcurrency());
            }
        }
    }

    @Nested
    class SetFetchSize {
        @BeforeEach
        void setUp() {
            List<ColumnInfo> columns = Arrays.asList(
                    createColumn("col1", "string"),
                    createColumn("col2", "integer")
            );
            List<Row> rows = new ArrayList<>(3000);
            for (int i = 0; i < 3000; i++) {
                rows.add(createRow("row" + i, String.valueOf(i)));
            }
            queryResultsHelper.update(columns, rows);
        }

        @Test
        void setsTheFetchSize() throws Exception {
            resultSet.setFetchSize(77);
            resultSet.next();
            assertEquals(77, queryResultsHelper.pageSizes().get(0));
        }

        @Test
        void setsTheFetchSizeOfAFutureRequest() throws Exception {
            resultSet.next();
            resultSet.setFetchSize(99);
            while (resultSet.next()) {
            }
            List<Integer> pageSizes = queryResultsHelper.pageSizes();
            assertNotEquals(99, pageSizes.get(0));
            assertEquals(99, pageSizes.get(pageSizes.size() - 1));
        }

        @Nested
        class WhenCalledWithNegativeNumber {
            @Test
            void throwsAnError() throws Exception {
                assertThrows(SQLException.class, () -> resultSet.setFetchSize(-1));
            }
        }

        @Nested
        class WhenCalledWithTooLargeNumber {
            @Test
            void throwsAnError() throws Exception {
                assertThrows(SQLException.class, () -> resultSet.setFetchSize(1001));
            }
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnError() throws Exception {
                resultSet.close();
                assertThrows(SQLException.class, () -> resultSet.setFetchSize(1));
            }
        }
    }

    @Nested
    class GetFetchSize {
        @Test
        void returnsTheDefaultFetchSize() throws Exception {
            assertTrue(resultSet.getFetchSize() > 0);
        }

        @Test
        void returnsTheConfiguredFetchSize() throws Exception {
            resultSet.setFetchSize(99);
            assertEquals(99, resultSet.getFetchSize());
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnError() throws Exception {
                resultSet.close();
                assertThrows(SQLException.class, () -> resultSet.getFetchSize());
            }
        }
    }

    @Nested
    class FindColumn {
        @BeforeEach
        void setUp() {
            defaultRows();
        }

        @Test
        void returnsTheColumnIndex() throws Exception {
            assertEquals(1, resultSet.findColumn("col1"));
            assertEquals(2, resultSet.findColumn("col2"));
        }

        @Test
        void loadsTheMetaDataIfNotLoaded() throws Exception {
            resultSet.findColumn("col1");
            assertTrue(queryResultsHelper.requestCount() > 0);
        }

        @Test
        void throwsExceptionWhenNoSuchColumnExists() {
            assertThrows(SQLException.class, () -> resultSet.findColumn("col99"));
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnError() throws Exception {
                resultSet.close();
                assertThrows(SQLException.class, () -> resultSet.findColumn("col1"));
            }
        }
    }

    @Nested
    class Unwrap {
        @Test
        void returnsTypedInstance() throws SQLException {
            AthenaResultSet ars = resultSet.unwrap(AthenaResultSet.class);
            assertNotNull(ars);
        }

        @Test
        void throwsWhenAskedToUnwrapClassItIsNotWrapperFor() {
            assertThrows(SQLException.class, () -> resultSet.unwrap(String.class));
        }
    }

    @Nested
    class IsWrapperFor {
        @Test
        void isWrapperForAthenaResultSet() throws Exception {
            assertTrue(resultSet.isWrapperFor(AthenaResultSet.class));
        }

        @Test
        void isWrapperForObject() throws Exception {
            assertTrue(resultSet.isWrapperFor(Object.class));
        }

        @Test
        void isNotWrapperForOtherClasses() throws Exception {
            assertFalse(resultSet.isWrapperFor(String.class));
        }
    }

    @Nested
    class GetHoldability {
        @Test
        void isNotSupported() {
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getHoldability());
        }
    }

    @Nested
    class GetCursorName {
        @Test
        void isNotSupported() {
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getCursorName());
        }
    }

    abstract class SharedWhenOutOfPosition<T> {
        abstract protected T get(int n) throws Exception;

        abstract protected T get(String n) throws Exception;

        @Test
        void throwsAnError() throws Exception {
            assertThrows(SQLException.class, () -> get(1));
            resultSet.next();
            assertThrows(SQLException.class, () -> get(-1));
            assertThrows(SQLException.class, () -> get(0));
            assertThrows(SQLException.class, () -> get(99));
            assertThrows(SQLException.class, () -> get("col99"));
            while (resultSet.next()) {
            }
            assertThrows(SQLException.class, () -> get(1));
        }
    }

    abstract class SharedWhenClosed<T> {
        abstract protected T get(int n) throws Exception;

        abstract protected T get(String n) throws Exception;

        @Test
        void throwsAnError() throws Exception {
            resultSet.close();
            assertThrows(SQLException.class, () -> get(1));
            assertThrows(SQLException.class, () -> get("col1"));
        }
    }

    @Nested
    class GetString {
        protected String getString(int n) throws Exception {
            return resultSet.getString(n);
        }

        protected String getString(String n) throws Exception {
            return resultSet.getString(n);
        }

        @BeforeEach
        void setUp() {
            queryResultsHelper.update(Arrays.asList(
                    createColumn("col1", "string"),
                    createColumn("col2", "integer")
            ), Arrays.asList(
                    createRow("row1", "1"),
                    createRow("row2", "2"),
                    createRow(null, null)
            ));
        }

        @Test
        void returnsTheColumnAtTheSpecifiedIndexOfTheCurrentRowAsAString() throws Exception {
            resultSet.next();
            assertEquals("row1", getString(1));
            assertEquals("1", getString(2));
            resultSet.next();
            assertEquals("row2", getString(1));
            assertEquals("2", getString(2));
        }

        @Test
        void returnsTheColumnWithTheSpecifiedNameOfTheCurrentRowAsAString() throws Exception {
            resultSet.next();
            assertEquals("row1", getString("col1"));
            assertEquals("1", getString("col2"));
            resultSet.next();
            assertEquals("row2", getString("col1"));
            assertEquals("2", getString("col2"));
        }

        @Test
        void returnsNullWhenValueIsNull() throws Exception {
            resultSet.relative(3);
            assertNull(getString("col1"));
        }

        @Nested
        class WhenOutOfPosition extends SharedWhenOutOfPosition<String> {
            protected String get(int n) throws Exception {
                return getString(n);
            }

            protected String get(String n) throws Exception {
                return getString(n);
            }
        }

        @Nested
        class WhenClosed extends SharedWhenClosed<String> {
            protected String get(int n) throws Exception {
                return getString(n);
            }

            protected String get(String n) throws Exception {
                return getString(n);
            }
        }
    }

    @Nested
    class GetNString extends GetString {
        @Override
        protected String getString(int n) throws Exception {
            return resultSet.getNString(n);
        }

        @Override
        protected String getString(String n) throws Exception {
            return resultSet.getNString(n);
        }
    }

    @Nested
    class GetBytes {
        private final Charset UTF_8 = Charset.forName("UTF-8");

        @BeforeEach
        void setUp() {
            queryResultsHelper.update(Arrays.asList(
                    createColumn("col1", "string"),
                    createColumn("col2", "integer")
            ), Arrays.asList(
                    createRow("row1", "1"),
                    createRow("row2", "2"),
                    createRow(null, null)
            ));
        }

        @Test
        void returnsTheColumnAtTheSpecifiedIndexOfTheCurrentRowAsAString() throws Exception {
            resultSet.next();
            assertArrayEquals("row1".getBytes(UTF_8), resultSet.getBytes(1));
            assertArrayEquals("1".getBytes(UTF_8), resultSet.getBytes(2));
            resultSet.next();
            assertArrayEquals("row2".getBytes(UTF_8), resultSet.getBytes(1));
            assertArrayEquals("2".getBytes(UTF_8), resultSet.getBytes(2));
        }

        @Test
        void returnsTheColumnWithTheSpecifiedNameOfTheCurrentRowAsAString() throws Exception {
            resultSet.next();
            assertArrayEquals("row1".getBytes(UTF_8), resultSet.getBytes("col1"));
            assertArrayEquals("1".getBytes(UTF_8), resultSet.getBytes("col2"));
            resultSet.next();
            assertArrayEquals("row2".getBytes(UTF_8), resultSet.getBytes("col1"));
            assertArrayEquals("2".getBytes(UTF_8), resultSet.getBytes("col2"));
        }

        @Test
        void returnsNullWhenValueIsNull() throws Exception {
            resultSet.relative(3);
            assertNull(resultSet.getBytes("col1"));
        }

        @Nested
        class WhenOutOfPosition extends SharedWhenOutOfPosition<byte[]> {
            protected byte[] get(int n) throws Exception {
                return resultSet.getBytes(n);
            }

            protected byte[] get(String n) throws Exception {
                return resultSet.getBytes(n);
            }
        }

        @Nested
        class WhenClosed extends SharedWhenClosed<byte[]> {
            protected byte[] get(int n) throws Exception {
                return resultSet.getBytes(n);
            }

            protected byte[] get(String n) throws Exception {
                return resultSet.getBytes(n);
            }
        }
    }

    @Nested
    class GetBoolean {
        @BeforeEach
        void setUp() {
            queryResultsHelper.update(Collections.singletonList(
                    createColumn("col1", "boolean")
            ), Arrays.asList(
                    createRow("0"),
                    createRow("1"),
                    createRow("3"),
                    createRow("-1"),
                    createRow("false"),
                    createRow("true"),
                    createRow("FALSE"),
                    createRow("TRUE"),
                    createRow("FaLsE"),
                    createRow("TruE"),
                    createRowWithNull()
            ));
        }

        @Test
        void returns0AsFalse() throws Exception {
            resultSet.next();
            assertFalse(resultSet.getBoolean(1));
            assertFalse(resultSet.getBoolean("col1"));
        }

        @Test
        void returns1AsTrue() throws Exception {
            resultSet.next();
            resultSet.next();
            assertTrue(resultSet.getBoolean(1));
            assertTrue(resultSet.getBoolean("col1"));
        }

        @Test
        void returnsAllOtherNumbersAsTrue() throws Exception {
            resultSet.relative(3);
            assertTrue(resultSet.getBoolean(1));
            assertTrue(resultSet.getBoolean("col1"));
            resultSet.next();
            assertTrue(resultSet.getBoolean(1));
            assertTrue(resultSet.getBoolean("col1"));
        }

        @Test
        void returnsFalseAsFalse() throws Exception {
            for (int i = 0; i < 5; i++) {
                resultSet.next();
            }
            assertFalse(resultSet.getBoolean(1));
            assertFalse(resultSet.getBoolean("col1"));
            resultSet.next();
            resultSet.next();
            assertFalse(resultSet.getBoolean(1));
            assertFalse(resultSet.getBoolean("col1"));
            resultSet.next();
            resultSet.next();
            assertFalse(resultSet.getBoolean(1));
            assertFalse(resultSet.getBoolean("col1"));
        }

        @Test
        void returnsTrueAsTrue() throws Exception {
            for (int i = 0; i < 6; i++) {
                resultSet.next();
            }
            assertTrue(resultSet.getBoolean(1));
            assertTrue(resultSet.getBoolean("col1"));
            resultSet.next();
            resultSet.next();
            assertTrue(resultSet.getBoolean(1));
            assertTrue(resultSet.getBoolean("col1"));
            resultSet.next();
            resultSet.next();
            assertTrue(resultSet.getBoolean(1));
            assertTrue(resultSet.getBoolean("col1"));
        }

        @Test
        void returnsNullAsFalse() throws Exception {
            for (int i = 0; i < 11; i++) {
                resultSet.next();
            }
            assertFalse(resultSet.getBoolean(1));
            assertFalse(resultSet.getBoolean("col1"));
        }

        @Nested
        class WhenOutOfPosition extends SharedWhenOutOfPosition<Boolean> {
            protected Boolean get(int n) throws Exception {
                return resultSet.getBoolean(n);
            }

            protected Boolean get(String n) throws Exception {
                return resultSet.getBoolean(n);
            }
        }

        @Nested
        class WhenClosed extends SharedWhenClosed<Boolean> {
            protected Boolean get(int n) throws Exception {
                return resultSet.getBoolean(n);
            }

            protected Boolean get(String n) throws Exception {
                return resultSet.getBoolean(n);
            }
        }
    }

    abstract class SharedGetNumber<T> {
        protected abstract T zero();

        protected abstract T negativeValue();

        protected abstract T positiveValue();

        protected abstract T get(int n) throws Exception;

        protected abstract T get(String n) throws Exception;

        protected boolean supportsReallyLargeNumbers() {
            return false;
        }

        protected boolean supportsNativeNull() {
            return false;
        }

        @BeforeEach
        void setUp() {
            queryResultsHelper.update(Collections.singletonList(
                    createColumn("col1", "tinyint")
            ), Arrays.asList(
                    createRow(zero().toString()),
                    createRow(negativeValue().toString()),
                    createRow(positiveValue().toString()),
                    createRow("234234544234234523423423434534523412324234234234234"),
                    createRow("-13123102830192830801283085023749123749273498"),
                    createRow("fnord"),
                    createRow(""),
                    createRowWithNull()
            ));
        }

        @Test
        void returnsTheIntegerValueOfTheSpecifiedColumn() throws Exception {
            resultSet.next();
            assertEquals(zero(), get(1));
            resultSet.next();
            assertEquals(negativeValue(), get("col1"));
            resultSet.next();
            assertEquals(positiveValue(), get(1));
        }

        @Test
        void throwsAnErrorWhenNotSupported() throws Exception {
            if (!supportsReallyLargeNumbers()) {
                resultSet.relative(4);
                assertThrows(SQLDataException.class, () -> get(1));
                assertThrows(SQLDataException.class, () -> get("col1"));
                resultSet.next();
                assertThrows(SQLDataException.class, () -> get(1));
                assertThrows(SQLDataException.class, () -> get("col1"));
            }
        }

        @Test
        void throwsAnErrorWhenNotANumber() throws Exception {
            resultSet.relative(6);
            assertThrows(SQLDataException.class, () -> get(1));
            assertThrows(SQLDataException.class, () -> get("col1"));
            resultSet.next();
            assertThrows(SQLDataException.class, () -> get(1));
            assertThrows(SQLDataException.class, () -> get("col1"));
        }

        @Test
        void returnsNullAsZeroOrNull() throws Exception {
            if (supportsNativeNull()) {
                resultSet.relative(8);
                assertNull(get(1));
                assertNull(get("col1"));
            } else {
                resultSet.relative(8);
                assertEquals(zero(), get(1));
                assertEquals(zero(), get("col1"));
            }
        }

        @Nested
        class WhenOutOfPosition extends SharedWhenOutOfPosition<T> {
            protected T get(int n) throws Exception {
                return SharedGetNumber.this.get(n);
            }

            protected T get(String n) throws Exception {
                return SharedGetNumber.this.get(n);
            }
        }

        @Nested
        class WhenClosed extends SharedWhenClosed<T> {
            protected T get(int n) throws Exception {
                return SharedGetNumber.this.get(n);
            }

            protected T get(String n) throws Exception {
                return SharedGetNumber.this.get(n);
            }
        }
    }

    @Nested
    class GetByte extends SharedGetNumber<Byte> {
        @Override
        protected Byte zero() {
            return (byte) 0;
        }

        @Override
        protected Byte negativeValue() {
            return (byte) -1;
        }

        @Override
        protected Byte positiveValue() {
            return (byte) 123;
        }

        @Override
        protected Byte get(int n) throws Exception {
            return resultSet.getByte(n);
        }

        @Override
        protected Byte get(String n) throws Exception {
            return resultSet.getByte(n);
        }
    }

    @Nested
    class GetShort extends SharedGetNumber<Short> {
        @Override
        protected Short zero() {
            return (short) 0;
        }

        @Override
        protected Short negativeValue() {
            return (short) -1234;
        }

        @Override
        protected Short positiveValue() {
            return (short) 31001;
        }

        @Override
        protected Short get(int n) throws Exception {
            return resultSet.getShort(n);
        }

        @Override
        protected Short get(String n) throws Exception {
            return resultSet.getShort(n);
        }
    }

    @Nested
    class GetInt extends SharedGetNumber<Integer> {
        @Override
        protected Integer zero() {
            return 0;
        }

        @Override
        protected Integer negativeValue() {
            return -123123;
        }

        @Override
        protected Integer positiveValue() {
            return 24456343;
        }

        @Override
        protected Integer get(int n) throws Exception {
            return resultSet.getInt(n);
        }

        @Override
        protected Integer get(String n) throws Exception {
            return resultSet.getInt(n);
        }
    }

    @Nested
    class GetLong extends SharedGetNumber<Long> {
        @Override
        protected Long zero() {
            return 0L;
        }

        @Override
        protected Long negativeValue() {
            return -12312312312L;
        }

        @Override
        protected Long positiveValue() {
            return 234L;
        }

        @Override
        protected Long get(int n) throws Exception {
            return resultSet.getLong(n);
        }

        @Override
        protected Long get(String n) throws Exception {
            return resultSet.getLong(n);
        }
    }

    @Nested
    class GetFloat extends SharedGetNumber<Float> {
        @Override
        protected Float zero() {
            return 0f;
        }

        @Override
        protected Float negativeValue() {
            return -1.234f;
        }

        @Override
        protected Float positiveValue() {
            return 13413.234231f;
        }

        @Override
        protected Float get(int n) throws Exception {
            return resultSet.getFloat(n);
        }

        @Override
        protected Float get(String n) throws Exception {
            return resultSet.getFloat(n);
        }
    }

    @Nested
    class GetDouble extends SharedGetNumber<Double> {
        @Override
        protected Double zero() {
            return 0d;
        }

        @Override
        protected Double negativeValue() {
            return -1.234d;
        }

        @Override
        protected Double positiveValue() {
            return 13413.234231d;
        }

        @Override
        protected boolean supportsReallyLargeNumbers() {
            return true;
        }

        @Override
        protected Double get(int n) throws Exception {
            return resultSet.getDouble(n);
        }

        @Override
        protected Double get(String n) throws Exception {
            return resultSet.getDouble(n);
        }
    }

    @Nested
    class GetBigDecimal extends SharedGetNumber<BigDecimal> {
        @Override
        protected BigDecimal zero() {
            return new BigDecimal("0");
        }

        @Override
        protected BigDecimal negativeValue() {
            return new BigDecimal("-1");
        }

        @Override
        protected BigDecimal positiveValue() {
            return new BigDecimal("34234123");
        }

        @Override
        protected boolean supportsReallyLargeNumbers() {
            return true;
        }

        @Override
        protected boolean supportsNativeNull() {
            return true;
        }

        @Override
        protected BigDecimal get(int n) throws Exception {
            return resultSet.getBigDecimal(n);
        }

        @Override
        protected BigDecimal get(String n) throws Exception {
            return resultSet.getBigDecimal(n);
        }
    }

    @Nested
    class GetDate {
        @BeforeEach
        void setUp() {
            queryResultsHelper.update(Collections.singletonList(
                    createColumn("col1", "date")
            ), Arrays.asList(
                    createRow("2019-04-20"),
                    createRow("not a date"),
                    createRow("0"),
                    createRowWithNull()
            ));
        }

        @Test
        void returnsDates() throws Exception {
            resultSet.next();
            assertEquals(Date.valueOf(LocalDate.of(2019, 4, 20)), resultSet.getDate(1));
            assertEquals(Date.valueOf(LocalDate.of(2019, 4, 20)), resultSet.getDate("col1"));
        }

        @Test
        void throwsWhenValueIsNotADate() throws Exception {
            resultSet.next();
            resultSet.next();
            assertThrows(SQLException.class, () -> resultSet.getDate(1));
            assertThrows(SQLException.class, () -> resultSet.getDate("col1"));
            resultSet.next();
            assertThrows(SQLException.class, () -> resultSet.getDate(1));
            assertThrows(SQLException.class, () -> resultSet.getDate("col1"));
        }

        @Test
        void returnsNullForNull() throws Exception {
            resultSet.relative(4);
            assertNull(resultSet.getDate(1));
            assertNull(resultSet.getDate("col1"));
        }

        @Nested
        class WhenGivenACalendar {
            @Test
            void isNotSupported() throws Exception {
                Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC-12"));
                resultSet.next();
                assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getDate(1, calendar));
                assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getDate("col1", calendar));
            }
        }

        @Nested
        class WhenOutOfPosition extends SharedWhenOutOfPosition<Date> {
            protected Date get(int n) throws Exception {
                return resultSet.getDate(n);
            }

            protected Date get(String n) throws Exception {
                return resultSet.getDate(n);
            }
        }

        @Nested
        class WhenClosed extends SharedWhenClosed<Date> {
            protected Date get(int n) throws Exception {
                return resultSet.getDate(n);
            }

            protected Date get(String n) throws Exception {
                return resultSet.getDate(n);
            }
        }
    }

    @Nested
    class GetTime {
        @BeforeEach
        void setUp() {
            queryResultsHelper.update(Collections.singletonList(
                    createColumn("col1", "time")
            ), Arrays.asList(
                    createRow("09:36:16.363"),
                    createRow("not a time"),
                    createRow("0"),
                    createRowWithNull()
            ));
        }

        @Test
        void returnsTimes() throws Exception {
            resultSet.next();
            assertEquals(Time.valueOf("09:36:16"), resultSet.getTime(1));
            assertEquals(Time.valueOf("09:36:16"), resultSet.getTime("col1"));
        }

        @Test
        void throwsWhenValueIsNotATime() throws Exception {
            resultSet.next();
            resultSet.next();
            assertThrows(SQLException.class, () -> resultSet.getTime(1));
            assertThrows(SQLException.class, () -> resultSet.getTime("col1"));
            resultSet.next();
            assertThrows(SQLException.class, () -> resultSet.getTime(1));
            assertThrows(SQLException.class, () -> resultSet.getTime("col1"));
        }

        @Test
        void returnsNullForNull() throws Exception {
            resultSet.relative(4);
            assertNull(resultSet.getTime(1));
            assertNull(resultSet.getTime("col1"));
        }

        @Nested
        class WhenGivenACalendar {
            @Test
            void isNotSupported() throws Exception {
                Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC-12"));
                resultSet.next();
                assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getTime(1, calendar));
                assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getTime("col1", calendar));
            }
        }

        @Nested
        class WhenOutOfPosition extends SharedWhenOutOfPosition<Time> {
            protected Time get(int n) throws Exception {
                return resultSet.getTime(n);
            }

            protected Time get(String n) throws Exception {
                return resultSet.getTime(n);
            }
        }

        @Nested
        class WhenClosed extends SharedWhenClosed<Time> {
            protected Time get(int n) throws Exception {
                return resultSet.getTime(n);
            }

            protected Time get(String n) throws Exception {
                return resultSet.getTime(n);
            }
        }
    }

    @Nested
    class GetTimestamp {
        @BeforeEach
        void setUp() {
            queryResultsHelper.update(Collections.singletonList(
                    createColumn("col1", "timestamp")
            ), Arrays.asList(
                    createRow("2019-04-23 09:35:23.291"),
                    createRow("not a time"),
                    createRow("0"),
                    createRowWithNull()
            ));
        }

        @Test
        void returnsTimestamps() throws Exception {
            resultSet.next();
            assertEquals(Timestamp.valueOf("2019-04-23 09:35:23.291"), resultSet.getTimestamp(1));
            assertEquals(Timestamp.valueOf("2019-04-23 09:35:23.291"), resultSet.getTimestamp("col1"));
        }

        @Test
        void throwsWhenValueIsNotATimestamp() throws Exception {
            resultSet.next();
            resultSet.next();
            assertThrows(SQLException.class, () -> resultSet.getTimestamp(1));
            assertThrows(SQLException.class, () -> resultSet.getTimestamp("col1"));
            resultSet.next();
            assertThrows(SQLException.class, () -> resultSet.getTimestamp(1));
            assertThrows(SQLException.class, () -> resultSet.getTimestamp("col1"));
        }

        @Test
        void returnsNullForNull() throws Exception {
            resultSet.relative(4);
            assertNull(resultSet.getTimestamp(1));
            assertNull(resultSet.getTimestamp("col1"));
        }

        @Nested
        class WhenGivenACalendar {
            @Test
            void isNotSupported() throws Exception {
                Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC-12"));
                resultSet.next();
                assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getTimestamp(1, calendar));
                assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getTimestamp("col1", calendar));
            }
        }

        @Nested
        class WhenOutOfPosition extends SharedWhenOutOfPosition<Timestamp> {
            protected Timestamp get(int n) throws Exception {
                return resultSet.getTimestamp(n);
            }

            protected Timestamp get(String n) throws Exception {
                return resultSet.getTimestamp(n);
            }
        }

        @Nested
        class WhenClosed extends SharedWhenClosed<Timestamp> {
            protected Timestamp get(int n) throws Exception {
                return resultSet.getTimestamp(n);
            }

            protected Timestamp get(String n) throws Exception {
                return resultSet.getTimestamp(n);
            }
        }
    }

    @Nested
    class GetArray {
        @BeforeEach
        void setUp() {
            queryResultsHelper.update(Collections.singletonList(
                    createColumn("col1", "array")
            ), Arrays.asList(
                    createRow("[1, 2, 3]"),
                    createRow("[a, b, c]"),
                    createRow("not an array"),
                    createRow("0"),
                    createRowWithNull()
            ));
        }

        @Test
        void returnsArrays() throws Exception {
            resultSet.next();
            assertNotNull(resultSet.getArray(1));
            assertNotNull(resultSet.getArray("col1"));
        }

        @Test
        void arraysAreReportedAsVARCHAR() throws Exception {
            resultSet.next();
            assertEquals(Types.VARCHAR, resultSet.getArray(1).getBaseType());
            assertEquals(Types.VARCHAR, resultSet.getArray("col1").getBaseType());
        }

        @Test
        void arraysAreStringArrays() throws Exception {
            resultSet.next();
            assertArrayEquals(new String[]{"1", "2", "3"}, (String[]) resultSet.getArray(1).getArray());
            assertArrayEquals(new String[]{"1", "2", "3"}, (String[]) resultSet.getArray("col1").getArray());
            resultSet.next();
            assertArrayEquals(new String[]{"a", "b", "c"}, (String[]) resultSet.getArray(1).getArray());
            assertArrayEquals(new String[]{"a", "b", "c"}, (String[]) resultSet.getArray("col1").getArray());
        }

        @Test
        void throwsWhenValueIsNotAnArray() throws Exception {
            resultSet.next();
            resultSet.next();
            resultSet.next();
            assertThrows(SQLException.class, () -> resultSet.getArray(1));
            assertThrows(SQLException.class, () -> resultSet.getArray("col1"));
            resultSet.next();
            assertThrows(SQLException.class, () -> resultSet.getArray(1));
            assertThrows(SQLException.class, () -> resultSet.getArray("col1"));
        }

        @Test
        void returnsNullForNull() throws Exception {
            resultSet.relative(5);
            assertNull(resultSet.getArray(1));
            assertNull(resultSet.getArray("col1"));
        }

        @Test
        void allowsTheArrayToBeSubdivided() throws Exception {
            resultSet.next();
            assertArrayEquals(new String[]{"2", "3"}, (String[]) resultSet.getArray(1).getArray(1, 2));
            assertArrayEquals(new String[]{"2", "3"}, (String[]) resultSet.getArray("col1").getArray(1, 2));
        }

        @Nested
        class WhenTheArrayIsAskedForAResultSet {
            @Test
            void isNotSupported() throws Exception {
                resultSet.next();
                assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getArray(1).getResultSet());
                assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getArray("col1").getResultSet());
                assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getArray(1).getResultSet(1L, 2));
                assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getArray("col1").getResultSet(1L, 2));
                assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getArray(1).getResultSet(Collections.emptyMap()));
                assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getArray("col1").getResultSet(Collections.emptyMap()));
                assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getArray(1).getResultSet(1L, 2, Collections.emptyMap()));
                assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getArray("col1").getResultSet(1L, 2, Collections.emptyMap()));
            }
        }

        @Nested
        class WhenTheArrayIsAskedToConvertTheElements {
            @Test
            void isNotSupported() throws Exception {
                resultSet.next();
                assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getArray(1).getArray(Collections.emptyMap()));
                assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getArray("col1").getArray(Collections.emptyMap()));
                assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getArray(1).getArray(1L, 2, Collections.emptyMap()));
                assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getArray("col1").getArray(1L, 2, Collections.emptyMap()));
            }
        }

        @Nested
        class WhenOutOfPosition extends SharedWhenOutOfPosition<Array> {
            protected Array get(int n) throws Exception {
                return resultSet.getArray(n);
            }

            protected Array get(String n) throws Exception {
                return resultSet.getArray(n);
            }
        }

        @Nested
        class WhenClosed extends SharedWhenClosed<Array> {
            protected Array get(int n) throws Exception {
                return resultSet.getArray(n);
            }

            protected Array get(String n) throws Exception {
                return resultSet.getArray(n);
            }
        }
    }

    @Nested
    class WasNull {
        @BeforeEach
        void setUp() {
            queryResultsHelper.update(Arrays.asList(
                    createColumn("col1", "boolean"),
                    createColumn("col2", "integer")
            ), Arrays.asList(
                    createRow("true", null),
                    createRow(null, "1")
            ));
        }

        @Test
        void returnsFalseBeforeAnyValueIsRead() throws Exception {
            assertFalse(resultSet.wasNull());
        }

        @Test
        void returnsTrueWhenTheLastReadValueWasNull() throws Exception {
            resultSet.next();
            resultSet.getInt(2);
            assertTrue(resultSet.wasNull());
            resultSet.next();
            resultSet.getBoolean(1);
            assertTrue(resultSet.wasNull());
        }

        @Test
        void returnsFalseWhenTheLastReadValueWasNotNull() throws Exception {
            resultSet.next();
            resultSet.getBoolean(1);
            assertFalse(resultSet.wasNull());
            resultSet.next();
            resultSet.getInt(2);
            assertFalse(resultSet.wasNull());
        }

        @Test
        void resetsWhenANonNullValueIsRead() throws Exception {
            resultSet.next();
            resultSet.getInt(2);
            resultSet.getBoolean(1);
            assertFalse(resultSet.wasNull());
        }
    }

    @Nested
    class MutationQueries {
        @Test
        void returnFalse() throws Exception {
            assertFalse(resultSet.rowUpdated());
            assertFalse(resultSet.rowInserted());
            assertFalse(resultSet.rowDeleted());
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnError() throws Exception {
                resultSet.close();
                assertThrows(SQLException.class, () -> resultSet.rowUpdated());
                assertThrows(SQLException.class, () -> resultSet.rowInserted());
                assertThrows(SQLException.class, () -> resultSet.rowDeleted());
            }
        }
    }

    @Nested
    class Mutations {
        @Test
        void isNotSupported() {
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.insertRow());
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateRow());
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.deleteRow());
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.refreshRow());
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.cancelRowUpdates());
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.moveToInsertRow());
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.moveToCurrentRow());
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNull(1));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNull(""));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBoolean(1, true));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBoolean("", true));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateByte(1, (byte) 0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateByte("", (byte) 0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateShort(1, (short) 0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateShort("", (short) 0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateInt(1, 0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateInt("", 0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateLong(1, 0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateLong("", 0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateFloat(1, 0.0f));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateFloat("", 0.0f));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateDouble(1, 0.0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateDouble("", 0.0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBigDecimal(1, new BigDecimal(0)));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBigDecimal("", new BigDecimal(0)));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateString(1, ""));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateString("", ""));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBytes(1, new byte[0]));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBytes("", new byte[0]));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateDate(1, new Date(0)));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateDate("", new Date(0)));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateTime(1, new Time(0)));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateTime("", new Time(0)));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateTimestamp(1, new Timestamp(0)));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateTimestamp("", new Timestamp(0)));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateAsciiStream(1, null, 0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateAsciiStream("", null, 0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBinaryStream(1, null, 0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBinaryStream("", null, 0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateCharacterStream(1, null, 0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateCharacterStream("", null, 0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateObject(1, null, 0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateObject("", null, 0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateObject(1, null));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateObject("", null));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateRef(1, null));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateRef("", null));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBlob(1, new ByteArrayInputStream(new byte[0])));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBlob(1, new ByteArrayInputStream(new byte[0]), 0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBlob(1, (Blob) null));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBlob("", new ByteArrayInputStream(new byte[0])));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBlob("", new ByteArrayInputStream(new byte[0]), 0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBlob("", (Blob) null));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateClob(1, (Reader) null));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateClob(1, null, 0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateClob(1, (Clob) null));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateClob("", (Reader) null));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateClob("", null, 0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateClob("", (Clob) null));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateArray(1, null));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateArray("", null));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateRowId(1, null));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateRowId("", null));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNString(1, ""));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNString("", ""));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNClob(1, (Reader) null));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNClob(1, null, 0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNClob(1, (NClob) null));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNClob("", (Reader) null));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNClob("", null, 0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNClob("", (NClob) null));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateSQLXML(1, null));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateSQLXML("", null));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNCharacterStream(1, null));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNCharacterStream(1, null, 0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNCharacterStream("", null));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNCharacterStream("", null, 0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateAsciiStream(1, null));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateAsciiStream(1, null, 0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateAsciiStream(1, null, 0L));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateAsciiStream("", null));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateAsciiStream("", null, 0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateAsciiStream("", null, 0L));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBinaryStream(1, null));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBinaryStream(1, null, 0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBinaryStream(1, null, 0L));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBinaryStream("", null));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBinaryStream("", null, 0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBinaryStream("", null, 0L));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateCharacterStream(1, null));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateCharacterStream(1, null, 0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateCharacterStream(1, null, 0L));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateCharacterStream("", null));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateCharacterStream("", null, 0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateCharacterStream("", null, 0L));
        }
    }
}
