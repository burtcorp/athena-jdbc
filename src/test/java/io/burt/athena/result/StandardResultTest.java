package io.burt.athena.result;

import io.burt.athena.support.GetQueryResultsHelper;
import io.burt.athena.support.TestNameGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.athena.AthenaAsyncClient;
import software.amazon.awssdk.services.athena.model.ColumnInfo;
import software.amazon.awssdk.services.athena.model.GetQueryResultsRequest;
import software.amazon.awssdk.services.athena.model.QueryExecution;
import software.amazon.awssdk.services.athena.model.Row;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static io.burt.athena.support.GetQueryResultsHelper.createColumn;
import static io.burt.athena.support.GetQueryResultsHelper.createRow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(TestNameGenerator.class)
class StandardResultTest {
    private GetQueryResultsHelper queryResultsHelper;
    private StandardResult result;

    protected StandardResult createResult(AthenaAsyncClient athenaClient) {
        QueryExecution queryExecution = QueryExecution.builder().queryExecutionId("Q1234").build();
        return new StandardResult(queryResultsHelper, queryExecution, 123, Duration.ofSeconds(10));
    }

    @BeforeEach
    void setUp() {
        queryResultsHelper = new GetQueryResultsHelper();
        result = createResult(queryResultsHelper);
    }

    @Nested
    class FetchSize {
        @Test
        void returnsTheCurrentFetchSize() throws Exception {
            assertEquals(123, result.getFetchSize());
            result.setFetchSize(234);
            assertEquals(234, result.getFetchSize());
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
            result.setFetchSize(77);
            result.next();
            assertEquals(77, queryResultsHelper.pageSizes().get(0));
        }

        @Test
        void setsTheFetchSizeOfAFutureRequest() throws Exception {
            result.next();
            result.setFetchSize(99);
            while (result.next()) {
            }
            List<Integer> pageSizes = queryResultsHelper.pageSizes();
            assertNotEquals(99, pageSizes.get(0));
            assertEquals(99, pageSizes.get(pageSizes.size() - 1));
        }

        @Nested
        class WhenCalledWithTooLargeNumber {
            @Test
            void throwsAnError() {
                assertThrows(SQLException.class, () -> result.setFetchSize(1001));
            }
        }
    }

    @Nested
    class GetMetaData {
        @BeforeEach
        void setUp() {
            queryResultsHelper.update(Arrays.asList(
                    createColumn("col1", "string"),
                    createColumn("col2", "integer")
            ), Arrays.asList(
                    createRow("row1", "1"),
                    createRow("row2", "2"),
                    createRow("row3", "3")
            ));
        }

        @Test
        void returnsMetaData() throws Exception {
            result.next();
            assertEquals("col1", result.getMetaData().getColumnLabel(1));
        }

        @Test
        void loadsTheMetaDataIfNotLoaded() throws Exception {
            assertNotNull(result.getMetaData());
            assertTrue(queryResultsHelper.requestCount() > 0);
        }

        @Test
        void doesNotLoadTheSamePageTwice() throws Exception {
            result.getMetaData();
            result.next();
            List<String> nextTokens = queryResultsHelper.nextTokens();
            assertEquals(new HashSet<>(nextTokens).size(), nextTokens.size());
        }

        @Nested
        class WhenLoadingIsInterrupted {
            private Thread runner;
            private AtomicReference<Optional<ResultSetMetaData>> executeResult;
            private AtomicReference<Throwable> executeThrowable;
            private AtomicReference<Boolean> interruptedState;

            @BeforeEach
            void setUp() {
                executeResult = new AtomicReference<>(null);
                executeThrowable = new AtomicReference<>(null);
                interruptedState = new AtomicReference<>(null);
                runner = new Thread(() -> {
                    try {
                        executeResult.set(Optional.ofNullable(result.getMetaData()));
                    } catch (Throwable t) {
                        executeThrowable.set(t);
                    } finally {
                        interruptedState.set(Thread.currentThread().isInterrupted());
                    }
                });
                queryResultsHelper.interruptLoading(true);
            }

            @Test
            void setsTheInterruptFlag() throws Exception {
                runner.start();
                runner.join();
                assertTrue(interruptedState.get());
            }

            @Test
            void throwsAnSQLException() throws Exception {
                runner.start();
                runner.join();
                assertNull(executeResult.get());
                assertTrue(SQLException.class.isAssignableFrom(executeThrowable.get().getClass()));
            }
        }
    }

    @Nested
    class Next {
        @BeforeEach
        void setUp() {
            queryResultsHelper.update(Arrays.asList(
                    createColumn("col1", "string"),
                    createColumn("col2", "integer")
            ), Collections.emptyList());

        }

        @Test
        void callsGetQueryResults() throws Exception {
            result.next();
            GetQueryResultsRequest request = queryResultsHelper.resultsRequests().get(0);
            assertEquals("Q1234", request.queryExecutionId());
        }

        @Test
        void loadsTheConfiguredNumberOfRows() throws Exception {
            result.next();
            GetQueryResultsRequest request = queryResultsHelper.resultsRequests().get(0);
            assertEquals(123, request.maxResults());
        }

        @Nested
        class WhenTheResultIsEmpty {
            @Test
            void returnsFalse() throws Exception {
                assertFalse(result.next());
            }
        }

        @Nested
        class WhenTheResultHasRows {
            @BeforeEach
            void setUp() {
                queryResultsHelper.update(Arrays.asList(
                        createColumn("col1", "string"),
                        createColumn("col2", "integer")
                ), Arrays.asList(
                        createRow("row1", "1"),
                        createRow("row2", "2"),
                        createRow("row3", "3")
                ));
            }

            @Test
            void returnsTrue() throws Exception {
                assertTrue(result.next());
            }

            @Test
            void skipsTheHeaderRow() throws Exception {
                result.next();
                assertNotEquals("col1", result.getString(1));
            }

            @Test
            void returnsEachRow() throws Exception {
                result.next();
                assertEquals("row1", result.getString(1));
                result.next();
                assertEquals("row2", result.getString(1));
                result.next();
                assertEquals("row3", result.getString(1));
            }

            @Nested
            class AndIsExhausted {
                @Test
                void returnsFalse() throws Exception {
                    assertTrue(result.next());
                    assertTrue(result.next());
                    assertTrue(result.next());
                    assertFalse(result.next());
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
                result.setFetchSize(3);
                List<String> rows = new ArrayList<>(9);
                while (result.next()) {
                    rows.add(result.getString(2));
                }
                assertEquals(4, queryResultsHelper.requestCount());
                assertEquals(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9"), rows);
                assertEquals(Arrays.asList(null, "2", "3", "4"), queryResultsHelper.nextTokens());
            }

            @Test
            void stopsLoadingWhenThereAreNoMorePages() throws Exception {
                result.setFetchSize(3);
                for (int i = 0; i < 9; i++) {
                    result.next();
                }
                assertFalse(result.next());
            }
        }

        @Nested
        class WhenLoadingIsInterrupted {
            private Thread runner;
            private AtomicReference<Boolean> executeResult;
            private AtomicReference<Throwable> executeThrowable;
            private AtomicReference<Boolean> interruptedState;

            @BeforeEach
            void setUp() {
                executeResult = new AtomicReference<>(null);
                executeThrowable = new AtomicReference<>(null);
                interruptedState = new AtomicReference<>(null);
                runner = new Thread(() -> {
                    try {
                        executeResult.set(result.next());
                    } catch (Throwable t) {
                        executeThrowable.set(t);
                    } finally {
                        interruptedState.set(Thread.currentThread().isInterrupted());
                    }
                });
                queryResultsHelper.interruptLoading(true);
            }

            @Test
            void setsTheInterruptFlag() throws Exception {
                runner.start();
                runner.join();
                assertTrue(interruptedState.get());
            }

            @Test
            void throwsAnSQLException() throws Exception {
                runner.start();
                runner.join();
                assertNull(executeResult.get());
                assertTrue(SQLException.class.isAssignableFrom(executeThrowable.get().getClass()));
            }
        }

        @Nested
        class WhenLoadingTimesOut {
            @Test
            void throwsSQLTimeoutException() {
                QueryExecution queryExecution = QueryExecution.builder().queryExecutionId("Q1234").build();
                queryResultsHelper.delayResponses(Duration.ofSeconds(10));
                result = new StandardResult(queryResultsHelper, queryExecution, 123, Duration.ZERO);
                Exception e = assertThrows(Exception.class, () -> result.next());
                assertEquals(SQLTimeoutException.class, e.getClass());
                assertEquals(TimeoutException.class, e.getCause().getClass());
            }
        }

        @Nested
        class WhenLoadingThrowsAnError {
            @Test
            void wrapsTheErrorInSQLException() {
                queryResultsHelper.queueException(new ArithmeticException("b0rk"));
                Exception e = assertThrows(Exception.class, () -> result.next());
                assertEquals(SQLException.class, e.getClass());
                assertEquals(ArithmeticException.class, e.getCause().getClass());
            }
        }
    }

    @Nested
    class GetRowNumber {
        @BeforeEach
        void setUp() {
            queryResultsHelper.update(Arrays.asList(
                    createColumn("col1", "string"),
                    createColumn("col2", "integer")
            ), Arrays.asList(
                    createRow("row1", "1"),
                    createRow("row2", "2"),
                    createRow("row3", "3")
            ));
        }

        @Test
        void returnsZeroWhenBeforeFirstRow() throws Exception {
            assertEquals(0, result.getRowNumber());
        }

        @Test
        void returnsOneWhenOnFirstRow() throws Exception {
            result.next();
            assertEquals(1, result.getRowNumber());
        }

        @Test
        void returnsTheRowNumber() throws Exception {
            result.next();
            assertEquals(1, result.getRowNumber());
            result.next();
            assertEquals(2, result.getRowNumber());
            result.next();
            assertEquals(3, result.getRowNumber());
        }
    }

    @Nested
    class GetString {
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
            result.next();
            assertEquals("row1", result.getString(1));
            assertEquals("1", result.getString(2));
            result.next();
            assertEquals("row2", result.getString(1));
            assertEquals("2", result.getString(2));
        }

        @Test
        void returnsNullWhenValueIsNull() throws Exception {
            for (int i = 0; i < 3; i++) {
                result.next();
            }
            assertNull(result.getString(1));
        }
    }

    @Nested
    class Position {
        @BeforeEach
        void setUp() {
            queryResultsHelper.update(Arrays.asList(
                    createColumn("col1", "string"),
                    createColumn("col2", "integer")
            ), Arrays.asList(
                    createRow("row1", "1"),
                    createRow("row2", "2"),
                    createRow("row3", "3")
            ));
        }

        @Test
        void returnsBeforeFirstBeforeNextIsCalled() throws Exception {
            assertEquals(ResultPosition.BEFORE_FIRST, result.getPosition());
        }

        @Test
        void returnsFirstWhenOnFirstRow() throws Exception {
            result.next();
            assertEquals(ResultPosition.FIRST, result.getPosition());
        }

        @Test
        void returnsMiddleAfterFirst() throws Exception {
            result.next();
            result.next();
            assertEquals(ResultPosition.MIDDLE, result.getPosition());
        }

        @Test
        void returnsLastWhenOnLastRow() throws Exception {
            for (int i = 0; i < 3; i++) {
                result.next();
            }
            assertEquals(ResultPosition.LAST, result.getPosition());
        }

        @Test
        void returnsAfterLastWhenAfterLastRow() throws Exception {
            for (int i = 0; i < 4; i++) {
                result.next();
            }
            assertEquals(ResultPosition.AFTER_LAST, result.getPosition());
            result.next();
            assertEquals(ResultPosition.AFTER_LAST, result.getPosition());
        }
    }
}
