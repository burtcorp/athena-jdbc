package io.burt.athena.result;

import io.burt.athena.support.GetObjectHelper;
import io.burt.athena.support.GetQueryResultsHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.athena.model.GetQueryResultsRequest;
import software.amazon.awssdk.services.athena.model.QueryExecution;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static io.burt.athena.support.GetQueryResultsHelper.createColumn;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class S3ResultTest {
    private GetQueryResultsHelper queryResultsHelper;
    private GetObjectHelper getObjectHelper;
    private S3Result result;

    @BeforeEach
    void setUp() {
        QueryExecution queryExecution = QueryExecution
                .builder()
                .queryExecutionId("Q1234")
                .resultConfiguration(b -> b.outputLocation("s3://some-bucket/the/prefix/Q1234.csv"))
                .build();
        queryResultsHelper = new GetQueryResultsHelper();
        getObjectHelper = new GetObjectHelper();
        result = new S3Result(queryResultsHelper, getObjectHelper, queryExecution, Duration.ofMillis(10));
    }

    private void createData() {
        queryResultsHelper.update(Arrays.asList(
                createColumn("col1", "string"),
                createColumn("col2", "integer")
        ), Collections.emptyList());
        StringBuilder contents = new StringBuilder();
        contents.append("\"col1\",\"col2\"\n");
        contents.append("\"row1\",\"1\"\n");
        contents.append("\"row2\",\"2\"\n");
        contents.append("\"row3\",\"3\"\n");
        getObjectHelper.setObject("some-bucket", "the/prefix/Q1234.csv", contents.toString());

    }

    @Nested
    class FetchSize {
        // TODO
    }

    @Nested
    class SetFetchSize {
        // TODO
    }

    @Nested
    class GetMetaData {
        @BeforeEach
        void setUp() {
            createData();
        }

        @Test
        void returnsMetaData() throws Exception {
            assertEquals("col1", result.getMetaData().getColumnLabel(1));
        }

        @Test
        void loadsTheMetaDataIfNotLoaded() throws Exception {
            assertNotNull(result.getMetaData());
            assertTrue(queryResultsHelper.requestCount() > 0);
        }

        @Test
        void doesNotLoadTheMetaDataAgain() throws Exception {
            result.getMetaData();
            result.next();
            result.getMetaData();
            result.next();
            assertEquals(1, queryResultsHelper.requestCount());
        }

        @Test
        void doesNotLoadRowsWhenLoadingMetaData() throws Exception {
            result.getMetaData();
            GetQueryResultsRequest request = queryResultsHelper.resultsRequests().get(0);
            assertEquals(1, request.maxResults());
        }

        @Nested
        class WhenLoadingIsInterrupted {
            private Thread runner;
            private AtomicReference<ResultSetMetaData> executeResult;
            private AtomicReference<Boolean> interruptedState;

            @BeforeEach
            void setUp() {
                executeResult = new AtomicReference<>(null);
                interruptedState = new AtomicReference<>(null);
                runner = new Thread(() -> {
                    try {
                        executeResult.set(result.getMetaData());
                        interruptedState.set(Thread.currentThread().isInterrupted());
                    } catch (SQLException sqle) {
                        throw new RuntimeException(sqle);
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
            void returnsNull() throws Exception {
                runner.start();
                runner.join();
                assertNull(executeResult.get());
            }
        }
    }

    @Nested
    class Next {
        @BeforeEach
        void setUp() {
            createData();
        }

        @Test
        void loadsMetaData() throws Exception {
            result.next();
            assertEquals(1, queryResultsHelper.requestCount());
        }

        @Test
        void onlyLoadsMetaDataOnTheFirstCall() throws Exception {
            result.next();
            result.next();
            assertEquals(1, queryResultsHelper.requestCount());
        }

        @Test
        void requestsTheResultObject() throws Exception {
            result.next();
            List<GetObjectRequest> requests = getObjectHelper.getObjectRequests();
            assertEquals(1, requests.size());
            GetObjectRequest request = requests.get(0);
            assertEquals("some-bucket", request.bucket());
            assertEquals("the/prefix/Q1234.csv", request.key());
        }

        @Test
        void parsesTheResultObject() throws Exception {
            result.next();
            assertEquals("row1", result.getString(1));
            assertEquals("1", result.getString(2));
            result.next();
            assertEquals("row2", result.getString(1));
            assertEquals("2", result.getString(2));
            result.next();
            assertEquals("row3", result.getString(1));
            assertEquals("3", result.getString(2));
        }
    }

    @Nested
    class GetRowNumber {
        @BeforeEach
        void setUp() {
            createData();
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
    class Position {
        @BeforeEach
        void setUp() {
            createData();
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

    @Nested
    class Close {
        @Test
        void abortsTheResponseStreamWhenNotAtEnd() {
            // TODO
        }
    }
}