package io.burt.athena.result;

import io.burt.athena.support.GetObjectHelper;
import io.burt.athena.support.TestNameGenerator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.athena.model.ColumnInfo;
import software.amazon.awssdk.services.athena.model.QueryExecution;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.text.ParseException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import static io.burt.athena.support.GetQueryResultsHelper.createColumn;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayNameGeneration(TestNameGenerator.class)
class S3ResultTest {
    private GetObjectHelper getObjectHelper;
    private S3Result result;

    @BeforeEach
    void setUp() {
        QueryExecution queryExecution = QueryExecution
                .builder()
                .queryExecutionId("Q1234")
                .resultConfiguration(b -> b.outputLocation("s3://some-bucket/the/prefix/Q1234.csv"))
                .build();
        getObjectHelper = new GetObjectHelper();
        result = new S3Result(getObjectHelper, queryExecution, Duration.ofMillis(10));
    }

    @AfterEach
    void tearDown() {
        getObjectHelper.close();
    }

    private ByteBuffer createMetadata(List<ColumnInfo> columns) {
        ByteBuffer outerBuffer = ByteBuffer.allocate(1 << 16);
        outerBuffer.put((byte) (1 << 3 | 2));
        outerBuffer.put((byte) 5);
        outerBuffer.put("fnord".getBytes());
        for (ColumnInfo column : columns) {
            ByteBuffer innerBuffer = ByteBuffer.allocate(1 << 12);
            if (column.name() != null) {
                innerBuffer.put((byte) (4 << 3 | 2));
                innerBuffer.put((byte) column.name().length());
                innerBuffer.put(column.name().getBytes(StandardCharsets.UTF_8));
            }
            if (column.label() != null) {
                innerBuffer.put((byte) (5 << 3 | 2));
                innerBuffer.put((byte) column.label().length());
                innerBuffer.put(column.label().getBytes(StandardCharsets.UTF_8));
            }
            if (column.type() != null) {
                innerBuffer.put((byte) (6 << 3 | 2));
                innerBuffer.put((byte) column.type().length());
                innerBuffer.put(column.type().getBytes(StandardCharsets.UTF_8));
            }
            ((Buffer) innerBuffer).flip();
            outerBuffer.put((byte) (4 << 3 | 2));
            outerBuffer.put((byte) innerBuffer.remaining());
            outerBuffer.put(innerBuffer);
        }
        ((Buffer) outerBuffer).flip();
        return outerBuffer;
    }

    private void createData() {
        ByteBuffer metadata = createMetadata(Arrays.asList(
                createColumn("col1", "string"),
                createColumn("col2", "integer")
        ));
        byte[] bytes = new byte[metadata.remaining()];
        metadata.get(bytes);
        getObjectHelper.setObject("some-bucket", "the/prefix/Q1234.csv.metadata", bytes);
        StringBuilder contents = new StringBuilder();
        contents.append("\"col1\",\"col2\"\n");
        contents.append("\"row1\",\"1\"\n");
        contents.append("\"row2\",\"2\"\n");
        contents.append("\"row3\",\"3\"\n");
        getObjectHelper.setObject("some-bucket", "the/prefix/Q1234.csv", contents.toString().getBytes(StandardCharsets.UTF_8));

    }

    @Nested
    class Constructor {
        @Nested
        class WhenTheOutputLocationIsMalformed {
            @Test
            void throwsAnException() {
                QueryExecution queryExecution = QueryExecution
                        .builder()
                        .queryExecutionId("Q1234")
                        .resultConfiguration(b -> b.outputLocation("://some-bucket/the/prefix/Q1234.csv"))
                        .build();
                Exception e = assertThrows(IllegalArgumentException.class, () -> new S3Result(getObjectHelper, queryExecution, Duration.ofMillis(10)));
                assertTrue(e.getMessage().contains("\"://some-bucket/the/prefix/Q1234.csv\""));
                assertTrue(e.getMessage().contains("malformed"));
            }
        }
    }


    @Nested
    class GetFetchSize {
        @Test
        void alwaysReturnsMinusOne() {
            assertEquals(-1, result.getFetchSize());
        }
    }

    @Nested
    class SetFetchSize {
        @Test
        void ignoresTheNewSize() {
            result.setFetchSize(1000000);
            assertEquals(-1, result.getFetchSize());
        }
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
            GetObjectRequest request = getObjectHelper.getObjectRequests().stream().filter(r -> r.key().endsWith(".metadata")).findFirst().get();
            assertEquals("some-bucket", request.bucket());
            assertEquals("the/prefix/Q1234.csv.metadata", request.key());
        }

        @Test
        void doesNotLoadTheMetaDataAgain() throws Exception {
            result.getMetaData();
            result.next();
            result.getMetaData();
            result.next();
            long count = getObjectHelper.getObjectRequests().stream().filter(r -> r.key().endsWith(".metadata")).count();
            assertEquals(1, count);
        }

        @Nested
        class WhenTheMetaDataObjectIsNotFound {
            @Test
            void throwsAnException() {
                getObjectHelper.removeObject("some-bucket", "the/prefix/Q1234.csv.metadata");
                Exception e = assertThrows(SQLException.class, () -> result.getMetaData());
                assertEquals(NoSuchKeyException.class, e.getCause().getClass());
            }
        }

        @Nested
        class WhenLoadingTheMetaDataThrowsAnException {
            @Test
            void wrapsItInSqlException() {
                getObjectHelper.setObjectException("some-bucket", "the/prefix/Q1234.csv.metadata", new ArrayIndexOutOfBoundsException("b0rk"));
                Exception e = assertThrows(SQLException.class, () -> result.getMetaData());
                assertEquals("b0rk", e.getCause().getMessage());
                assertEquals(ArrayIndexOutOfBoundsException.class, e.getCause().getClass());
            }
        }

        @Nested
        class WhenLoadingTheMetaDataThrowsAnExceptionDuringLoading {
            @Test
            void wrapsItInSqlException() {
                getObjectHelper.setObjectLateException("some-bucket", "the/prefix/Q1234.csv.metadata", new ArrayIndexOutOfBoundsException("b0rk"));
                Exception e = assertThrows(SQLException.class, () -> result.getMetaData());
                assertEquals("b0rk", e.getCause().getMessage());
                assertEquals(ArrayIndexOutOfBoundsException.class, e.getCause().getClass());
            }
        }

        @Nested
        class WhenLoadingTheMetaDataTimesOut {
            @Test
            void throwsSqlTimeoutException() {
                getObjectHelper.delayObject("some-bucket", "the/prefix/Q1234.csv.metadata", Duration.ofSeconds(1));
                Exception e = assertThrows(SQLTimeoutException.class, () -> result.getMetaData());
                assertEquals(TimeoutException.class, e.getCause().getClass());
            }
        }

        @Nested
        class WhenInterruptedWhileLoadingTheMetaData {
            @Test
            void returnsNull() {
                // PENDING: very hard to set up
            }

            @Test
            void marksTheThreadAsInterrupted() {
                // PENDING: very hard to set up
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
            long count = getObjectHelper.getObjectRequests().stream().filter(r -> r.key().endsWith(".metadata")).count();
            assertEquals(1, count);
        }

        @Test
        void onlyLoadsMetaDataOnTheFirstCall() throws Exception {
            result.next();
            result.next();
            long count = getObjectHelper.getObjectRequests().stream().filter(r -> r.key().endsWith(".metadata")).count();
            assertEquals(1, count);
        }

        @Test
        void requestsTheResultObject() throws Exception {
            result.next();
            GetObjectRequest request = getObjectHelper.getObjectRequests().stream().filter(r -> r.key().endsWith(".csv")).findFirst().get();
            assertEquals("some-bucket", request.bucket());
            assertEquals("the/prefix/Q1234.csv", request.key());
        }

        @Test
        void requestsTheResultObjectOnce() throws Exception {
            result.next();
            long count = getObjectHelper.getObjectRequests().stream().filter(r -> r.key().endsWith(".csv")).count();
            assertEquals(1, count);
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

        @Nested
        class WhenTheResultObjectIsNotFound {
            @Test
            void throwsAnException() {
                getObjectHelper.removeObject("some-bucket", "the/prefix/Q1234.csv");
                Exception e = assertThrows(SQLException.class, () -> result.next());
                assertEquals(NoSuchKeyException.class, e.getCause().getClass());
            }
        }

        @Nested
        class WhenLoadingTheResultThrowsAnException {
            @Test
            void wrapsItInSqlException() {
                getObjectHelper.setObjectException("some-bucket", "the/prefix/Q1234.csv", new ArrayIndexOutOfBoundsException("b0rk"));
                Exception e = assertThrows(SQLException.class, () -> result.next());
                assertEquals("b0rk", e.getCause().getMessage());
                assertEquals(ArrayIndexOutOfBoundsException.class, e.getCause().getClass());
            }
        }

        @Nested
        class WhenLoadingTheResultThrowsAnExceptionDuringLoading {
            @Test
            void wrapsItInIoExceptionAndThenSqlException() {
                getObjectHelper.setObjectLateException("some-bucket", "the/prefix/Q1234.csv", new RuntimeException(new ParseException("b0rk", 13)));
                Exception e = assertThrows(SQLException.class, () -> result.next());
                assertEquals("b0rk", e.getCause().getCause().getCause().getMessage());
                assertEquals(IOException.class, e.getCause().getClass());
                assertEquals(ParseException.class, e.getCause().getCause().getCause().getClass());
            }
        }

        @Nested
        class WhenLoadingTheResultTimesOut {
            @Test
            void throwsSqlTimeoutException() {
                getObjectHelper.delayObject("some-bucket", "the/prefix/Q1234.csv", Duration.ofSeconds(1));
                Exception e = assertThrows(SQLTimeoutException.class, () -> result.next());
                assertEquals(TimeoutException.class, e.getCause().getClass());
            }
        }

        @Nested
        class WhenInterruptedWhileLoadingTheResult {
            void returnsFalse() {
                // PENDING: very hard to set up
            }

            void marksTheThreadAsInterrupted() {
                // PENDING: very hard to set up
            }
        }
    }

    @Nested
    class GetRowNumber {
        @BeforeEach
        void setUp() {
            createData();
        }

        @Test
        void returnsZeroWhenBeforeFirstRow() {
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
        void returnsBeforeFirstBeforeNextIsCalled() {
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
        @BeforeEach
        void setUp() {
            createData();
        }

        private class CancellablePublisher implements SdkPublisher<ByteBuffer>, AutoCloseable {
            CancellableSubscription subscription;

            @Override
            public void subscribe(Subscriber<? super ByteBuffer> s) {
                subscription = new CancellableSubscription(s);
                s.onSubscribe(subscription);
            }

            @Override
            public void close() throws Exception {
                subscription.close();
            }
        }

        private class CancellableSubscription implements Subscription, AutoCloseable {
            private final Subscriber<? super ByteBuffer> subscriber;
            private final ExecutorService executor;

            boolean cancelled = false;

            CancellableSubscription(Subscriber<? super ByteBuffer> subscriber) {
                this.subscriber = subscriber;
                this.executor = Executors.newSingleThreadExecutor();
            }

            @Override
            public void request(long n) {
                executor.submit(() -> subscriber.onNext(ByteBuffer.wrap("\"col1\",\"col2\"\n\"one\",\"1\"\n".getBytes(StandardCharsets.UTF_8))));
            }

            @Override
            public void cancel() {
                cancelled = true;
            }

            @Override
            public void close() {
                executor.shutdown();
            }
        }

        @Test
        void abortsTheDownloadByCancellingTheSubscription() throws Exception {
            try (CancellablePublisher publisher = new CancellablePublisher()) {
                getObjectHelper.setObjectPublisher("some-bucket", "the/prefix/Q1234.csv", publisher);
                result.next();
                result.close();
                assertTrue(publisher.subscription.cancelled);
            }
        }
    }
}
