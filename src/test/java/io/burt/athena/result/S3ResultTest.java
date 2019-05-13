package io.burt.athena.result;

import io.burt.athena.support.GetObjectHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.athena.model.ColumnInfo;
import software.amazon.awssdk.services.athena.model.QueryExecution;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static io.burt.athena.support.GetQueryResultsHelper.createColumn;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(MockitoExtension.class)
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
            innerBuffer.flip();
            outerBuffer.put((byte) (4 << 3 | 2));
            outerBuffer.put((byte) innerBuffer.remaining());
            outerBuffer.put(innerBuffer);
        }
        return outerBuffer;
    }

    private void createData() {
        ByteBuffer metadata = createMetadata(Arrays.asList(
                createColumn("col1", "string"),
                createColumn("col2", "integer")
        ));
        getObjectHelper.setObject("some-bucket", "the/prefix/Q1234.csv.metadata", new String(metadata.array(), StandardCharsets.ISO_8859_1));
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
        class WhenTheOutputLocationIsMalformed {
            // TODO
        }

        @Nested
        class WhenReadingTheMetaDataThrowsIoException {
            // TODO
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
        class WhenReadingTheDataThrowsIoException {
            // TODO
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
        @Test
        void abortsTheResponseStreamWhenNotAtEnd() {
            // TODO
        }
    }
}
