package io.burt.athena.result;

import io.burt.athena.AthenaResultSetMetaData;
import io.burt.athena.result.csv.VeryBasicCsvParser;
import io.burt.athena.result.protobuf.VeryBasicProtobufParser;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.athena.model.ColumnInfo;
import software.amazon.awssdk.services.athena.model.ColumnNullable;
import software.amazon.awssdk.services.athena.model.QueryExecution;
import software.amazon.awssdk.services.athena.model.ResultSetMetadata;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class S3Result implements Result {
    private static final Pattern S3_URI_PATTERN = Pattern.compile("^s3://([^/]+)/(.+)$");

    private final QueryExecution queryExecution;
    private final S3AsyncClient s3Client;
    private final String bucketName;
    private final String key;

    private AthenaResultSetMetaData resultSetMetaData;
    private InputStream responseStream;
    private Iterator<String[]> csvParser;
    private String[] currentRow;
    private int rowNumber;

    public S3Result(S3AsyncClient s3Client, QueryExecution queryExecution, Duration timeout) {
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

    private void start() throws ExecutionException, InterruptedException {
        CompletableFuture<AthenaResultSetMetaData> metadataFuture = s3Client.getObject(b -> b.bucket(bucketName).key(key + ".metadata"), new ByteBufferResponseTransformer()).thenApply(this::parseResultSetMetadata);
        CompletableFuture<InputStream> responseStreamFuture = s3Client.getObject(b -> b.bucket(bucketName).key(key), new InputStreamResponseTransformer());
        CompletableFuture<Iterator<String[]>> combinedFuture = metadataFuture.thenCombine(responseStreamFuture, (metaData, responseStream) -> new VeryBasicCsvParser(new BufferedReader(new InputStreamReader(responseStream)), metaData.getColumnCount()));
        csvParser = combinedFuture.get();
        csvParser.next();
        rowNumber = 0;
        resultSetMetaData = metadataFuture.get();
    }

    @Override
    public AthenaResultSetMetaData getMetaData() throws SQLException {
        if (resultSetMetaData == null) {
            try {
                start();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            } catch (ExecutionException e) {
                throw new SQLException(e.getCause());
            }
        }
        return resultSetMetaData;
    }

    private AthenaResultSetMetaData parseResultSetMetadata(ByteBuffer buffer) {
        VeryBasicProtobufParser parser = new VeryBasicProtobufParser();
        List<VeryBasicProtobufParser.Field> fields = parser.parse(buffer);
        List<ColumnInfo> columns = new LinkedList<>();
        for (VeryBasicProtobufParser.Field field : fields) {
            if (field.getNumber() == 4) {
                byte[] contents = ((VeryBasicProtobufParser.BinaryField) field).getContents();
                List<VeryBasicProtobufParser.Field> parse = parser.parse(contents);
                ColumnInfo columnInfo = fieldsToColumn(parse);
                columns.add(columnInfo);
            }
        }
        return new AthenaResultSetMetaData(queryExecution, ResultSetMetadata.builder().columnInfo(columns).build());
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
        if (resultSetMetaData == null) {
            try {
                start();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } catch (ExecutionException e) {
                throw new SQLException(e.getCause());
            }
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

    private static class InputStreamResponseTransformer extends InputStream implements AsyncResponseTransformer<GetObjectResponse, InputStream>, Subscriber<ByteBuffer> {
        private static final ByteBuffer END_MARKER = ByteBuffer.allocate(0);

        private final CompletableFuture<InputStream> future;
        private final BlockingQueue<ByteBuffer> chunks;

        private Subscription subscription;
        private ByteBuffer readChunk;
        private Throwable error;
        private AtomicBoolean complete;
        private AtomicInteger approximateBufferSize;

        public InputStreamResponseTransformer() {
            this.future = new CompletableFuture<>();
            this.chunks = new LinkedBlockingQueue<>();
            this.complete = new AtomicBoolean(false);
            this.approximateBufferSize = new AtomicInteger(0);
        }

        @Override
        public CompletableFuture<InputStream> prepare() {
            return future;
        }

        @Override
        public void onResponse(GetObjectResponse response) {
            future.complete(this);
        }

        @Override
        public void onStream(SdkPublisher<ByteBuffer> publisher) {
            publisher.subscribe(this);
        }

        @Override
        public void exceptionOccurred(Throwable t) {
            error = t;
            future.completeExceptionally(t);
        }

        @Override
        public void onSubscribe(Subscription s) {
            subscription = s;
            subscription.request(1000L);
        }

        @Override
        public void onNext(ByteBuffer byteBuffer) {
            chunks.offer(byteBuffer);
            int size = approximateBufferSize.addAndGet(byteBuffer.remaining());
            maybeRequestMore(size);
        }

        private void maybeRequestMore(int currentSize) {
            if (currentSize < (1 << 25)) {
                subscription.request(1000L);
            }
        }

        @Override
        public void onError(Throwable t) {
            exceptionOccurred(t);
        }

        @Override
        public void onComplete() {
            chunks.offer(END_MARKER);
            complete.set(true);
        }

        @Override
        public int available() throws IOException {
            if (error != null) {
                throw new IOException(error);
            }
            if (readChunk != null) {
                return readChunk.remaining();
            } else {
                return 0;
            }
        }

        private boolean ensureChunk() throws IOException {
            if (error != null) {
                throw new IOException(error);
            }
            if (readChunk == END_MARKER) {
                return false;
            } else if (readChunk == null || !readChunk.hasRemaining()) {
                try {
                    readChunk = chunks.take();
                    if (readChunk == END_MARKER) {
                        return false;
                    } else {
                        int size = approximateBufferSize.addAndGet(-readChunk.remaining());
                        maybeRequestMore(size);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return true;
        }

        @Override
        public int read(byte[] destination, int offset, int length) throws IOException {
            if (ensureChunk()) {
                int actualLength = Math.min(length, readChunk.remaining());
                readChunk.get(destination, offset, actualLength);
                return actualLength;
            } else {
                return -1;
            }
        }

        @Override
        public int read() throws IOException {
            if (ensureChunk()) {
                return Byte.toUnsignedInt(readChunk.get());
            } else {
                return -1;
            }
        }

        @Override
        public void close() throws IOException {
            if (!complete.get()) {
                chunks.clear();
                chunks.offer(END_MARKER);
                subscription.cancel();
                future.cancel(true);
            }
            super.close();
        }
    }

    private static class ByteBufferResponseTransformer implements AsyncResponseTransformer<GetObjectResponse, ByteBuffer>, Subscriber<ByteBuffer> {
        private final CompletableFuture<ByteBuffer> future;

        private Subscription subscription;
        private ByteBuffer resultBuffer;

        public ByteBufferResponseTransformer() {
            this.future = new CompletableFuture<>();
        }

        @Override
        public CompletableFuture<ByteBuffer> prepare() {
            return future;
        }

        @Override
        public void onResponse(GetObjectResponse response) {
            resultBuffer = ByteBuffer.allocate(Math.toIntExact(response.contentLength()));
        }

        @Override
        public void onStream(SdkPublisher<ByteBuffer> publisher) {
            publisher.subscribe(this);
        }

        @Override
        public void exceptionOccurred(Throwable t) {
            future.completeExceptionally(t);
        }

        @Override
        public void onSubscribe(Subscription s) {
            subscription = s;
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(ByteBuffer byteBuffer) {
            resultBuffer.put(byteBuffer);
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onError(Throwable t) {
            exceptionOccurred(t);
        }

        @Override
        public void onComplete() {
            resultBuffer.flip();
            future.complete(resultBuffer);
        }
    }
}
