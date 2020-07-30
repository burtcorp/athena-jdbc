package io.burt.athena.result.s3;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class InputStreamResponseTransformer extends InputStream implements AsyncResponseTransformer<GetObjectResponse, InputStream>, Subscriber<ByteBuffer> {
    private static final ByteBuffer END_MARKER = ByteBuffer.allocate(0);
    private static final int TARGET_BUFFER_SIZE = 1 << 25;
    private static final int CHUNKS_REQUEST_LIMIT = 1000;
    private static final float CHUNK_SIZE_EXPONENTIAL_WEIGHT = 0.2f;
    private static final float CHUNK_SIZE_INITIAL_ESTIMATE = 8192f;

    private final CompletableFuture<InputStream> future;
    protected final BlockingQueue<ByteBuffer> chunks;
    protected Duration timeout;

    private GetObjectResponse response;
    private AtomicReference<Subscription> subscription;
    protected ByteBuffer readChunk;
    protected Throwable error;
    private AtomicInteger approximateBufferSize;
    private AtomicInteger requests;
    private volatile float approximateChunkSize;

    public InputStreamResponseTransformer(Duration timeout) {
        this.future = new CompletableFuture<>();
        this.chunks = new LinkedBlockingQueue<>();
        this.subscription = new AtomicReference<>();
        this.approximateBufferSize = new AtomicInteger(0);
        this.requests = new AtomicInteger(0);
        this.approximateChunkSize = CHUNK_SIZE_INITIAL_ESTIMATE;
        this.timeout = timeout;
    }

    @Override
    public CompletableFuture<InputStream> prepare() {
        return future;
    }

    @Override
    public void onResponse(GetObjectResponse r) {
        response = r;
        future.complete(this);
    }

    @Override
    public void onStream(SdkPublisher<ByteBuffer> publisher) {
        publisher.subscribe(this);
    }

    @Override
    public void exceptionOccurred(Throwable t) {
        future.completeExceptionally(t);
        onError(t);
    }

    @Override
    public void onSubscribe(Subscription s) {
        subscription.set(s);
        if (response.contentLength() < TARGET_BUFFER_SIZE) {
            requests.set(Integer.MAX_VALUE);
            s.request(Long.MAX_VALUE);
        } else {
            requests.set(10);
            s.request(10L);
        }
    }

    @Override
    public void onNext(ByteBuffer byteBuffer) {
        int chunkSize = byteBuffer.remaining();
        if (chunkSize > 0) {
            chunks.offer(byteBuffer);
            approximateChunkSize += CHUNK_SIZE_EXPONENTIAL_WEIGHT * (chunkSize - approximateChunkSize);
        }
        requests.decrementAndGet();
        int size = approximateBufferSize.addAndGet(chunkSize);
        maybeRequestMore(size);
    }

    private void maybeRequestMore(int currentSize) {
        if (currentSize < TARGET_BUFFER_SIZE) {
            int newRequests = requests.get() + 10;
            if (newRequests < CHUNKS_REQUEST_LIMIT) {
                if (newRequests * approximateChunkSize + currentSize < TARGET_BUFFER_SIZE) {
                    requests.addAndGet(10);
                    Subscription s = subscription.get();
                    if (s != null) {
                        s.request(10L);
                    }
                }
            }
        }
    }

    @Override
    public void onError(Throwable t) {
        error = t;
        onComplete();
    }

    @Override
    public void onComplete() {
        chunks.offer(END_MARKER);
        subscription.set(null);
    }

    @Override
    public int available() throws IOException {
        if (error != null && readChunk == END_MARKER) {
            throw new IOException(error);
        }
        if (readChunk != null) {
            return readChunk.remaining();
        } else {
            return 0;
        }
    }

    protected boolean ensureChunk() throws IOException {
        if (readChunk == END_MARKER) {
            if (error != null) {
                throw new IOException(error);
            }
            return false;
        } else if (readChunk == null || !readChunk.hasRemaining()) {
            try {
                readChunk = chunks.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
                if (readChunk == END_MARKER) {
                    if (error != null) {
                        throw new IOException(error);
                    }
                    return false;
                } else if (readChunk == null) {
                    throw new IOException("read timeout");
                } else {
                    int size = approximateBufferSize.addAndGet(-readChunk.remaining());
                    maybeRequestMore(size);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
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
        chunks.clear();
        Subscription s = subscription.get();
        if (s != null) {
            s.cancel();
        }
        onComplete();
        readChunk = END_MARKER;
        if (error == null) {
            error = new IOException("closed");
        }
        super.close();
    }
}
