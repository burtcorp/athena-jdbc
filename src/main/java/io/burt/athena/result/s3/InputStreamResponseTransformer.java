package io.burt.athena.result.s3;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class InputStreamResponseTransformer extends InputStream implements AsyncResponseTransformer<GetObjectResponse, InputStream>, Subscriber<ByteBuffer> {
    private static final ByteBuffer END_MARKER = ByteBuffer.allocate(0);

    private final CompletableFuture<InputStream> future;
    private final BlockingQueue<ByteBuffer> chunks;

    private GetObjectResponse response;
    private Subscription subscription;
    private ByteBuffer readChunk;
    private Throwable error;
    private AtomicBoolean complete;

    public InputStreamResponseTransformer() {
        this.future = new CompletableFuture<>();
        this.chunks = new LinkedBlockingQueue<>();
        this.complete = new AtomicBoolean(false);
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
        error = t;
        future.completeExceptionally(t);
        try {
            close();
        } catch (Exception e) { }
    }

    @Override
    public void onSubscribe(Subscription s) {
        subscription = s;
        subscription.request(30L);
    }

    @Override
    public void onNext(ByteBuffer byteBuffer) {
        if(byteBuffer.hasRemaining()) {
            chunks.offer(byteBuffer);
        } else {
            subscription.request(1L);
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
                    subscription.request(1L);
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
        if (!complete.get()) {
            chunks.clear();
            chunks.offer(END_MARKER);
            subscription.cancel();
            future.cancel(true);
        }
        super.close();
    }
}
