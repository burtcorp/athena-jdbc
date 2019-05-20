package io.burt.athena.result.s3;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public class ByteBufferResponseTransformer implements AsyncResponseTransformer<GetObjectResponse, ByteBuffer>, Subscriber<ByteBuffer> {
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
        ((Buffer) resultBuffer).flip();
        future.complete(resultBuffer);
    }
}
