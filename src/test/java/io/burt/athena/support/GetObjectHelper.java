package io.burt.athena.support;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class GetObjectHelper implements S3AsyncClient {
    private final Map<String, byte[]> objects;
    private final List<GetObjectRequest> getObjectRequests;

    public GetObjectHelper() {
        this.objects = new HashMap<>();
        this.getObjectRequests = new LinkedList<>();
    }

    public void setObject(String bucket, String key, byte[] contents) {
        objects.put(String.format("s3://%s/%s", bucket, key), contents);
    }

    public List<GetObjectRequest> getObjectRequests() {
        return getObjectRequests;
    }

    private static class GetObjectPublisher implements SdkPublisher<ByteBuffer>, Subscription {
        private final byte[] objectContents;
        private final ExecutorService executor;

        private int offset;
        private Subscriber<? super ByteBuffer> subscriber;
        private AtomicBoolean complete;

        public GetObjectPublisher(byte[] contents) {
            this.objectContents = contents;
            this.offset = 0;
            this.complete = new AtomicBoolean(false);
            this.executor = Executors.newSingleThreadExecutor();
        }

        @Override
        public void subscribe(Subscriber<? super ByteBuffer> s) {
            subscriber = s;
            executor.submit(() -> subscriber.onSubscribe(this));
        }

        @Override
        public void request(long n) {
            int actualLength;
            if (n == Long.MAX_VALUE) {
                actualLength = objectContents.length - offset;
            } else {
                actualLength = ((offset + n * 10) > objectContents.length) ? objectContents.length - offset : (int) (n * 10);
            }
            if (actualLength > 0) {
                final int o = offset;
                final int l = actualLength;
                offset += actualLength;
                ByteBuffer slice = ByteBuffer.wrap(objectContents, o, l);
                executor.submit(() -> subscriber.onNext(slice));
            }
            if (!complete.get() && offset >= objectContents.length) {
                complete.set(true);
                executor.submit(() -> subscriber.onComplete());
            }
        }

        @Override
        public void cancel() {
        }
    }

    @Override
    public <T> CompletableFuture<T> getObject(Consumer<GetObjectRequest.Builder> getObjectRequestConsumer, AsyncResponseTransformer<GetObjectResponse, T> requestTransformer) throws AwsServiceException, SdkClientException {
        GetObjectRequest.Builder requestBuilder = GetObjectRequest.builder();
        getObjectRequestConsumer.accept(requestBuilder);
        GetObjectRequest request = requestBuilder.build();
        getObjectRequests.add(request);
        String uri = String.format("s3://%s/%s", request.bucket(), request.key());
        if (objects.containsKey(uri)) {
            byte[] object = objects.get(uri);
            GetObjectResponse response = GetObjectResponse.builder().contentLength((long) object.length).build();
            CompletableFuture<T> future = requestTransformer.prepare();
            requestTransformer.onResponse(response);
            requestTransformer.onStream(new GetObjectPublisher(object));
            return future;
        } else {
            throw NoSuchKeyException.builder().build();
        }
    }

    @Override
    public String serviceName() {
        return null;
    }

    @Override
    public void close() {
    }
}
