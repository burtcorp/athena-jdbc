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
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class GetObjectHelper implements S3AsyncClient, AutoCloseable {
    private final Map<String, byte[]> objects;
    private final Map<String, SdkPublisher<ByteBuffer>> publishers;
    private final Map<String, Exception> exceptions;
    private final Map<String, Exception> lateExceptions;
    private final Map<String, Duration> delays;
    private final List<GetObjectRequest> getObjectRequests;
    private final List<AutoCloseable> closeables;
    private final TestClock clock;

    public GetObjectHelper() {
        this.objects = new HashMap<>();
        this.publishers = new HashMap<>();
        this.exceptions = new HashMap<>();
        this.lateExceptions = new HashMap<>();
        this.delays = new HashMap<>();
        this.getObjectRequests = new LinkedList<>();
        this.closeables = new LinkedList<>();
        this.clock = new TestClock();
    }

    private String uri(String bucket, String key) {
        return String.format("s3://%s/%s", bucket, key);
    }

    public void setObject(String bucket, String key, byte[] contents) {
        objects.put(uri(bucket, key), contents);
    }

    public void setObjectPublisher(String bucket, String key, SdkPublisher<ByteBuffer> publisher) {
        publishers.put(uri(bucket, key), publisher);
    }

    public void setObjectException(String bucket, String key, Exception e) {
        exceptions.put(uri(bucket, key), e);
    }

    public void setObjectLateException(String bucket, String key, Exception e) {
        lateExceptions.put(uri(bucket, key), e);
    }

    public void removeObject(String bucket, String key) {
        objects.remove(uri(bucket, key));
    }

    public void delayObject(String bucket, String key, Duration duration) {
        delays.put(uri(bucket, key), duration);
    }

    public List<GetObjectRequest> getObjectRequests() {
        return getObjectRequests;
    }

    private static class GetObjectPublisher implements SdkPublisher<ByteBuffer>, Subscription, AutoCloseable {
        private final byte[] objectContents;
        private final ExecutorService executor;

        private int offset;
        private Subscriber<? super ByteBuffer> subscriber;
        private AtomicBoolean complete;

        GetObjectPublisher(byte[] contents) {
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

        @Override
        public void close() {
            executor.shutdown();
        }
    }

    private static class GetObjectExceptionPublisher implements SdkPublisher<ByteBuffer>, Subscription, AutoCloseable {
        private final Exception e;
        private final ExecutorService executor;

        private Subscriber<? super ByteBuffer> subscriber;

        GetObjectExceptionPublisher(Exception e) {
            this.e = e;
            this.executor = Executors.newSingleThreadExecutor();
        }

        @Override
        public void subscribe(Subscriber<? super ByteBuffer> s) {
            subscriber = s;
            executor.submit(() -> subscriber.onSubscribe(this));
            executor.submit(() -> subscriber.onError(e));
        }

        @Override
        public void request(long n) {
        }

        @Override
        public void cancel() {
        }

        @Override
        public void close() {
            executor.shutdown();
        }
    }

    @Override
    public <T> CompletableFuture<T> getObject(Consumer<GetObjectRequest.Builder> getObjectRequestConsumer, AsyncResponseTransformer<GetObjectResponse, T> requestTransformer) throws AwsServiceException, SdkClientException {
        GetObjectRequest.Builder requestBuilder = GetObjectRequest.builder();
        getObjectRequestConsumer.accept(requestBuilder);
        GetObjectRequest request = requestBuilder.build();
        getObjectRequests.add(request);
        String uri = String.format("s3://%s/%s", request.bucket(), request.key());
        CompletableFuture<T> future;
        if (exceptions.containsKey(uri)) {
            future = new CompletableFuture<>();
            future.completeExceptionally(exceptions.get(uri));
        } else if (lateExceptions.containsKey(uri)) {
            GetObjectResponse response = GetObjectResponse.builder().contentLength(0L).build();
            future = requestTransformer.prepare();
            requestTransformer.onResponse(response);
            GetObjectExceptionPublisher publisher = new GetObjectExceptionPublisher(lateExceptions.get(uri));
            requestTransformer.onStream(publisher);
            closeables.add(publisher);
        } else if (publishers.containsKey(uri)) {
            GetObjectResponse response = GetObjectResponse.builder().contentLength(0L).build();
            future = requestTransformer.prepare();
            requestTransformer.onResponse(response);
            requestTransformer.onStream(publishers.get(uri));
        } else if (objects.containsKey(uri)) {
            byte[] object = objects.get(uri);
            GetObjectResponse response = GetObjectResponse.builder().contentLength((long) object.length).build();
            future = requestTransformer.prepare();
            requestTransformer.onResponse(response);
            GetObjectPublisher publisher = new GetObjectPublisher(object);
            requestTransformer.onStream(publisher);
            closeables.add(publisher);
        } else {
            throw NoSuchKeyException.builder().build();
        }
        future = TestDelayedCompletableFuture.wrapWithDelay(future, delays.get(uri), clock);
        return future;
    }

    @Override
    public String serviceName() {
        return null;
    }

    @Override
    public void close() {
        for (AutoCloseable closeable : closeables) {
            try {
                closeable.close();
            } catch (Exception e) { }
        }
    }
}
