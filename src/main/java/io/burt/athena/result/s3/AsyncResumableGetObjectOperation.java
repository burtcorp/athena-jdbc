package io.burt.athena.result.s3;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.core.internal.util.NoopSubscription;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;


public class AsyncResumableGetObjectOperation<ReturnT> implements Callable<CompletableFuture<ReturnT>> {
    private final S3AsyncClient s3AsyncClient;
    private final GetObjectRequest.Builder requestBuilder;
    private final AsyncResponseTransformer<GetObjectResponse, ReturnT> targetTransformer;
    private int retryLimit;

    public AsyncResumableGetObjectOperation(S3AsyncClient s3AsyncClient, Consumer<GetObjectRequest.Builder> request, AsyncResponseTransformer<GetObjectResponse, ReturnT> targetTransformer, int retryLimit) {
        this(s3AsyncClient, GetObjectRequest.builder().applyMutation(request), targetTransformer, retryLimit);
    }

    public AsyncResumableGetObjectOperation(S3AsyncClient s3AsyncClient, GetObjectRequest request, AsyncResponseTransformer<GetObjectResponse, ReturnT> targetTransformer, int retryLimit) {
        this(s3AsyncClient, request.toBuilder(), targetTransformer, retryLimit);
    }

    public AsyncResumableGetObjectOperation(S3AsyncClient s3AsyncClient, GetObjectRequest.Builder requestBuilder, AsyncResponseTransformer<GetObjectResponse, ReturnT> targetTransformer, int retryLimit) {
        this.s3AsyncClient = s3AsyncClient;
        this.requestBuilder = requestBuilder;
        this.targetTransformer = targetTransformer;
        this.retryLimit = retryLimit;
    }

    public CompletableFuture<ReturnT> call() {
        return s3AsyncClient.getObject(
            requestBuilder.build(),
            new WrapperAsyncResponseTransformer()
        );
    }

    private class WrapperAsyncResponseTransformer implements AsyncResponseTransformer<GetObjectResponse, ReturnT>, SdkPublisher<ByteBuffer>, Subscription, Subscriber<ByteBuffer> {
        private final AtomicReference<SdkPublisher<ByteBuffer>> sourcePublisher = new AtomicReference<>();
        private final AtomicReference<Subscription> sourceSubscription = new AtomicReference<>();
        private final AtomicBoolean isBeforeFirstBody = new AtomicBoolean(true);
        private final AtomicBoolean isCancelled = new AtomicBoolean();
        private final AtomicReference<Subscriber<? super ByteBuffer>> targetSubscriber = new AtomicReference<>();
        private final Set<Throwable> delayedExceptions = new CopyOnWriteArraySet<>();
        private final AtomicBoolean hasSubscribed = new AtomicBoolean();
        private final AtomicBoolean hasOnSubscribedTarget = new AtomicBoolean();
        private final LongAdder bytesReceived = new LongAdder();
        private long messagesRequested = 0L;

        @Override
        public CompletableFuture<ReturnT> prepare() {
            if (isBeforeFirstBody.get()) {
                return targetTransformer.prepare();
            } else {
                return new CompletableFuture<>();
            }
        }

        @Override
        public void onResponse(GetObjectResponse response) {
            if (isBeforeFirstBody.get()) {
                requestBuilder.ifMatch(response.eTag());
                targetTransformer.onResponse(response);
            }
        }

        @Override
        public void onStream(SdkPublisher<ByteBuffer> sdkPublisher) {
            sourcePublisher.set(sdkPublisher);
            if (isBeforeFirstBody.getAndSet(false)) {
                targetTransformer.onStream(this);
            } else {
                sdkPublisher.subscribe(this);
            }
        }

        @Override
        public void exceptionOccurred(Throwable throwable) {
            delayedExceptions.add(throwable);
            if (sourcePublisher.get() == null) {
                failTarget();
            }
        }

        private void failTarget() {
            Throwable resolved = resolveDelayedException();
            targetTransformer.exceptionOccurred(resolved);
            Subscriber<? super ByteBuffer> subscriber = targetSubscriber.getAndSet(null);
            if (subscriber != null) {
                subscriber.onError(resolved);
            }
        }

        private Throwable resolveDelayedException() {
            Iterator<Throwable> iterator = delayedExceptions.iterator();
            Throwable result = iterator.next();
            while (iterator.hasNext()) {
                try {
                    Throwable throwable = iterator.next();
                    Throwable cause = throwable.getCause();
                    if (cause == null) {
                        try {
                            throwable.initCause(result);
                            result = throwable;
                        } catch (IllegalStateException e) {
                            result.addSuppressed(throwable);
                            if (result.getSuppressed().length == 0) {
                                result = new RuntimeException(result);
                                result.addSuppressed(throwable);
                            }
                        }
                    }
                } catch (Exception e) {
                    result.addSuppressed(e);
                }
            }
            return result;
        }

        @Override
        public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
            if (hasSubscribed.compareAndSet(false, true)) {
                targetSubscriber.set(subscriber);
                sourcePublisher.get().subscribe(this);
            } else {
                subscriber.onSubscribe(new NoopSubscription(subscriber));
                subscriber.onError(new IllegalStateException("only single subscriber allowed"));
            }
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            if (isCancelled.get()) {
                subscription.cancel();
                return;
            }
            synchronized (this) {
                sourceSubscription.set(subscription);
                if (messagesRequested > 0) {
                    subscription.request(messagesRequested);
                }
            }
            if (hasOnSubscribedTarget.compareAndSet(false, true)) {
                targetSubscriber.get().onSubscribe(this);
            }
        }

        @Override
        public void onNext(ByteBuffer byteBuffer) {
            long size = byteBuffer.remaining();
            bytesReceived.add(size);
            synchronized (this) {
                messagesRequested--;
            }
            targetSubscriber.get().onNext(byteBuffer);
        }

        @Override
        public void onError(Throwable throwable) {
            sourcePublisher.set(null);
            sourceSubscription.set(null);
            delayedExceptions.add(throwable);
            if (!isCancelled.get() && retryLimit-- > 0) {
                requestBuilder.range("bytes=" + bytesReceived + "-");
                s3AsyncClient
                    .getObject(requestBuilder.build(), this)
                    .whenComplete((r, e) -> {
                        if (e != null) {
                            delayedExceptions.add(e);
                            failTarget();
                        }
                    });
            } else {
                failTarget();
            }
        }

        @Override
        public void onComplete() {
            sourcePublisher.set(null);
            sourceSubscription.set(null);
            targetSubscriber.getAndSet(null).onComplete();
        }

        @Override
        public void request(long amount) {
            if (amount <= 0) {
                throw new IllegalArgumentException("non-positive request amount");
            }
            synchronized (this) {
                messagesRequested += amount;
                Subscription s = sourceSubscription.get();
                if (s != null) {
                    s.request(amount);
                }
            }
        }

        @Override
        public void cancel() {
            isCancelled.set(true);
            Subscription s = sourceSubscription.get();
            if (s != null) {
                s.cancel();
            }
        }
    }
}
