package io.burt.athena.result.s3;

import io.burt.athena.support.GetObjectHelper;
import io.burt.athena.support.TestNameGenerator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(TestNameGenerator.class)
public class AsyncResumableGetObjectOperationTest {
    private GetObjectHelper getObjectHelper;
    private Consumer<GetObjectRequest.Builder> requestBuilder;
    private AsyncResponseTransformer<GetObjectResponse, Long> targetTransformer;
    private CompletableFuture<Long> targetSubscriberCompletion;
    private Subscriber<? super ByteBuffer> targetSubscriber;

    private final List<ByteBuffer> resultBytes = new ArrayList<>();

    @BeforeEach
    void setUp() {
        getObjectHelper = new GetObjectHelper();
        getObjectHelper.setObject("example-bucket", "path/to/my-key", new byte[44]);
        requestBuilder = builder -> builder.bucket("example-bucket").key("path/to/my-key");
        targetTransformer = spy(new AsyncResponseTransformer<GetObjectResponse, Long>() {
            @Override
            public CompletableFuture<Long> prepare() {
                return CompletableFuture.completedFuture(1L);
            }

            @Override
            public void onResponse(GetObjectResponse getObjectResponse) {
            }

            @Override
            public void onStream(SdkPublisher<ByteBuffer> sdkPublisher) {
                sdkPublisher.subscribe(targetSubscriber);
            }

            @Override
            public void exceptionOccurred(Throwable throwable) {
            }
        });
        targetSubscriberCompletion = new CompletableFuture<>();
        targetSubscriber = spy(new Subscriber<ByteBuffer>() {
            private Subscription subscription;
            private long request = 1;

            @Override
            public void onSubscribe(Subscription subscription) {
                this.subscription = subscription;
                subscription.request(1);
            }

            @Override
            public void onNext(ByteBuffer byteBuffer) {
                resultBytes.add(byteBuffer);
                subscription.request(request *= 2);
            }

            @Override
            public void onError(Throwable throwable) {
                targetSubscriberCompletion.completeExceptionally(throwable);
            }

            @Override
            public void onComplete() {
                targetSubscriberCompletion.complete(404L);
            }
        });
    }

    @AfterEach
    void tearDown() {
        getObjectHelper.close();
    }

    CompletableFuture<Long> call() {
        return new AsyncResumableGetObjectOperation<>(getObjectHelper, requestBuilder, targetTransformer, 3).call();
    }

    private static class NoopSubscription implements Subscription {
        @Override
        public void request(long l) {
        }

        @Override
        public void cancel() {
        }
    }

    @Nested
    class Call {
        @Test
        void performsAGetObjectRequest() throws ExecutionException, InterruptedException, TimeoutException {
            assertEquals(1L, call().get(0, TimeUnit.DAYS));
            assertEquals(Collections.singletonList(GetObjectRequest.builder().applyMutation(requestBuilder).build()), getObjectHelper.getObjectRequests());
        }

        @Nested
        class WhenPrimaryRequestFails {

            @BeforeEach
            void setUp() {
                getObjectHelper.setObjectException("example-bucket", "path/to/my-key", new UnsupportedOperationException("b0rk"));
            }
            @Test
            void propagateTheFailure() {
                Exception e = assertThrows(ExecutionException.class, () -> call().get(1, TimeUnit.DAYS));
                assertEquals(UnsupportedOperationException.class, e.getCause().getClass());
                assertEquals("b0rk", e.getCause().getMessage());
            }
        }

        @Nested
        class WhenPrimaryRequestSendFails {
            @BeforeEach
            void setUp() {
                getObjectHelper.setObjectSendException("example-bucket", "path/to/my-key", new UnsupportedOperationException("b0rk"));
            }

            @Test
            void propagateTheFailure() {
                call();
                ArgumentCaptor<Throwable> argumentCaptor = ArgumentCaptor.forClass(Throwable.class);
                verify(targetTransformer).exceptionOccurred(argumentCaptor.capture());
                Throwable throwable = argumentCaptor.getValue();
                while (throwable instanceof CompletionException) {
                    throwable = throwable.getCause();
                }
                assertEquals(UnsupportedOperationException.class, throwable.getClass());
                assertEquals("b0rk", throwable.getMessage());
            }
        }

        @Nested
        class WhenDownloadingBodyFails {
            private int attempt;

            private final LinkedList<Subscription> subscriptions = new LinkedList<>();

            @BeforeEach
            void setUp() {
                attempt = 1;
                getObjectHelper.setObjectPublisher("example-bucket","path/to/my-key", SdkPublisher.adapt(s -> {
                    subscriptions.add(spy(new Subscription() {
                        private boolean first = true;
                        @Override
                        public void request(long l) {
                            if (first) {
                                first = false;
                                ByteBuffer buffer = ByteBuffer.allocate(10);
                                Arrays.fill(buffer.array(), (byte) ('@' + attempt++));
                                s.onNext(buffer);
                                s.onError(new TimeoutException("b0rk"));
                            }
                        }

                        @Override
                        public void cancel() {
                        }
                    }));
                    s.onSubscribe(subscriptions.getLast());
                }));
            }

            @Test
            void performsThreeRetries() throws ExecutionException, InterruptedException, TimeoutException {
                assertEquals(1L, call().get(0, TimeUnit.DAYS));
                assertEquals(4, getObjectHelper.getObjectRequests().size());
            }

            @Test
            void pushesDataFromSubsequentRequests() {
                call();
                assertEquals(
                    Arrays.asList("AAAAAAAAAA", "BBBBBBBBBB", "CCCCCCCCCC", "DDDDDDDDDD"),
                    resultBytes.stream().map(buffer -> StandardCharsets.ISO_8859_1.decode(buffer).toString()).collect(Collectors.toList())
                );
            }

            @Test
            void includesTheOriginalEtagAsIfMatchInSubsequentRequests() {
                call();
                assertNull(getObjectHelper.getObjectRequests().get(0).ifMatch());
                assertEquals("tag-1", getObjectHelper.getObjectRequests().get(1).ifMatch());
            }

            @Test
            void requestOnlyRemainingByteRange() {
                call();
                assertNull(getObjectHelper.getObjectRequests().get(0).range());
                assertEquals("bytes=10-", getObjectHelper.getObjectRequests().get(1).range());
                assertEquals("bytes=20-", getObjectHelper.getObjectRequests().get(2).range());
                assertEquals("bytes=30-", getObjectHelper.getObjectRequests().get(3).range());
            }

            @Test
            void propagatesRequestsForAdditionalData() {
                call();
                verify(subscriptions.get(0)).request(1L);
                verify(subscriptions.get(0)).request(2L);
                verify(subscriptions.get(1)).request(4L);
                verify(subscriptions.get(2)).request(8L);
                verify(subscriptions.get(3)).request(16L);
            }

            @Test
            void reissuesOutstandingRequestsOnRetries() {
                call();
                verify(subscriptions.get(1)).request(1L+2L-1L);
                verify(subscriptions.get(2)).request(1L+2L+4L-1L-1L);
                verify(subscriptions.get(3)).request(1L+2L+4L+8L-1L-1L-1L);
            }

            @Nested
            class AndExplicitlyCancelled {
                @BeforeEach
                void setUp() {
                    targetSubscriber = new Subscriber<ByteBuffer>() {
                        @Override
                        public void onSubscribe(Subscription subscription) {
                            subscription.cancel();
                        }

                        @Override
                        public void onNext(ByteBuffer byteBuffer) {

                        }

                        @Override
                        public void onError(Throwable throwable) {

                        }

                        @Override
                        public void onComplete() {

                        }
                    };
                }

                @Test
                void cancelSourceSubscription() {
                    call();
                    verify(subscriptions.get(0)).cancel();
                }

                @Test
                void preventRetries() {
                    call();
                    assertEquals(1, getObjectHelper.getObjectRequests().size());
                }
            }

            @Nested
            class AndSubsequentRequestFutureFails {
                @BeforeEach
                void setUp() {
                    getObjectHelper.setObjectPublisher("example-bucket","path/to/my-key", SdkPublisher.adapt(s -> {
                        getObjectHelper.removeObject("example-bucket", "path/to/my-key");
                        getObjectHelper.removeObjectPublisher("example-bucket", "path/to/my-key");
                        s.onSubscribe(new NoopSubscription());
                        s.onError(new RuntimeException("b0rk"));
                    }));
                }

                @Test
                void behavesLikeALateErrorInTheOriginalRequest() {
                    call();
                    ArgumentCaptor<Throwable> argumentCaptor = ArgumentCaptor.forClass(Throwable.class);
                    verify(targetSubscriber).onError(argumentCaptor.capture());
                    Throwable throwable = argumentCaptor.getValue();
                    assertEquals(RuntimeException.class, throwable.getClass());
                    assertEquals("b0rk", throwable.getMessage());
                    assertEquals(NoSuchKeyException.class, throwable.getSuppressed()[0].getClass());
                }
            }

            @Nested
            class AndSubsequentRequestFailsEarly {
                @BeforeEach
                void setUp() {
                    getObjectHelper.setObjectPublisher("example-bucket","path/to/my-key", SdkPublisher.adapt(s -> {
                        getObjectHelper.setObjectSendException("example-bucket", "path/to/my-key", new RuntimeException("second"));
                        s.onSubscribe(new NoopSubscription());
                        s.onError(new RuntimeException("b0rk"));
                    }));
                }

                @Test
                void behavesLikeALateErrorInTheOriginalRequest() {
                    call();
                    ArgumentCaptor<Throwable> argumentCaptor = ArgumentCaptor.forClass(Throwable.class);
                    verify(targetSubscriber).onError(argumentCaptor.capture());
                    Throwable throwable = argumentCaptor.getValue();
                    assertEquals(RuntimeException.class, throwable.getClass());
                    assertEquals("second", throwable.getMessage());
                }

                @Test
                void callsOnErrorExactlyOnce() {
                    call();
                    verify(targetSubscriber, times(1)).onError(any());
                }
            }
        }
    }
}
