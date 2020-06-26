package io.burt.athena.result.s3;

import io.burt.athena.support.GetObjectHelper;
import io.burt.athena.support.TestNameGenerator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.utils.IoUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;


@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(TestNameGenerator.class)
public class GetObjectInputStreamTransformerTest {
    private GetObjectHelper getObjectHelper;
    private GetObjectRequest.Builder requestBuilder;
    private GetObjectInputStreamTransformer subject;

    @BeforeEach
    void setUp() {
        getObjectHelper = new GetObjectHelper();
        getObjectHelper.setObject("example-bucket", "path/to/my-key", "abcd".getBytes(StandardCharsets.UTF_8));
        requestBuilder = GetObjectRequest.builder().bucket("example-bucket").key("path/to/my-key");
        subject = new GetObjectInputStreamTransformer(getObjectHelper, requestBuilder, Duration.ofSeconds(1));
    }

    @AfterEach
    void tearDown() {
        getObjectHelper.close();
    }

    CompletableFuture<InputStream> call() {
        return getObjectHelper.getObject(requestBuilder.build(), subject);
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
    class Read {
        @Test
        void returnsStreamedContent() throws ExecutionException, InterruptedException, TimeoutException, IOException {
            assertEquals("abcd", IoUtils.toUtf8String(call().get(0, TimeUnit.DAYS)));
            assertEquals(Collections.singletonList(GetObjectRequest.builder().bucket("example-bucket").key("path/to/my-key").build()), getObjectHelper.getObjectRequests());
        }

        @Nested
        class WhenTheRequestFails {
            @BeforeEach
            void setUp() {
                getObjectHelper.setObjectException("example-bucket", "path/to/my-key", new UnsupportedOperationException("b0rk"));
            }
            @Test
            void propagatesTheFailure() {
                Exception e = assertThrows(ExecutionException.class, () -> call().get(1, TimeUnit.DAYS));
                assertEquals(UnsupportedOperationException.class, e.getCause().getClass());
                assertEquals("b0rk", e.getCause().getMessage());
            }
        }

        @Nested
        class WhenDownloadingTheBodyFails {
            @BeforeEach
            void setUp() {
                getObjectHelper.setObjectPublisher("example-bucket","path/to/my-key", SdkPublisher.adapt(s -> {
                    getObjectHelper.removeObjectPublisher("example-bucket", "path/to/my-key");
                    s.onSubscribe(new NoopSubscription());
                    s.onNext(ByteBuffer.wrap("1234".getBytes(StandardCharsets.UTF_8)));
                    s.onError(new TimeoutException("b0rk"));
                }));
            }

            @Test
            void retriesOnce() {
                call();
                IoUtils.drainInputStream(subject);
                assertEquals(2, getObjectHelper.getObjectRequests().size());
            }

            @Test
            void pushesDataFromRetry() throws IOException, InterruptedException, ExecutionException, TimeoutException {
                assertEquals("1234abcd", IoUtils.toUtf8String(call().get(0, TimeUnit.DAYS)));
            }

            @Test
            void includesTheOriginalEtagAsIfMatchInSubsequentRequests() {
                call();
                IoUtils.drainInputStream(subject);
                assertNull(getObjectHelper.getObjectRequests().get(0).ifMatch());
                assertEquals("tag-1", getObjectHelper.getObjectRequests().get(1).ifMatch());
            }

            @Test
            void requestOnlyRemainingByteRange() {
                call();
                IoUtils.drainInputStream(subject);
                assertNull(getObjectHelper.getObjectRequests().get(0).range());
                assertEquals("bytes=4-", getObjectHelper.getObjectRequests().get(1).range());
            }

            @Nested
            class AndRetryFails {
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
                void propagatesOriginalFailureWithSubsequentFailureSupressed() {
                    Throwable throwable = assertThrows(IOException.class, () -> IoUtils.toUtf8String(call().get(1, TimeUnit.SECONDS)));
                    assertEquals(RuntimeException.class, throwable.getCause().getClass());
                    assertEquals("b0rk", throwable.getCause().getMessage());
                    assertEquals(ExecutionException.class, throwable.getSuppressed()[0].getClass());
                    assertEquals(NoSuchKeyException.class, throwable.getSuppressed()[0].getCause().getClass());
                }
            }
        }
    }
}
