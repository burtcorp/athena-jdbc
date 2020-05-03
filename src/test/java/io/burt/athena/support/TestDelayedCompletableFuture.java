package io.burt.athena.support;

import org.mockito.AdditionalAnswers;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class TestDelayedCompletableFuture<T> extends CompletableFuture<T> {
    public CompletableFuture<T> wrappedFuture;
    public Instant finishedAt;
    public TestClock clock;

    static <T> CompletableFuture<T> create(CompletableFuture<T> future, Duration delay, TestClock clock) {
        TestDelayedCompletableFuture<T> testFuture = new TestDelayedCompletableFuture<>(future, delay, clock);
        @SuppressWarnings("unchecked") CompletableFuture<T> restrictedFuture = mock(CompletableFuture.class, invocation -> {
            throw new UnsupportedOperationException(invocation.getMethod().toString());
        });
        try {
            doAnswer(AdditionalAnswers.delegatesTo(testFuture)).when(restrictedFuture).get(anyLong(), any());
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
        return restrictedFuture;
    }

    private TestDelayedCompletableFuture(CompletableFuture<T> wrappedFuture, Duration delay, TestClock clock) {
        this.wrappedFuture = wrappedFuture;
        this.finishedAt = clock.instant().plus(delay);
        this.clock = clock;
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        Instant deadline = clock.instant().plusMillis(unit.toMillis(timeout));
        if (deadline.isBefore(finishedAt)) {
            clock.tick(Duration.ofMillis(deadline.toEpochMilli()));
            throw new TimeoutException("simulated timeout");
        } else {
            clock.tick(Duration.ofMillis(finishedAt.toEpochMilli()));
            return wrappedFuture.get();
        }
    }
}
