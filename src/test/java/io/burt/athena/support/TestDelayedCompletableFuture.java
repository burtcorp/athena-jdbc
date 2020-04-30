package io.burt.athena.support;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TestDelayedCompletableFuture<T> extends CompletableFuture<T> {
    public CompletableFuture<T> wrappedFuture;
    public Instant finishedAt;
    public TestClock clock;

    public TestDelayedCompletableFuture(CompletableFuture<T> wrappedFuture, Duration delay, TestClock clock) {
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
