package io.burt.athena.support;

import org.mockito.AdditionalAnswers;
import org.mockito.stubbing.Stubber;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

public class TestDelayedCompletableFuture<T> extends CompletableFuture<T> {
    private static final Executor EXECUTOR = Executors.newCachedThreadPool();

    private CompletableFuture<T> wrappedFuture;
    private TestClock clock;

    public static <T> CompletableFuture<T> wrap(CompletableFuture<T> future, TestClock clock) {
        TestDelayedCompletableFuture<T> testFuture = new TestDelayedCompletableFuture<>(future, clock);
        @SuppressWarnings("unchecked") TestDelayedCompletableFuture<T> restrictedFuture = mock(
            TestDelayedCompletableFuture.class,
            withSettings()
              .stubOnly()
              .defaultAnswer(invocation -> {
                throw new UnsupportedOperationException(invocation.getMethod().toString());
            })
        );
        try {
            Stubber stubber = lenient().doAnswer(AdditionalAnswers.delegatesTo(testFuture));
            stubber.when(restrictedFuture).getWrappedFuture();
            stubber.when(restrictedFuture).get(anyLong(), any());
            stubber.when(restrictedFuture).thenApply(any());
            stubber.when(restrictedFuture).thenCombine(any(), any());
            stubber.when(restrictedFuture).toString();
            stubber.when(restrictedFuture).whenComplete(any());
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
        return restrictedFuture;
    }

    public static <T> CompletableFuture<T> wrapWithDelay(CompletableFuture<T> future, Duration delay, TestClock clock) {
        if (delay == null || delay.compareTo(Duration.ZERO) <= 0) {
            return wrap(future, clock);
        } else {
            CompletableFuture<T> newFuture = new CompletableFuture<>();
            EXECUTOR.execute(() -> {
                clock.tick(delay);
                try {
                    newFuture.complete(future.get());
                } catch (Exception e) {
                    newFuture.completeExceptionally(e);
                }
            });
            return wrap(newFuture, clock);
        }
    }

    private TestDelayedCompletableFuture(CompletableFuture<T> wrappedFuture, TestClock clock) {
        this.wrappedFuture = wrappedFuture;
        this.clock = clock;
    }

    public CompletableFuture<T> getWrappedFuture() {
        return wrappedFuture;
    }

    private <U> CompletableFuture<U> unwrap(CompletionStage<U> stage) {
        if (stage instanceof TestDelayedCompletableFuture) {
            return ((TestDelayedCompletableFuture<U>) stage).getWrappedFuture();
        } else {
            return stage.toCompletableFuture();
        }
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        Instant deadline = Instant.ofEpochMilli(unit.toMillis(timeout));
        T result = wrappedFuture.get(timeout, unit);
        if (deadline.isBefore(clock.instant())) {
            throw new TimeoutException("simulated timeout");
        } else {
            return result;
        }
    }

    @Override
    public <U> CompletableFuture<U> thenApply(Function<? super T, ? extends U> fn) {
        return new TestDelayedCompletableFuture<>(unwrap(wrappedFuture.thenApply(fn)), clock);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <U, V> CompletableFuture<V> thenCombine(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
        return new TestDelayedCompletableFuture<>(unwrap(wrappedFuture.thenCombine(unwrap(other), fn)), clock);
    }

    @Override
    public CompletableFuture<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
        return new TestDelayedCompletableFuture<>(unwrap(wrappedFuture.whenComplete(action)), clock);
    }
}
