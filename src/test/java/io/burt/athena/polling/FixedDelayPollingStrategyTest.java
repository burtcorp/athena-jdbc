package io.burt.athena.polling;

import io.burt.athena.support.TestClock;
import io.burt.athena.support.TestNameGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(TestNameGenerator.class)
class FixedDelayPollingStrategyTest {
    private Sleeper sleeper;
    private TestClock clock;
    private PollingStrategy pollingStrategy;

    @BeforeEach
    void setUp() {
        sleeper = mock(Sleeper.class);
        clock = new TestClock();
        pollingStrategy = new FixedDelayPollingStrategy(Duration.ofSeconds(3), sleeper, clock);
    }

    @Nested
    class PollUntilCompleted {
        @Test
        void pollsUntilTheCallbackReturnsAResultSet() throws Exception {
            AtomicInteger counter = new AtomicInteger(0);
            pollingStrategy.pollUntilCompleted((Instant deadline) -> {
                if (counter.get() == 3) {
                    return Optional.of(mock(ResultSet.class));
                } else {
                    counter.incrementAndGet();
                    return Optional.empty();
                }
            }, clock.instant().plus(Duration.ofSeconds(30)));
            assertEquals(3, counter.get());
        }

        @Test
        void returnsTheResultSet() throws Exception {
            ResultSet rs1 = mock(ResultSet.class);
            ResultSet rs2 = pollingStrategy.pollUntilCompleted((Instant deadline) -> Optional.of(rs1), clock.instant().plus(Duration.ofSeconds(30)));
            assertSame(rs1, rs2);
        }

        @Test
        void delaysTheConfiguredDurationBetweenPolls() throws Exception {
            AtomicInteger counter = new AtomicInteger(0);
            pollingStrategy.pollUntilCompleted((Instant deadline) -> {
                if (counter.getAndIncrement() == 3) {
                    return Optional.of(mock(ResultSet.class));
                } else {
                    return Optional.empty();
                }
            }, clock.instant().plus(Duration.ofSeconds(30)));
            verify(sleeper, times(3)).sleep(Duration.ofSeconds(3));
        }

        @Test
        void reducesFinalDelayToMatchDeadline() throws Exception {
            AtomicInteger counter = new AtomicInteger(0);
            pollingStrategy.pollUntilCompleted((Instant deadline) -> {
                if (counter.getAndIncrement() >= 1) {
                    return Optional.of(mock(ResultSet.class));
                } else {
                    return Optional.empty();
                }
            }, clock.instant().plus(Duration.ofMillis(100)));
            verify(sleeper, times(1)).sleep(Duration.ofMillis(100));
        }

        @Test
        void throwsTimeoutExceptionIfNotCompletedWithinDeadline() throws Exception {
            assertThrows(TimeoutException.class, () -> {
               pollingStrategy.pollUntilCompleted((Instant deadline) -> {
                   clock.tick(Duration.ofSeconds(10));
                   return Optional.empty();
               }, clock.instant());
            });
        }

        @Nested
        class WhenTheCallbackThrowsAnException {
            @Test
            void passesTheExceptionAlong() {
                assertThrows(InterruptedException.class, () -> {
                    pollingStrategy.pollUntilCompleted((Instant deadline) -> {
                        throw new InterruptedException();
                    }, clock.instant().plus(Duration.ofSeconds(30)));
                });
                assertThrows(SQLException.class, () -> {
                    pollingStrategy.pollUntilCompleted((Instant deadline) -> {
                        throw new SQLException();
                    }, clock.instant().plus(Duration.ofSeconds(30)));
                });
                assertThrows(ExecutionException.class, () -> {
                    pollingStrategy.pollUntilCompleted((Instant deadline) -> {
                        throw new ExecutionException(new ArithmeticException());
                    }, clock.instant().plus(Duration.ofSeconds(30)));
                });
                assertThrows(TimeoutException.class, () -> {
                    pollingStrategy.pollUntilCompleted((Instant deadline) -> {
                        throw new TimeoutException();
                    }, clock.instant().plus(Duration.ofSeconds(30)));
                });
            }
        }
    }
}
