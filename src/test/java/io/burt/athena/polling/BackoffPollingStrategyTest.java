package io.burt.athena.polling;

import io.burt.athena.support.TestClock;
import io.burt.athena.support.TestNameGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(TestNameGenerator.class)
class BackoffPollingStrategyTest {
    private Sleeper sleeper;
    private TestClock clock;
    private PollingStrategy pollingStrategy;

    @BeforeEach
    void setUp() {
        sleeper = mock(Sleeper.class);
        clock = new TestClock();
        pollingStrategy = new BackoffPollingStrategy(Duration.ofMillis(3), Duration.ofSeconds(1), sleeper, clock);
    }

    @Nested
    class PollUntilCompleted {
        @Captor ArgumentCaptor<Duration> delayCaptor;

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
            }, Instant.now().plus(Duration.ofSeconds(30)));
            assertEquals(3, counter.get());
        }

        @Test
        void returnsTheResultSet() throws Exception {
            ResultSet rs1 = mock(ResultSet.class);
            ResultSet rs2 = pollingStrategy.pollUntilCompleted((Instant deadline) -> Optional.of(rs1), clock.instant().plus(Duration.ofSeconds(30)));
            assertSame(rs1, rs2);
        }

        @Test
        void doublesTheDelayAfterEachPollUpToTheConfiguredMax() throws Exception {
            AtomicInteger counter = new AtomicInteger(0);
            pollingStrategy.pollUntilCompleted((Instant deadline) -> {
                if (counter.getAndIncrement() == 15) {
                    return Optional.of(mock(ResultSet.class));
                } else {
                    return Optional.empty();
                }
            }, clock.instant().plus(Duration.ofSeconds(30)));
            verify(sleeper, atLeastOnce()).sleep(delayCaptor.capture());
            List<Duration> delays = delayCaptor.getAllValues();
            assertEquals(Duration.ofMillis(3), delays.get(0));
            assertEquals(Duration.ofMillis(6), delays.get(1));
            assertEquals(Duration.ofMillis(12), delays.get(2));
            assertEquals(Duration.ofMillis(24), delays.get(3));
            assertEquals(Duration.ofMillis(48), delays.get(4));
            assertEquals(Duration.ofMillis(96), delays.get(5));
            assertEquals(Duration.ofMillis(192), delays.get(6));
            assertEquals(Duration.ofMillis(384), delays.get(7));
            assertEquals(Duration.ofMillis(768), delays.get(8));
            assertEquals(Duration.ofMillis(1000), delays.get(9));
            assertEquals(Duration.ofMillis(1000), delays.get(10));
            assertEquals(Duration.ofMillis(1000), delays.get(11));
            assertEquals(Duration.ofMillis(1000), delays.get(12));
        }

        @Test
        void reducesFinalDelayToMatchDeadline() throws Exception {
            AtomicInteger counter = new AtomicInteger(0);
            pollingStrategy.pollUntilCompleted((Instant deadline) -> {
                clock.tick(Duration.ofMillis(20));
                if (counter.getAndIncrement() >= 4) {
                    return Optional.of(mock(ResultSet.class));
                } else {
                    return Optional.empty();
                }
            }, clock.instant().plus(Duration.ofMillis(100)));
            verify(sleeper, atLeastOnce()).sleep(delayCaptor.capture());
            List<Duration> delays = delayCaptor.getAllValues();
            assertEquals(Duration.ofMillis(3), delays.get(0));
            assertEquals(Duration.ofMillis(6), delays.get(1));
            assertEquals(Duration.ofMillis(12), delays.get(2));
            assertEquals(Duration.ofMillis(20), delays.get(3));
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
        class WithAFactor {
            @BeforeEach
            void setUp() {
                pollingStrategy = new BackoffPollingStrategy(Duration.ofMillis(3), Duration.ofSeconds(1), 7, sleeper, clock);
            }

            @Test
            void usesTheFactorToCalculateTheNextDelay() throws Exception {
                AtomicInteger counter = new AtomicInteger(0);
                pollingStrategy.pollUntilCompleted((Instant deadline) -> {
                    if (counter.getAndIncrement() == 15) {
                        return Optional.of(mock(ResultSet.class));
                    } else {
                        return Optional.empty();
                    }
                }, clock.instant().plus(Duration.ofSeconds(30)));
                verify(sleeper, atLeastOnce()).sleep(delayCaptor.capture());
                List<Duration> delays = delayCaptor.getAllValues();
                assertEquals(Duration.ofMillis(3), delays.get(0));
                assertEquals(Duration.ofMillis(21), delays.get(1));
                assertEquals(Duration.ofMillis(147), delays.get(2));
                assertEquals(Duration.ofMillis(1000), delays.get(3));
                assertEquals(Duration.ofMillis(1000), delays.get(4));
                assertEquals(Duration.ofMillis(1000), delays.get(5));
            }
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
