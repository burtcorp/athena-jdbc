package io.burt.athena.polling;

import io.burt.athena.support.TestNameGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
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
    @Mock private Sleeper sleeper;

    private PollingStrategy pollingStrategy;

    @BeforeEach
    void setUp() {
        pollingStrategy = new BackoffPollingStrategy(Duration.ofMillis(3), Duration.ofSeconds(1), sleeper);
    }

    @Nested
    class PollUntilCompleted {
        @Captor ArgumentCaptor<Duration> delayCaptor;

        @Test
        void pollsUntilTheCallbackReturnsAResultSet() throws Exception {
            AtomicInteger counter = new AtomicInteger(0);
            pollingStrategy.pollUntilCompleted(() -> {
                if (counter.get() == 3) {
                    return Optional.of(mock(ResultSet.class));
                } else {
                    counter.incrementAndGet();
                    return Optional.empty();
                }
            });
            assertEquals(3, counter.get());
        }

        @Test
        void returnsTheResultSet() throws Exception {
            ResultSet rs1 = mock(ResultSet.class);
            ResultSet rs2 = pollingStrategy.pollUntilCompleted(() -> Optional.of(rs1));
            assertSame(rs1, rs2);
        }

        @Test
        void doublesTheDelayAfterEachPollUpToTheConfiguredMax() throws Exception {
            AtomicInteger counter = new AtomicInteger(0);
            pollingStrategy.pollUntilCompleted(() -> {
                if (counter.getAndIncrement() == 15) {
                    return Optional.of(mock(ResultSet.class));
                } else {
                    return Optional.empty();
                }
            });
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

        @Nested
        class WithAFactor {
            @BeforeEach
            void setUp() {
                pollingStrategy = new BackoffPollingStrategy(Duration.ofMillis(3), Duration.ofSeconds(1), 7, sleeper);
            }

            @Test
            void usesTheFactorToCalculateTheNextDelay() throws Exception {
                AtomicInteger counter = new AtomicInteger(0);
                pollingStrategy.pollUntilCompleted(() -> {
                    if (counter.getAndIncrement() == 15) {
                        return Optional.of(mock(ResultSet.class));
                    } else {
                        return Optional.empty();
                    }
                });
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
                    pollingStrategy.pollUntilCompleted(() -> {
                        throw new InterruptedException();
                    });
                });
                assertThrows(SQLException.class, () -> {
                    pollingStrategy.pollUntilCompleted(() -> {
                        throw new SQLException();
                    });
                });
                assertThrows(ExecutionException.class, () -> {
                    pollingStrategy.pollUntilCompleted(() -> {
                        throw new ExecutionException(new ArithmeticException());
                    });
                });
                assertThrows(TimeoutException.class, () -> {
                    pollingStrategy.pollUntilCompleted(() -> {
                        throw new TimeoutException();
                    });
                });
            }
        }
    }
}
