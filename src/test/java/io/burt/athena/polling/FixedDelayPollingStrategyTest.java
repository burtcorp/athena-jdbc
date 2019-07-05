package io.burt.athena.polling;

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
    private PollingStrategy pollingStrategy;

    @BeforeEach
    void setUp() {
        sleeper = mock(Sleeper.class);
        pollingStrategy = new FixedDelayPollingStrategy(Duration.ofSeconds(3), sleeper);
    }

    @Nested
    class PollUntilCompleted {
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
        void delaysTheConfiguredDurationBetweenPolls() throws Exception {
            AtomicInteger counter = new AtomicInteger(0);
            pollingStrategy.pollUntilCompleted(() -> {
                if (counter.getAndIncrement() == 3) {
                    return Optional.of(mock(ResultSet.class));
                } else {
                    return Optional.empty();
                }
            });
            verify(sleeper, times(3)).sleep(Duration.ofSeconds(3));
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
