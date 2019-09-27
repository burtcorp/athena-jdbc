package io.burt.athena.polling;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class BackoffPollingStrategy implements PollingStrategy {
    private final Duration firstDelay;
    private final Duration maxDelay;
    private final long factor;
    private final Sleeper sleeper;
    private final Clock clock;

    BackoffPollingStrategy(Duration firstDelay, Duration maxDelay) {
        this(firstDelay, maxDelay, 2L, duration -> TimeUnit.MILLISECONDS.sleep(duration.toMillis()), Clock.systemDefaultZone());
    }

    BackoffPollingStrategy(Duration firstDelay, Duration maxDelay, long factor) {
        this(firstDelay, maxDelay, factor, duration -> TimeUnit.MILLISECONDS.sleep(duration.toMillis()), Clock.systemDefaultZone());
    }

    BackoffPollingStrategy(Duration firstDelay, Duration maxDelay, Sleeper sleeper) {
        this(firstDelay, maxDelay, 2L, sleeper, Clock.systemDefaultZone());
    }

    BackoffPollingStrategy(Duration firstDelay, Duration maxDelay, Sleeper sleeper, Clock clock) {
        this(firstDelay, maxDelay, 2L, sleeper, clock);
    }

    BackoffPollingStrategy(Duration firstDelay, Duration maxDelay, long factor, Sleeper sleeper, Clock clock) {
        this.firstDelay = firstDelay;
        this.maxDelay = maxDelay;
        this.factor = factor;
        this.sleeper = sleeper;
        this.clock = clock;
    }

    @Override
    public ResultSet pollUntilCompleted(PollingCallback callback, Instant deadline) throws SQLException, TimeoutException, ExecutionException, InterruptedException {
        Duration nextDelay = firstDelay;
        while (true) {
            Optional<ResultSet> resultSet = callback.poll(deadline);
            if (resultSet.isPresent()) {
                return resultSet.get();
            } else {
                Duration beforeDeadline = Duration.between(clock.instant(), deadline);
                if (beforeDeadline.compareTo(nextDelay) < 0) {
                    if (beforeDeadline.isNegative()) {
                        throw new TimeoutException();
                    }
                    sleeper.sleep(beforeDeadline);
                } else {
                    sleeper.sleep(nextDelay);
                }
                nextDelay = nextDelay.multipliedBy(factor);
                if (nextDelay.compareTo(maxDelay) > 0) {
                    nextDelay = maxDelay;
                }
            }
        }
    }
}
