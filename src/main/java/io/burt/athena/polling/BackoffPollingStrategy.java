package io.burt.athena.polling;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class BackoffPollingStrategy implements PollingStrategy {
    private final Duration firstDelay;
    private final Duration maxDelay;
    private final long factor;
    private final Sleeper sleeper;

    public BackoffPollingStrategy(Duration firstDelay, Duration maxDelay) {
        this(firstDelay, maxDelay, 2L, duration -> TimeUnit.MILLISECONDS.sleep(duration.toMillis()));
    }

    public BackoffPollingStrategy(Duration firstDelay, Duration maxDelay, long factor) {
        this(firstDelay, maxDelay, factor, duration -> TimeUnit.MILLISECONDS.sleep(duration.toMillis()));
    }

    BackoffPollingStrategy(Duration firstDelay, Duration maxDelay, Sleeper sleeper) {
        this(firstDelay, maxDelay, 2L, sleeper);
    }

    BackoffPollingStrategy(Duration firstDelay, Duration maxDelay, long factor, Sleeper sleeper) {
        this.firstDelay = firstDelay;
        this.maxDelay = maxDelay;
        this.factor = factor;
        this.sleeper = sleeper;
    }

    @Override
    public ResultSet pollUntilCompleted(PollingCallback callback) throws SQLException, TimeoutException, ExecutionException, InterruptedException {
        Duration nextDelay = firstDelay;
        while (true) {
            Optional<ResultSet> resultSet = callback.poll();
            if (resultSet.isPresent()) {
                return resultSet.get();
            } else {
                sleeper.sleep(nextDelay);
                nextDelay = nextDelay.multipliedBy(factor);
                if (nextDelay.compareTo(maxDelay) > 0) {
                    nextDelay = maxDelay;
                }
            }
        }
    }
}
