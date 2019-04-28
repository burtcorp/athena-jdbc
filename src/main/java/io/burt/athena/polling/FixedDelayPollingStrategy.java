package io.burt.athena.polling;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FixedDelayPollingStrategy implements PollingStrategy {
    private final Duration delay;
    private final Sleeper sleeper;

    public FixedDelayPollingStrategy(Duration delay) {
        this(delay, duration -> TimeUnit.MILLISECONDS.sleep(duration.toMillis()));
    }

    FixedDelayPollingStrategy(Duration delay, Sleeper sleeper) {
        this.delay = delay;
        this.sleeper = sleeper;
    }

    @Override
    public ResultSet pollUntilCompleted(PollingCallback callback) throws SQLException, TimeoutException, ExecutionException, InterruptedException {
        while (true) {
            Optional<ResultSet> resultSet = callback.poll();
            if (resultSet.isPresent()) {
                return resultSet.get();
            } else {
                sleeper.sleep(delay);
            }
        }
    }
}
