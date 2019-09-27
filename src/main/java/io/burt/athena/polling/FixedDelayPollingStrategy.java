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

public class FixedDelayPollingStrategy implements PollingStrategy {
    private final Duration delay;
    private final Sleeper sleeper;
    private Clock clock;

    FixedDelayPollingStrategy(Duration delay) {
        this(delay, duration -> TimeUnit.MILLISECONDS.sleep(duration.toMillis()), Clock.systemDefaultZone());
    }

    FixedDelayPollingStrategy(Duration delay, Sleeper sleeper, Clock clock) {
        this.delay = delay;
        this.sleeper = sleeper;
        this.clock = clock;
    }

    @Override
    public ResultSet pollUntilCompleted(PollingCallback callback, Instant deadline) throws SQLException, TimeoutException, ExecutionException, InterruptedException {
        while (true) {
            Optional<ResultSet> resultSet = callback.poll(deadline);
            if (resultSet.isPresent()) {
                return resultSet.get();
            } else {
                Duration beforeDeadline = Duration.between(clock.instant(), deadline);
                if (beforeDeadline.compareTo(delay) < 0) {
                    if (beforeDeadline.isNegative()) {
                        throw new TimeoutException();
                    }
                    sleeper.sleep(beforeDeadline);
                } else {
                    sleeper.sleep(delay);
                }
            }
        }
    }
}
