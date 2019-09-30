package io.burt.athena.polling;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public interface PollingStrategy {
    ResultSet pollUntilCompleted(PollingCallback callback, Instant deadline) throws SQLException, TimeoutException, ExecutionException, InterruptedException;

    default Duration sleepDuration(Duration desired, Instant now, Instant deadline) throws TimeoutException {
        Duration beforeDeadline = Duration.between(now, deadline);
        if (beforeDeadline.compareTo(desired) < 0) {
            if (beforeDeadline.isNegative()) {
                throw new TimeoutException("polling reached deadline");
            }
            return beforeDeadline;
        } else {
            return desired;
        }
    }
}
