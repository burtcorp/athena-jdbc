package io.burt.athena.polling;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public interface PollingStrategy {
    ResultSet pollUntilCompleted(PollingCallback callback, Instant deadline) throws SQLException, TimeoutException, ExecutionException, InterruptedException;
}
