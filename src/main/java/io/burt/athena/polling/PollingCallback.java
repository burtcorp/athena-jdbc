package io.burt.athena.polling;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@FunctionalInterface
public interface PollingCallback {
    Optional<ResultSet> poll(Instant deadline) throws SQLException, TimeoutException, ExecutionException, InterruptedException;
}
