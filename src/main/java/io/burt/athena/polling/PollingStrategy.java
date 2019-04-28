package io.burt.athena.polling;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public interface PollingStrategy {
    ResultSet pollUntilCompleted(PollingCallback callback) throws SQLException, TimeoutException, ExecutionException, InterruptedException;
}
