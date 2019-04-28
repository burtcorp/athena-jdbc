package io.burt.athena.polling;

import java.sql.ResultSet;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class PollingStrategies {
    public static PollingStrategy simpleInterval() {
        return simpleInterval(100L);
    }

    public static PollingStrategy simpleInterval(final long delay) {
        return callback -> {
            while (true) {
                Optional<ResultSet> resultSet = callback.poll();
                if (resultSet.isPresent()) {
                    return resultSet.get();
                } else {
                    TimeUnit.MILLISECONDS.sleep(delay);
                }
            }
        };
    }
}
