package io.burt.athena.polling;

import java.time.Duration;

public class PollingStrategies {
    public static PollingStrategy defaultFixedDelay() {
        return fixedDelay(Duration.ofMillis(100));
    }

    public static PollingStrategy fixedDelay(Duration delay) {
        return new FixedDelayPollingStrategy(delay);
    }
}
