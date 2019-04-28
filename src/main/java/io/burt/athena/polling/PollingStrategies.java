package io.burt.athena.polling;

public class PollingStrategies {
    public static PollingStrategy simpleInterval() {
        return simpleInterval(100L);
    }

    public static PollingStrategy simpleInterval(final long delay) {
        return new PollingStrategy() {
            @Override
            public void waitUntilNext() throws InterruptedException {
                Thread.sleep(delay);
            }
        };
    }
}
