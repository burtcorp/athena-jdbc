package io.burt.athena.polling;

public interface PollingStrategy {
    void waitUntilNext() throws InterruptedException;
}
