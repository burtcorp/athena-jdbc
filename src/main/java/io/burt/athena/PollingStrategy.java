package io.burt.athena;

public interface PollingStrategy {
    void waitUntilNext() throws InterruptedException;
}
