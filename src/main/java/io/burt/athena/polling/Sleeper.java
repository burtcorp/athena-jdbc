package io.burt.athena.polling;

import java.time.Duration;

interface Sleeper {
    void sleep(Duration duration) throws InterruptedException;
}
