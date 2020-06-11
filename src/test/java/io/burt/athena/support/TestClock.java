package io.burt.athena.support;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicLong;

public class TestClock extends Clock {
  private AtomicLong millis = new AtomicLong();

  @Override
  public ZoneId getZone() {
    return ZoneId.of("UTC");
  }

  @Override
  public Clock withZone(ZoneId zone) {
    return null;
  }

  @Override
  public Instant instant() {
    return Instant.ofEpochMilli(millis());
  }

  @Override
  public long millis() {
    return this.millis.get();
  }

  public void tick(Duration duration) {
    this.millis.addAndGet(duration.toMillis());
  }
}
