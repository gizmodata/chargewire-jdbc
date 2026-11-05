package com.gizmodata.chargewire.jdbc.client;

import java.time.Duration;

/**
 * The clock and the pause the driver's {@code server_resuming} retries use. Tests replace it so
 * they need not wait for real {@code Retry-After} pauses.
 *
 * <p>Internal API.
 */
public interface ResumeClock {
  /** The system's monotonic clock, and {@link Thread#sleep}. */
  ResumeClock SYSTEM =
      new ResumeClock() {
        @Override
        public long nanoTime() {
          return System.nanoTime();
        }

        @Override
        public void sleep(Duration pause) throws InterruptedException {
          long millis = pause.toMillis();
          int nanos = (int) (pause.toNanos() - Duration.ofMillis(millis).toNanos());
          Thread.sleep(millis, nanos);
        }
      };

  /**
   * Returns a monotonic time, as {@link System#nanoTime()} does.
   *
   * @return nanoseconds since an arbitrary origin
   */
  long nanoTime();

  /**
   * Waits.
   *
   * @param pause how long
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  void sleep(Duration pause) throws InterruptedException;
}
