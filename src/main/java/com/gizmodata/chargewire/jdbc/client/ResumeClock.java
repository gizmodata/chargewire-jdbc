package com.gizmodata.chargewire.jdbc.client;

import java.time.Duration;
import java.util.function.BooleanSupplier;

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

        @Override
        public void sleep(Duration pause, BooleanSupplier stop, Duration step)
            throws InterruptedException {
          long end = nanoTime() + pause.toNanos();
          for (long left = pause.toNanos();
              left > 0 && !stop.getAsBoolean();
              left = end - nanoTime()) {
            sleep(Duration.ofNanos(Math.min(left, step.toNanos())));
          }
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

  /**
   * Waits, but returns early once {@code stop} says so, looking at it every {@code step}. The
   * system clock does so; by default (test clocks) it waits the whole pause in one {@link
   * #sleep(Duration)}, and the caller looks at {@code stop} again afterwards.
   *
   * @param pause how long at most
   * @param stop whether to stop waiting
   * @param step how often to look at {@code stop}
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  default void sleep(Duration pause, BooleanSupplier stop, Duration step)
      throws InterruptedException {
    sleep(pause);
  }
}
