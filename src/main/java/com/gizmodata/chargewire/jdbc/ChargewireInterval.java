package com.gizmodata.chargewire.jdbc;

import java.time.Duration;
import java.time.Period;
import java.util.Objects;

/**
 * A SQL interval of months, days and nanoseconds, as Arrow's {@code month_day_nano} interval holds
 * it. {@link java.sql.ResultSet#getObject(int)} returns this for such columns; {@link #toString()}
 * is ISO 8601 ({@code P1M2DT3.5S}).
 */
public final class ChargewireInterval {
  private final int months;
  private final int days;
  private final long nanos;

  /**
   * Creates an interval.
   *
   * @param months whole months
   * @param days whole days
   * @param nanos nanoseconds
   */
  public ChargewireInterval(int months, int days, long nanos) {
    this.months = months;
    this.days = days;
    this.nanos = nanos;
  }

  /**
   * Returns the months.
   *
   * @return the months
   */
  public int months() {
    return months;
  }

  /**
   * Returns the days.
   *
   * @return the days
   */
  public int days() {
    return days;
  }

  /**
   * Returns the nanoseconds.
   *
   * @return the nanoseconds
   */
  public long nanos() {
    return nanos;
  }

  /**
   * Returns the months and days as a {@link Period}.
   *
   * @return the date part
   */
  public Period period() {
    return Period.of(0, months, days);
  }

  /**
   * Returns the nanoseconds as a {@link Duration}.
   *
   * @return the time part
   */
  public Duration duration() {
    return Duration.ofNanos(nanos);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ChargewireInterval)) {
      return false;
    }
    ChargewireInterval that = (ChargewireInterval) other;
    return months == that.months && days == that.days && nanos == that.nanos;
  }

  @Override
  public int hashCode() {
    return Objects.hash(months, days, nanos);
  }

  @Override
  public String toString() {
    Period period = Period.of(0, months, days).normalized();
    Duration duration = Duration.ofNanos(nanos);
    if (period.isZero()) {
      return duration.toString();
    }
    return duration.isZero() ? period.toString() : period + duration.toString().substring(1);
  }
}
