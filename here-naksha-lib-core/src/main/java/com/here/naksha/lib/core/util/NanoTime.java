package com.here.naksha.lib.core.util;

import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;

/**
 * A precise clock. The usage is mainly to measure elapsed time, create the clock, then ask for the
 * time passed via {@link #get(TimeUnit)}. Another usage is statically
 */
public class NanoTime {

  /** The current millis when initializing the nano-time class. */
  protected static final long __startMillis = System.currentTimeMillis();

  /** The current nanos when initializing the nano-time class. */
  protected static final long __startNanos = System.nanoTime();

  /**
   * Returns the current time in nanoseconds, basically just an alias for {@link System#nanoTime()},
   * but calling the method guarantees that the class is initialized and the private static {@code
   * __startMillis} is initialized, that is important to convert nanos precise into milliseconds.
   *
   * @return the current nano-time.
   */
  public static long now() {
    return System.nanoTime();
  }

  /** Create a new clock. */
  public NanoTime() {
    this.startNanos = System.nanoTime();
    this.startMillis = __startMillis + TimeUnit.NANOSECONDS.toMillis(startNanos - __startMillis);
  }

  /** The current millis when creating. */
  public final long startMillis;

  /** The current nanos when creating. */
  public final long startNanos;

  /**
   * Returns the time passed since creation in microseconds.
   *
   * @return the time passed since creation in microseconds.
   */
  public final long elapsedMicros() {
    return get(TimeUnit.MICROSECONDS);
  }

  /**
   * Returns the time passed since creation in milliseconds.
   *
   * @return the time passed since creation in milliseconds.
   */
  public final long elapsedMillis() {
    return get(TimeUnit.MILLISECONDS);
  }

  /**
   * Calculates the time passed since creation.
   *
   * @param timeUnit the time unit to return.
   * @return the time passed.
   */
  public final long get(@NotNull TimeUnit timeUnit) {
    return till(System.nanoTime(), timeUnit);
  }

  /**
   * Calculates the time passed since creation of the time and now.
   *
   * @param timeUnit the time-unit to return.
   * @return the time passed till now.
   */
  public final long tillNow(@NotNull TimeUnit timeUnit) {
    return till(System.nanoTime(), timeUnit);
  }

  /**
   * Calculates the time till the given time-stamp.
   *
   * @param nanos the nanos till when to calculate the time.
   * @param timeUnit the time-unit to return.
   * @return the time passed till the given nano time-stamp.
   */
  public final long till(long nanos, @NotNull TimeUnit timeUnit) {
    return TimeUnit.NANOSECONDS.convert(nanos - startNanos, timeUnit);
  }

  /**
   * Returns the epoch millis. This is method is precise on all platforms.
   *
   * @return the epoch millis.
   */
  public static long currentMillis() {
    return toMillis(System.nanoTime());
  }

  /**
   * Returns the epoch millis of the given nano time.
   *
   * @param nanos the nanos as queried via {@link System#nanoTime()}.
   * @return the epoch millis.
   */
  public static long toMillis(long nanos) {
    return __startMillis + TimeUnit.NANOSECONDS.toMillis(nanos - __startNanos);
  }

  /**
   * Returns the time passed since the given start nanos.
   *
   * @param startNanos the start timestamp queried via {@link System#nanoTime()}.
   * @param timeUnit the time-unit in which to calculate the elapsed time.
   * @return the elapsed time.
   */
  public static long timeSince(long startNanos, @NotNull TimeUnit timeUnit) {
    return TimeUnit.NANOSECONDS.convert(System.nanoTime() - startNanos, timeUnit);
  }
}
