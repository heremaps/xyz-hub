/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */
package com.here.naksha.lib.core.util;

import com.here.naksha.lib.core.NakshaVersion;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/**
 * A precise clock. The usage is mainly to measure elapsed time, create the clock, then ask for the time passed via {@link #get(TimeUnit)}.
 */
public class NanoTime {

  /**
   * The current millis when initializing the nano-time class.
   */
  protected static final long __startMillis;

  /**
   * The current nanos when initializing the nano-time class.
   */
  protected static final long __startNanos;

  /**
   * Returns the system start nanos.
   * @return The system start nanos.
   */
  public static long systemStart() {
    return __startNanos;
  }

  // Note: We grab the current millis, knowing the value is cached.
  //       Then we run in a tight loop to wait for the OS or underlying to update the millis.
  //       When that happens, we take a precise nano-time, so that we know that we have the nano-time for an exact
  // millisecond.
  static {
    final long adjustMe = System.currentTimeMillis();
    long startMillis = System.currentTimeMillis();
    while (startMillis == adjustMe) {
      startMillis = System.currentTimeMillis();
    }
    long startNanos = System.nanoTime();
    __startMillis = startMillis;
    __startNanos = startNanos;
  }

  /**
   * Returns the current time in nanoseconds, basically just an alias for {@link System#nanoTime()}, but calling the method guarantees that
   * the class is initialized and the private static {@code __startMillis} is initialized, that is important to convert nanos precise into
   * milliseconds.
   *
   * @return the current nano-time.
   */
  public static long now() {
    return System.nanoTime();
  }

  /**
   * Create a new clock.
   */
  public NanoTime() {
    this.startNanos = System.nanoTime();
    this.startMillis = __startMillis + TimeUnit.NANOSECONDS.toMillis(startNanos - __startNanos);
  }

  /**
   * The current millis when creating.
   */
  public final long startMillis;

  /**
   * The current nanos when creating.
   */
  public final long startNanos;

  /**
   * Returns the time passed since creation in nanoseconds.
   *
   * @return the time passed since creation in nanoseconds.
   */
  @AvailableSince(NakshaVersion.v2_0_6)
  public final long elapsedNanos() {
    return get(TimeUnit.NANOSECONDS);
  }

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
   * Calculates the time passed since creation and now.
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
   * @param nanos    the nanos till when to calculate the time.
   * @param timeUnit the time-unit to return.
   * @return the time passed till the given nano time-stamp.
   */
  public final long till(long nanos, @NotNull TimeUnit timeUnit) {
    return timeUnit.convert(nanos - startNanos, TimeUnit.NANOSECONDS);
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
   * @param timeUnit   the time-unit in which to calculate the elapsed time.
   * @return the elapsed time.
   */
  public static long timeSince(long startNanos, @NotNull TimeUnit timeUnit) {
    return timeUnit.convert(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
  }

  /**
   * Returns the time passed since the service state, or more detailed since loading this class. It is recommended to call
   * {@link NanoTime#now()} as soon as possible to get this class to be initialized.
   *
   * @param timeUnit the time-unit in which to calculate the elapsed time.
   * @return the elapsed time.
   */
  public static long timeSinceStart(@NotNull TimeUnit timeUnit) {
    return timeUnit.convert(System.nanoTime() - __startNanos, TimeUnit.NANOSECONDS);
  }
}
