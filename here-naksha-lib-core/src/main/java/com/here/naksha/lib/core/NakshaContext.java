/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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
package com.here.naksha.lib.core;

import static java.lang.ThreadLocal.withInitial;

import com.here.naksha.lib.core.util.NanoTime;
import org.jetbrains.annotations.NotNull;

/**
 * A context that is virtually attached to all threads in the JVM.
 */
public class NakshaContext {

  /**
   * The thread local instance.
   */
  private static final ThreadLocal<NakshaContext> instance = withInitial(NakshaContext::new);

  /**
   * Returns the {@link NakshaContext} of the current thread. If the thread does not have a context yet, create a new context, attach it to
   * the current thread and return it.
   *
   * @return The {@link NakshaContext} of the current thread.
   */
  public static @NotNull NakshaContext currentContext() {
    final NakshaContext context = instance.get();
    final AbstractTask<?> task = AbstractTask.currentTask();
    if (task != null) {
      return task;
    }
    return context;
  }

  /**
   * Create a new context with defaults.
   */
  protected NakshaContext() {
    startNanos = NanoTime.now();
    logger = new NakshaLogger(this);
  }

  @Deprecated
  public final @NotNull NakshaLogger logger;

  /**
   * The start nano time for time measurements.
   */
  protected long startNanos;

  /**
   * Returns the stream-id.
   *
   * @return The stream-id.
   */
  public @NotNull String getStreamId() {
    return Thread.currentThread().getName();
  }

  /**
   * The start nanoseconds. This can be used to calculate time differences, for example via:
   *
   * <pre>{@code
   * final long millis = NanoTime.timeSince(XyzLogger.currentLogger().startNanos(), TimeUnit.MILLIS);
   * }</pre>
   *
   * @return The start nanoseconds.
   */
  public long getStartNanos() {
    return startNanos;
  }

  /**
   * Sets the start-nanos.
   *
   * @param startNanos The start-nanos to set.
   * @throws IllegalArgumentException If the given value lies before the {@link NanoTime#systemStart()}.
   */
  public void setStartNanos(long startNanos) {
    if (startNanos < NanoTime.systemStart()) {
      throw new IllegalArgumentException("The start nanos must be greater or equal to the system start time");
    }
    this.startNanos = startNanos;
  }
}
