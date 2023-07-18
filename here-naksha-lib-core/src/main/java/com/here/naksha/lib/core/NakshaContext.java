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

import com.here.naksha.lib.core.lambdas.F0;
import com.here.naksha.lib.core.util.NanoTime;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;

/**
 * A class implementing the logger interface that is thread local and exposes all kind of context information.
 */
public class NakshaContext {

  /**
   * A supplied that can be modified by an extending class, all instances of the logger then will be turned into the application class. This
   * can be used to redirect all logging. The default implementation will redirect all logging to SLF4j.
   */
  protected static final AtomicReference<F0<NakshaContext>> constructor = new AtomicReference<>(NakshaContext::new);

  /**
   * The thread local instance.
   */
  protected static final ThreadLocal<NakshaContext> instance =
      ThreadLocal.withInitial(() -> constructor.get().call());

  /**
   * Returns the current thread local context.
   *
   * @return The current thread local context.
   */
  public static @NotNull NakshaContext currentContext() {
    final NakshaContext logger = instance.get();
    final AbstractTask<?> task = AbstractTask.currentTask();
    if (task != null) {
      return logger.with(task.streamId(), task.startNanos());
    }
    final String threadName = Thread.currentThread().getName();
    //noinspection StringEquality
    if (logger.streamId() != threadName) {
      logger.with(threadName, NanoTime.now());
    }
    return logger;
  }

  /**
   * A special string that, when used as stream-id by the constructor, is replaced by the getter with a random string.
   */
  protected static final String CREATE_STREAM_ID = "";

  /**
   * Create a new thread local logger.
   */
  protected NakshaContext() {
    streamId = CREATE_STREAM_ID;
    startNanos = NanoTime.now();
    logger = new NakshaLogger(this);
  }

  /**
   * Create a new thread local logger.
   *
   * @param streamId   The initial stream-id.
   * @param startNanos The start nanos to use.
   */
  protected NakshaContext(@NotNull String streamId, long startNanos) {
    this.streamId = streamId;
    this.startNanos = startNanos;
    logger = new NakshaLogger(this);
  }

  /**
   * The logger.
   */
  public final @NotNull NakshaLogger logger;

  /**
   * The stream-id.
   */
  private @NotNull String streamId;

  /**
   * The start nano time for time measurements.
   */
  private long startNanos;

  /**
   * Returns the stream-id.
   *
   * @return The stream-id.
   */
  public @NotNull String streamId() {
    //noinspection StringEquality
    if (streamId == CREATE_STREAM_ID) {
      streamId = RandomStringUtils.randomAlphanumeric(12);
    }
    return streamId;
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
  public long startNanos() {
    return startNanos;
  }

  /**
   * Binds the thread local logger to the given stream and start time.
   *
   * @param streamId   The stream.
   * @param startNanos The start-time.
   * @return this.
   */
  public @NotNull NakshaContext with(@NotNull String streamId, long startNanos) {
    this.streamId = streamId;
    this.startNanos = startNanos;
    return this;
  }
}
