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
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A context that is virtually attached to all threads in the JVM.
 */
@SuppressWarnings({"unchecked", "ConstantValue", "unused"})
public final class NakshaContext {

  private static final Logger logger = LoggerFactory.getLogger(NakshaContext.class);

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
    return instance.get();
  }

  /**
   * Create a new context with random stream-id.
   */
  public NakshaContext() {
    this(null);
  }

  /**
   * Create a new context with a given untrusted stream-id.
   *
   * @param streamId the stream-id to use; if {@code null}, then a random one is generated.
   */
  public NakshaContext(@Nullable String streamId) {
    this(streamId, false);
  }

  /**
   * Create a new context with a given stream-id.
   *
   * @param streamId      the stream-id to use; if {@code null}, then a random one is generated.
   * @param trustStreamId if the stream-id is coming from a trusted source; otherwise it is verified against a regular expression and when
   *                      not matching, it is replaced with a random one and a corresponding log entry is produced.
   */
  public NakshaContext(@Nullable String streamId, boolean trustStreamId) {
    this.startNanos = NanoTime.now();
    if (streamId != null) {
      if (!trustStreamId) {
        // TODO: Verify the stream-id
        //       Log if the stream-id is switched!
      }
    } else {
      streamId = RandomStringUtils.randomAlphanumeric(12);
    }
    this.streamId = streamId;
    this.attachments = new ConcurrentHashMap<>();
  }

  /**
   * The start nano time for time measurements.
   */
  private final long startNanos;

  /**
   * Returns the stream-id.
   *
   * @return The stream-id.
   */
  public @NotNull String streamId() {
    return streamId;
  }

  /**
   * The steam-id of this task.
   */
  private final @NotNull String streamId;

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

  // TODO: Add setters and getters for app_id and author and user-rights-matrix!

  /**
   * Returns all attachments. It is totally okay to modify the returned map.
   *
   * @return The attachments.
   */
  public @NotNull ConcurrentHashMap<@NotNull Object, Object> attachments() {
    return attachments;
  }

  /**
   * Returns the value for the give type; if it exists.
   *
   * @param valueClass the class of the value type.
   * @param <T>        the value type.
   * @return the value or {@code null}.
   */
  public <T> @Nullable T getAttachment(@NotNull Class<T> valueClass) {
    final @NotNull ConcurrentHashMap<@NotNull Object, Object> attachments = attachments();
    final Object value = attachments.get(valueClass);
    return valueClass.isInstance(value) ? valueClass.cast(value) : null;
  }

  /**
   * Returns the value for the give type. This method simply uses the given class as key in the {@link #attachments()} and expects that the
   * value is of the same type. If the value is {@code null} or of a wrong type, the method will create a new instance of the given value
   * class and store it in the attachments, returning the new instance.
   *
   * @param valueClass the class of the value type.
   * @param <T>        the value type.
   * @return the value.
   * @throws NullPointerException if creating a new value instance failed.
   */
  public <T> @NotNull T getOrCreateAttachment(@NotNull Class<T> valueClass) {
    final @NotNull ConcurrentHashMap<@NotNull Object, Object> attachments = attachments();
    while (true) {
      final Object o = attachments.get(valueClass);
      if (valueClass.isInstance(o)) {
        return valueClass.cast(o);
      }
      final T newValue;
      try {
        newValue = valueClass.getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        throw new NullPointerException();
      }
      final Object existingValue = attachments.putIfAbsent(valueClass, newValue);
      if (existingValue == null) {
        return newValue;
      }
      if (valueClass.isInstance(existingValue)) {
        return valueClass.cast(existingValue);
      }
      // Overwrite the existing value, because it is of the wrong type.
      if (attachments.replace(valueClass, existingValue, newValue)) {
        return newValue;
      }
      // Conflict, two threads seem to want to update the same key the same time!
    }
  }

  /**
   * Sets the given value in the {@link #attachments()} using the class of the value as key.
   *
   * @param value the value to set.
   * @return the key.
   * @throws NullPointerException if the given value is null.
   */
  public <T> @NotNull Class<T> setAttachment(@NotNull T value) {
    if (value == null) {
      throw new NullPointerException();
    }
    attachments().put(value.getClass(), value);
    return (Class<T>) value.getClass();
  }

  /**
   * The attachments of this context.
   */
  private final @NotNull ConcurrentHashMap<@NotNull Object, Object> attachments;
}
