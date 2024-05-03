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

import com.here.naksha.lib.core.exceptions.Unauthorized;
import com.here.naksha.lib.core.models.auth.ServiceMatrix;
import com.here.naksha.lib.core.util.NanoTime;
import com.here.naksha.lib.core.util.StreamInfo;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A context that is virtually attached to all threads in the JVM.
 */
@SuppressWarnings({"unchecked", "ConstantValue", "unused"})
@AvailableSince(NakshaVersion.v2_0_5)
public final class NakshaContext {

  private static final Logger logger = LoggerFactory.getLogger(NakshaContext.class);

  /**
   * The thread local instance.
   */
  static final ThreadLocal<NakshaContext> instance = withInitial(NakshaContext::new);

  /**
   * Returns the {@link NakshaContext} of the current thread. If the thread does not have a context yet, create a new context, attach it to
   * the current thread and return it.
   *
   * @return The {@link NakshaContext} of the current thread.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  public static @NotNull NakshaContext currentContext() {
    final AbstractTask<?, ?> task = AbstractTask.currentTask();
    return task != null ? task.context() : instance.get();
  }

  /**
   * Attaches this context to the current thread and returns the context that was previously attached.
   *
   * <b>Warning</b>: This method will not override the context of the current task. Should this thread be attached to an
   * {@link AbstractTask}, then the {@link AbstractTask#context()} takes precedence.
   *
   * @return The previously attached context.
   */
  public @NotNull NakshaContext attachToCurrentThread() {
    final NakshaContext old = instance.get();
    instance.set(this);
    return old;
  }

  /**
   * Create a new context with random stream-id.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public NakshaContext() {
    this(null);
  }

  /**
   * Create a new context with a given untrusted stream-id.
   *
   * @param streamId the stream-id to use; if {@code null}, then a random one is generated.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
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
  @AvailableSince(NakshaVersion.v2_0_7)
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
   * @deprecated Please use {@link #getStreamId()}.
   */
  @Deprecated
  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull String streamId() {
    return streamId;
  }

  /**
   * Returns the stream-id.
   *
   * @return The stream-id.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull String getStreamId() {
    return streamId;
  }

  /**
   * The steam-id of this task.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
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
  @AvailableSince(NakshaVersion.v2_0_7)
  public long startNanos() {
    return startNanos;
  }

  // TODO: Add setters and getters for app_id and author and user-rights-matrix!

  /**
   * Returns all attachments. It is totally okay to modify the returned map.
   *
   * @return The attachments.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
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
  @AvailableSince(NakshaVersion.v2_0_7)
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
  @AvailableSince(NakshaVersion.v2_0_7)
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
  @AvailableSince(NakshaVersion.v2_0_7)
  public <T> @NotNull Class<T> setAttachment(@NotNull T value) {
    if (value == null) {
      throw new NullPointerException();
    }
    attachments().put(value.getClass(), value);
    return (Class<T>) value.getClass();
  }

  /**
   * Returns the value for the give key; if it exists.
   *
   * @param key The key of the value to return.
   * @param <T> The value type.
   * @return The value or {@code null}, if no such key exists.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public <T> @Nullable T get(@NotNull Object key) {
    final @NotNull ConcurrentHashMap<@NotNull Object, Object> attachments = attachments();
    return (T) attachments.get(key);
  }

  /**
   * Returns the value for the give key and removes it; if it exists.
   *
   * @param key The key to remove.
   * @param <T> The value type.
   * @return The value of the key or {@code null}, if no such key existed.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public <T> @Nullable T remove(@NotNull Object key) {
    return (T) attachments().remove(key);
  }

  /**
   * Sets the given value in the {@link #attachments()} using the given key.
   *
   * @param key   The key.
   * @param value the value to set.
   * @return the key.
   * @throws NullPointerException if the given value is null.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull NakshaContext with(@NotNull Object key, @NotNull Object value) {
    if (value == null) {
      throw new NullPointerException();
    }
    attachments().put(key, value);
    return this;
  }

  /**
   * Sets the given streamInfo object in the {@link #attachments()}, if it is not null.
   *
   * @param streamInfo the streamInfo object to be added
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public void attachStreamInfo(final @Nullable StreamInfo streamInfo) {
    if (streamInfo == null) return;
    attachments().put(StreamInfo.class.getSimpleName(), streamInfo);
  }

  /**
   * Returns streamInfo object from {@link #attachments()}.
   *
   * @return the StreamInfo object if available, otherwise null
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public @Nullable StreamInfo getStreamInfo() {
    final Object o = attachments().get(StreamInfo.class.getSimpleName());
    return (o == null) ? null : (StreamInfo) o;
  }

  /**
   * The attachments of this context.
   */
  private final @NotNull ConcurrentHashMap<@NotNull Object, Object> attachments;

  /**
   * Returns the application-identifier or throws an {@link Unauthorized} exception.
   *
   * @return the application-identifier.
   * @throws Unauthorized if the application-identifier is {@code null.}
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull String getAppId() {
    final String appId = this.app_id;
    if (appId == null) {
      throw new Unauthorized();
    }
    return appId;
  }

  /**
   * Returns the raw application identifier.
   *
   * @return the raw application identifier.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public @Nullable String rawAppId() {
    return app_id;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public void setAppId(@Nullable String app_id) {
    this.app_id = app_id;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull NakshaContext withAppId(@Nullable String app_id) {
    this.app_id = app_id;
    return this;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public @Nullable String getAuthor() {
    return author;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public void setAuthor(@Nullable String author) {
    this.author = author;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull NakshaContext withAuthor(@Nullable String author) {
    this.author = author;
    return this;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  private @Nullable String app_id;

  @AvailableSince(NakshaVersion.v2_0_7)
  private @Nullable String author;

  @AvailableSince(NakshaVersion.v2_0_16)
  private @Nullable ServiceMatrix urm;

  @AvailableSince(NakshaVersion.v2_0_16)
  public @Nullable ServiceMatrix getUrm() {
    return urm;
  }

  @AvailableSince(NakshaVersion.v2_0_16)
  public void setUrm(@Nullable ServiceMatrix urm) {
    this.urm = urm;
  }

  @AvailableSince(NakshaVersion.v2_0_16)
  public @NotNull NakshaContext withUrm(@Nullable ServiceMatrix urm) {
    this.urm = urm;
    return this;
  }

  @AvailableSince(NakshaVersion.v2_0_16)
  private boolean superUser = false;

  @AvailableSince(NakshaVersion.v2_0_16)
  public boolean isSuperUser() {
    return superUser;
  }

  @AvailableSince(NakshaVersion.v2_0_16)
  /*
  This should be set only in rare cases where recursive / multiple layers of the Auth check needs to be avoided when request has already passed the Auth check in first layer.
  */
  public void setSuperUser(boolean superUser) {
    this.superUser = superUser;
  }

  @AvailableSince(NakshaVersion.v2_0_16)
  /*
  This should be set only in rare cases where recursive / multiple layers of the Auth check needs to be avoided when request has already passed the Auth check in first layer.
  */
  public @NotNull NakshaContext withSuperUser(boolean superUser) {
    setSuperUser(superUser);
    return this;
  }
}
