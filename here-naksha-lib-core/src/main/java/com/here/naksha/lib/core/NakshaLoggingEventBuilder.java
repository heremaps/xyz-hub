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
package com.here.naksha.lib.core;

import java.util.function.Supplier;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Marker;
import org.slf4j.helpers.CheckReturnValue;
import org.slf4j.spi.LoggingEventBuilder;

/**
 * Improved event builder interface.
 */
@SuppressWarnings("ResultOfMethodCallIgnored")
public class NakshaLoggingEventBuilder implements LoggingEventBuilder {

  NakshaLoggingEventBuilder() {}

  LoggingEventBuilder eventBuilder;

  @CheckReturnValue
  @Override
  public @NotNull NakshaLoggingEventBuilder setCause(@Nullable Throwable cause) {
    eventBuilder.setCause(cause);
    return this;
  }

  @CheckReturnValue
  @Override
  public @NotNull NakshaLoggingEventBuilder addMarker(@Nullable Marker marker) {
    eventBuilder.addMarker(marker);
    return this;
  }

  @CheckReturnValue
  @Override
  public @NotNull NakshaLoggingEventBuilder addArgument(@Nullable Object p) {
    eventBuilder.addArgument(p);
    return this;
  }

  @CheckReturnValue
  @Override
  public @NotNull NakshaLoggingEventBuilder addArgument(@NotNull Supplier<?> objectSupplier) {
    eventBuilder.addArgument(objectSupplier);
    return this;
  }

  /**
   * Add an argument to the event being built.
   *
   * @param p an Object to add.
   * @return a LoggingEventBuilder, usually <b>this</b>.
   */
  @CheckReturnValue
  @AvailableSince(NakshaVersion.v2_0_6)
  public @NotNull NakshaLoggingEventBuilder add(@Nullable Object p) {
    eventBuilder.addArgument(p);
    return this;
  }

  @CheckReturnValue
  @Override
  public @NotNull NakshaLoggingEventBuilder addKeyValue(@NotNull String key, @Nullable Object value) {
    eventBuilder.addKeyValue(key, value);
    return this;
  }

  @CheckReturnValue
  @Override
  public @NotNull NakshaLoggingEventBuilder addKeyValue(
      @NotNull String key, @NotNull Supplier<@Nullable Object> valueSupplier) {
    eventBuilder.addKeyValue(key, valueSupplier);
    return this;
  }

  /**
   * Add a {@link org.slf4j.event.KeyValuePair key value pair} to the event being built.
   *
   * @param key the key of the key value pair.
   * @param value the value of the key value pair.
   * @return a LoggingEventBuilder, usually <b>this</b>.
   */
  @CheckReturnValue
  @AvailableSince(NakshaVersion.v2_0_6)
  public @NotNull NakshaLoggingEventBuilder put(@NotNull String key, @Nullable Object value) {
    eventBuilder.addKeyValue(key, value);
    return this;
  }

  @CheckReturnValue
  @Override
  public @NotNull NakshaLoggingEventBuilder setMessage(@NotNull String message) {
    eventBuilder.setMessage(message);
    return this;
  }

  @CheckReturnValue
  @Override
  public @NotNull NakshaLoggingEventBuilder setMessage(@NotNull Supplier<@NotNull String> messageSupplier) {
    eventBuilder.setMessage(messageSupplier);
    return this;
  }

  @Override
  public void log() {
    eventBuilder.log();
  }

  @Override
  public void log(@NotNull String message) {
    eventBuilder.log(message);
  }

  @Override
  public void log(@NotNull String message, @Nullable Object arg) {
    eventBuilder.log(message, arg);
  }

  @Override
  public void log(@NotNull String message, @Nullable Object arg0, @Nullable Object arg1) {
    eventBuilder.log(message, arg0, arg1);
  }

  @Override
  public void log(@NotNull String message, @Nullable Object... args) {
    eventBuilder.log(message, args);
  }

  @Override
  public void log(@NotNull Supplier<@NotNull String> messageSupplier) {
    eventBuilder.log(messageSupplier);
  }
}
