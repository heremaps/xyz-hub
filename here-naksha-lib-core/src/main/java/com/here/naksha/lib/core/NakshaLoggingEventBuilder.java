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

import java.util.function.Supplier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Marker;
import org.slf4j.spi.LoggingEventBuilder;

/**
 * Improved event builder interface.
 */
public interface NakshaLoggingEventBuilder extends LoggingEventBuilder, AutoCloseable {

  @Override
  @NotNull
  NakshaLoggingEventBuilder setCause(@Nullable Throwable cause);

  @Override
  @NotNull
  NakshaLoggingEventBuilder addMarker(@Nullable Marker marker);

  @Override
  @NotNull
  NakshaLoggingEventBuilder addArgument(@Nullable Object p);

  @Override
  @NotNull
  NakshaLoggingEventBuilder addArgument(@NotNull Supplier<?> objectSupplier);

  @Override
  @NotNull
  NakshaLoggingEventBuilder addKeyValue(String key, Object value);

  @Override
  @NotNull
  NakshaLoggingEventBuilder addKeyValue(String key, Supplier<Object> valueSupplier);

  @Override
  @NotNull
  NakshaLoggingEventBuilder setMessage(String message);

  @Override
  @NotNull
  NakshaLoggingEventBuilder setMessage(Supplier<String> messageSupplier);

  // -----------------------------------------------------------------------------------------------------------------------------------
  // --------------------------------------------< Extensions
  // >-------------------------------------------------------------------------
  // -----------------------------------------------------------------------------------------------------------------------------------

  /**
   *  Sets the message of the logging event.
   *
   *  @since 2.0.0-beta0
   */
  @NotNull
  NakshaLoggingEventBuilder msg(@NotNull String message);

  /**
   * Add an argument to the event being built.
   *
   * @param p an Object to add.
   * @return a LoggingEventBuilder, usually <b>this</b>.
   */
  @NotNull
  NakshaLoggingEventBuilder add(@Nullable Object p);

  /**
   * Add a {@link org.slf4j.event.KeyValuePair key value pair} to the event being built.
   *
   * @param key the key of the key value pair.
   * @param value the value of the key value pair.
   * @return a LoggingEventBuilder, usually <b>this</b>.
   */
  @NotNull
  NakshaLoggingEventBuilder put(@NotNull String key, @Nullable Object value);

  /**
   * Close the logger.
   */
  @Override
  default void close() {
    log();
  }
}
