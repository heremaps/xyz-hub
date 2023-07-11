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
package com.here.naksha.lib.core.storage;

import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The transaction settings.
 */
public interface ITransactionSettings {

  /**
   * Returns the statement timeout.
   *
   * @param timeUnit The time-unit in which to return the timeout.
   * @return The timeout.
   */
  long getStatementTimeout(@NotNull TimeUnit timeUnit);

  /**
   * Sets the statement timeout.
   *
   * @param timeout  The timeout to set.
   * @param timeUnit The unit of the timeout.
   * @return this.
   * @throws Exception If any error occurred.
   */
  @NotNull
  ITransactionSettings withStatementTimeout(long timeout, @NotNull TimeUnit timeUnit) throws Exception;

  /**
   * Returns the lock timeout.
   *
   * @param timeUnit The time-unit in which to return the timeout.
   * @return The timeout.
   */
  long getLockTimeout(@NotNull TimeUnit timeUnit);

  /**
   * Sets the lock timeout.
   *
   * @param timeout  The timeout to set.
   * @param timeUnit The unit of the timeout.
   * @return this.
   * @throws Exception If any error occurred.
   */
  @NotNull
  ITransactionSettings withLockTimeout(long timeout, @NotNull TimeUnit timeUnit) throws Exception;

  /**
   * Returns the currently set application identifier.
   *
   * @throws Exception If any error occurred, for example no application identifier set.
   */
  @NotNull
  String getAppId() throws Exception;

  /**
   * Returns the currently set application identifier.
   *
   * @param app_id The application identifier to set.
   * @throws Exception If any error occurred, for example no application identifier set.
   */
  @NotNull
  ITransactionSettings withAppId(@NotNull String app_id) throws Exception;

  /**
   * Returns the currently set author.
   *
   * @throws Exception If any error occurred.
   */
  @Nullable
  String getAuthor() throws Exception;

  /**
   * Returns the currently set application identifier.
   *
   * @param author The application identifier to set.
   * @throws Exception If any error occurred, for example no application identifier set.
   */
  @NotNull
  ITransactionSettings withAuthor(@Nullable String author) throws Exception;
}
