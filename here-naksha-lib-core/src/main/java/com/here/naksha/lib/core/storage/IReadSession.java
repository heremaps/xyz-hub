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

import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.storage.Notification;
import com.here.naksha.lib.core.models.storage.ReadRequest;
import com.here.naksha.lib.core.models.storage.Result;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/**
 * A storage session that can only read. Each session is backed by a single storage connection with a single transaction.
 */
@AvailableSince(NakshaVersion.v2_0_7)
public interface IReadSession extends AutoCloseable {

  /**
   * Tests whether this session is connected to the master-node.
   *
   * @return {@code true}, if this session is connected to the master-node; {@code false} otherwise.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  boolean isMasterConnect();

  /**
   * Returns the Naksha context bound to this read-connection.
   *
   * @return the Naksha context bound to this read-connection.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  @NotNull
  NakshaContext getNakshaContext();

  /**
   * Returns the statement timeout.
   *
   * @param timeUnit The time-unit in which to return the timeout.
   * @return The timeout.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  long getStatementTimeout(@NotNull TimeUnit timeUnit);

  /**
   * Sets the statement timeout.
   *
   * @param timeout  The timeout to set.
   * @param timeUnit The unit of the timeout.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  void setStatementTimeout(long timeout, @NotNull TimeUnit timeUnit);

  /**
   * Returns the lock timeout.
   *
   * @param timeUnit The time-unit in which to return the timeout.
   * @return The timeout.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  long getLockTimeout(@NotNull TimeUnit timeUnit);

  /**
   * Sets the lock timeout.
   *
   * @param timeout  The timeout to set.
   * @param timeUnit The unit of the timeout.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  void setLockTimeout(long timeout, @NotNull TimeUnit timeUnit);

  /**
   * Execute the given read-request.
   *
   * @return the result.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  @NotNull
  Result execute(@NotNull ReadRequest<?> readRequest);

  /**
   * Process the given notification.
   *
   * @return the result.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  @NotNull
  Result process(@NotNull Notification<?> notification);

  /**
   * Closes the session, returns the underlying connection back to the connection pool. Any method of the session will from now on throw an
   * {@link IllegalStateException}.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  @Override
  void close();
}
