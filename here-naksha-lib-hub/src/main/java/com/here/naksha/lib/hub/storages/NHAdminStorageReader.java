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
package com.here.naksha.lib.hub.storages;

import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.storage.Notification;
import com.here.naksha.lib.core.models.storage.ReadRequest;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.storage.IReadSession;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

public class NHAdminStorageReader implements IReadSession {

  private final int DEFAULT_FETCH_SIZE = 1_000;

  /** Current session, all read storage operations should be executed against */
  final @NotNull IReadSession session;

  private int fetchSize;

  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  protected NHAdminStorageReader(final @NotNull IReadSession reader) {
    this.session = reader;
    fetchSize = DEFAULT_FETCH_SIZE;
  }

  /**
   * Tests whether this session is connected to the master-node.
   *
   * @return {@code true}, if this session is connected to the master-node; {@code false} otherwise.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public boolean isMasterConnect() {
    return session.isMasterConnect();
  }

  /**
   * Returns the Naksha context bound to this read-connection.
   *
   * @return the Naksha context bound to this read-connection.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull NakshaContext getNakshaContext() {
    return session.getNakshaContext();
  }

  /**
   * Returns the amount of features to fetch at ones.
   *
   * @return the amount of features to fetch at ones.
   */
  @Override
  public int getFetchSize() {
    return fetchSize;
  }

  /**
   * Changes the amount of features to fetch at ones.
   *
   * @param size The amount of features to fetch at ones.
   */
  @Override
  public void setFetchSize(int size) {
    this.fetchSize = size;
  }

  /**
   * Returns the statement timeout.
   *
   * @param timeUnit The time-unit in which to return the timeout.
   * @return The timeout.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public long getStatementTimeout(@NotNull TimeUnit timeUnit) {
    return session.getStatementTimeout(timeUnit);
  }

  /**
   * Sets the statement timeout.
   *
   * @param timeout  The timeout to set.
   * @param timeUnit The unit of the timeout.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public void setStatementTimeout(long timeout, @NotNull TimeUnit timeUnit) {
    session.setStatementTimeout(timeout, timeUnit);
  }

  /**
   * Returns the lock timeout.
   *
   * @param timeUnit The time-unit in which to return the timeout.
   * @return The timeout.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public long getLockTimeout(@NotNull TimeUnit timeUnit) {
    return session.getLockTimeout(timeUnit);
  }

  /**
   * Sets the lock timeout.
   *
   * @param timeout  The timeout to set.
   * @param timeUnit The unit of the timeout.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public void setLockTimeout(long timeout, @NotNull TimeUnit timeUnit) {
    session.setLockTimeout(timeout, timeUnit);
  }

  /**
   * Execute the given read-request.
   *
   * @param readRequest input request
   * @return the result.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull Result execute(@NotNull ReadRequest<?> readRequest) {
    return session.execute(readRequest);
  }

  /**
   * Process the given notification.
   *
   * @param notification input notification
   * @return the result.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull Result process(@NotNull Notification<?> notification) {
    return session.process(notification);
  }

  /**
   * Closes the session, returns the underlying connection back to the connection pool. Any method of the session will from now on throw an
   * {@link IllegalStateException}.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public void close() {
    session.close();
  }
}
