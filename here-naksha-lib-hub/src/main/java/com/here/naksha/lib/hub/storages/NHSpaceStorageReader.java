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

import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.models.storage.Notification;
import com.here.naksha.lib.core.models.storage.ReadRequest;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.storage.IReadSession;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class NHSpaceStorageReader implements IReadSession {

  /** Singleton instance of NakshaHub storage implementation */
  protected final @NotNull INaksha nakshaHub;
  /** Runtime NakshaContext which is to be associated with read operations */
  protected final @NotNull NakshaContext context;
  /** Flag to indicate whether it has to connect to master storage instance or not */
  protected final boolean useMaster;

  public NHSpaceStorageReader(final @NotNull INaksha hub, @Nullable NakshaContext context, boolean useMaster) {
    this.nakshaHub = hub;
    this.context = (context != null) ? context : NakshaContext.currentContext();
    this.useMaster = useMaster;
  }

  /**
   * Tests whether this session is connected to the master-node.
   *
   * @return {@code true}, if this session is connected to the master-node; {@code false} otherwise.
   */
  @Override
  public boolean isMasterConnect() {
    return useMaster;
  }

  /**
   * Returns the Naksha context bound to this read-connection.
   *
   * @return the Naksha context bound to this read-connection.
   */
  @Override
  public @NotNull NakshaContext getNakshaContext() {
    return this.context;
  }

  /**
   * Returns the statement timeout.
   *
   * @param timeUnit The time-unit in which to return the timeout.
   * @return The timeout.
   */
  @Override
  public long getStatementTimeout(@NotNull TimeUnit timeUnit) {
    return 0;
  }

  /**
   * Sets the statement timeout.
   *
   * @param timeout  The timeout to set.
   * @param timeUnit The unit of the timeout.
   */
  @Override
  public void setStatementTimeout(long timeout, @NotNull TimeUnit timeUnit) {}

  /**
   * Returns the lock timeout.
   *
   * @param timeUnit The time-unit in which to return the timeout.
   * @return The timeout.
   */
  @Override
  public long getLockTimeout(@NotNull TimeUnit timeUnit) {
    return 0;
  }

  /**
   * Sets the lock timeout.
   *
   * @param timeout  The timeout to set.
   * @param timeUnit The unit of the timeout.
   */
  @Override
  public void setLockTimeout(long timeout, @NotNull TimeUnit timeUnit) {}

  /**
   * Execute the given read-request.
   *
   * @param readRequest
   * @return the result.
   */
  @Override
  public @NotNull Result execute(@NotNull ReadRequest<?> readRequest) {
    return null;
  }

  /**
   * Process the given notification.
   *
   * @param notification
   * @return the result.
   */
  @Override
  public @NotNull Result process(@NotNull Notification<?> notification) {
    return null;
  }

  /**
   * Closes the session, returns the underlying connection back to the connection pool. Any method of the session will from now on throw an
   * {@link IllegalStateException}.
   */
  @Override
  public void close() {}
}
