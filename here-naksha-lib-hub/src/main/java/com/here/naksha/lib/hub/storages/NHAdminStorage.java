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
import com.here.naksha.lib.core.lambdas.Fe1;
import com.here.naksha.lib.core.storage.IReadSession;
import com.here.naksha.lib.core.storage.IStorage;
import com.here.naksha.lib.core.storage.IWriteSession;
import java.util.concurrent.Future;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class NHAdminStorage implements IStorage {

  /** Singleton instance of physical admin storage implementation */
  protected final @NotNull IStorage psqlStorage;

  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public NHAdminStorage(final @NotNull IStorage psqlStorage) {
    this.psqlStorage = psqlStorage;
  }

  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull IWriteSession newWriteSession(@Nullable NakshaContext context, boolean useMaster) {
    return new NHAdminStorageWriter(this.psqlStorage.newWriteSession(context, useMaster));
  }

  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull IReadSession newReadSession(@Nullable NakshaContext context, boolean useMaster) {
    return new NHAdminStorageReader(this.psqlStorage.newReadSession(context, useMaster));
  }

  /**
   * Shutdown the storage instance asynchronously. This method returns asynchronously whatever the given {@code onShutdown} handler returns.
   * If no shutdown handler given, then {@code null} is returned.
   *
   * @param onShutdown The (optional) method to call when the shutdown is done.
   * @return The future when the shutdown will be done.
   */
  @Override
  public @NotNull <T> Future<T> shutdown(@Nullable Fe1<T, IStorage> onShutdown) {
    return null;
  }

  /**
   * Initializes the storage, create the transaction table, install needed scripts and extensions.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public void initStorage() {
    this.psqlStorage.initStorage();
  }

  /**
   * Starts the maintainer thread that will take about history garbage collection, sequencing and other background jobs.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public void startMaintainer() {
    this.psqlStorage.startMaintainer();
  }

  /**
   * Blocking call to perform maintenance tasks right now. One-time maintenance.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public void maintainNow() {
    this.psqlStorage.maintainNow();
  }

  /**
   * Stops the maintainer thread.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public void stopMaintainer() {
    this.psqlStorage.stopMaintainer();
  }
}
