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
import com.here.naksha.lib.core.lambdas.Pe1;
import com.here.naksha.lib.core.models.TxSignalSet;
import com.here.naksha.lib.core.storage.*;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class NHAdminStorage implements IStorage {

  /** Singleton instance of physical admin storage implementation */
  protected final @NotNull IStorage psqlStorage;

  public NHAdminStorage(final @NotNull IStorage psqlStorage) {
    this.psqlStorage = psqlStorage;
  }

  @Override
  public @NotNull IWriteSession newWriteSession(@Nullable NakshaContext context, boolean useMaster) {
    return new NHAdminStorageWriter(psqlStorage, context, useMaster);
  }

  @Override
  public @NotNull IReadSession newReadSession(@Nullable NakshaContext context, boolean useMaster) {
    return new NHAdminStorageReader(psqlStorage, context, useMaster);
  }

  /**
   * Initializes the storage, create the transaction table, install needed scripts and extensions.
   */
  @Override
  public void initStorage() {
    this.psqlStorage.initStorage();
  }

  /**
   * Starts the maintainer thread that will take about history garbage collection, sequencing and other background jobs.
   */
  @Override
  public void startMaintainer() {
    this.psqlStorage.startMaintainer();
  }

  /**
   * Blocking call to perform maintenance tasks right now. One-time maintenance.
   */
  @Override
  public void maintainNow() {
    this.psqlStorage.maintainNow();
  }

  /**
   * Stops the maintainer thread.
   */
  @Override
  public void stopMaintainer() {
    this.psqlStorage.stopMaintainer();
  }

  // TODO HP : remove all below deprecated methods at the end

  @Override
  public void init() {}

  @Override
  public void maintain(@NotNull List<CollectionInfo> collectionInfoList) {}

  @Override
  public @NotNull ITransactionSettings createSettings() {
    return null;
  }

  @Override
  public @NotNull IReadTransaction openReplicationTransaction(@NotNull ITransactionSettings settings) {
    return null;
  }

  @Override
  public @NotNull IMasterTransaction openMasterTransaction(@NotNull ITransactionSettings settings) {
    return null;
  }

  @Override
  public void addListener(@NotNull Pe1<@NotNull TxSignalSet> listener) {}

  @Override
  public boolean removeListener(@NotNull Pe1<@NotNull TxSignalSet> listener) {
    return false;
  }

  @Override
  public void close() {}
}
