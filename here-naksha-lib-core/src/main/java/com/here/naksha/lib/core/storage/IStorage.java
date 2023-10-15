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
import com.here.naksha.lib.core.lambdas.Pe1;
import com.here.naksha.lib.core.models.TxSignalSet;
import java.util.List;
import org.jetbrains.annotations.NotNull;

/**
 * Storage API to gain access to storages.
 */
public interface IStorage {

  /**
   * Creates a new context, bound by default to the current {@link NakshaContext#currentContext()}.
   * @return the new context.
   */
  @NotNull
  IStorageContext newContext();

  // TODO: - Add transaction log access.
  //       - Add history access.

  /**
   * Perform storage initialization, especially useful when invoked for the first time storage is to be accessed.
   */
  @Deprecated
  void init();

  /**
   * Perform maintenance tasks, for example garbage collect features that are older than the set {@link CollectionInfo#getMaxAge()}. This
   * task is at least called ones every 12 hours. It is guaranteed that this is only executed on one Naksha instances at a given time, so
   * there is no concurrent execution.
   */
  @Deprecated
  void maintain(@NotNull List<CollectionInfo> collectionInfoList);

  /**
   * Create default transaction settings.
   *
   * @return New transaction settings.
   */
  @Deprecated
  @NotNull
  ITransactionSettings createSettings();

  /**
   * Opens a read-only transaction, preferably from a replication node; if no replication node is available, then returns a transaction to
   * the master node.
   *
   * @param settings Optional settings for the transaction.
   * @return the read transaction.
   */
  @Deprecated
  @NotNull
  IReadTransaction openReplicationTransaction(@NotNull ITransactionSettings settings);

  /**
   * Opens a read/write transaction to the master node, all writes require at least a valid
   * {@link ITransactionSettings#withAppId(String) application-id} being set in the {@link ITransactionSettings}.
   *
   * @param settings Optional settings for the transaction.
   * @return The mutator transaction.
   */
  @NotNull
  IMasterTransaction openMasterTransaction(@NotNull ITransactionSettings settings);

  /**
   * Add a listener to be called, when something changes in the storage.
   *
   * @param listener The change listener to invoke, receiving the transaction set.
   */
  @Deprecated
  void addListener(@NotNull Pe1<@NotNull TxSignalSet> listener);

  /**
   * Remove the given listener.
   *
   * @param listener the change listener to remove.
   * @return {@code true} if the listener was removed; {@code false} otherwise.
   */
  @Deprecated
  boolean removeListener(@NotNull Pe1<@NotNull TxSignalSet> listener);

  /**
   * Closes the storage, may block for cleanup work.
   */
  @Deprecated
  void close();
}
