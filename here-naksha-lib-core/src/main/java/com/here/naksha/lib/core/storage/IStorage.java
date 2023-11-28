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

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;

import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.lambdas.Fe1;
import com.here.naksha.lib.core.lambdas.Pe1;
import com.here.naksha.lib.core.models.TxSignalSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Storage API to gain access to storages.
 */
@AvailableSince(NakshaVersion.v2_0_6)
public interface IStorage extends AutoCloseable {

  /**
   * Initializes the storage, create the transaction table, install needed scripts and extensions.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  void initStorage();

  /**
   * Initializes the storage, create the transaction table, install needed scripts and extensions.
   *
   * @param params Special parameters that are storage dependent to influence how a storage is initialized.
   */
  @AvailableSince(NakshaVersion.v2_0_8)
  default void initStorage(@Nullable Map<String, Object> params) {
    initStorage();
  }

  /**
   * Starts the maintainer thread that will take about history garbage collection, sequencing and other background jobs.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  void startMaintainer();

  /**
   * Blocking call to perform maintenance tasks right now. One-time maintenance.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  void maintainNow();

  /**
   * Stops the maintainer thread.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  void stopMaintainer();

  /**
   * Open a new write-session, optionally to a master-node (when being in a multi-writer cluster).
   *
   * @param context   the {@link NakshaContext} to which to link the session.
   * @param useMaster {@code true} if the master-node should be connected to; false if any writer is okay.
   * @return the write-session.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  default @NotNull IWriteSession newWriteSession(@Nullable NakshaContext context, boolean useMaster) {
    if (context == null) {
      context = NakshaContext.currentContext();
    }
    throw new UnsupportedOperationException();
  }

  /**
   * Open a new read-session, optionally to a master-node to prevent replication lags.
   *
   * @param context   the {@link NakshaContext} to which to link the session.
   * @param useMaster {@code true} if the master-node should be connected to, to avoid replication lag; false if any reader is okay.
   * @return the read-session.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  default @NotNull IReadSession newReadSession(@Nullable NakshaContext context, boolean useMaster) {
    if (context == null) {
      context = NakshaContext.currentContext();
    }
    throw new UnsupportedOperationException();
  }

  /**
   * Perform storage initialization, especially useful when invoked for the first time storage is to be accessed.
   */
  @Deprecated
  default void init() {
    initStorage();
  }

  /**
   * Perform maintenance tasks, for example garbage collect features that are older than the set {@link CollectionInfo#getMaxAge()}. This
   * task is at least called ones every 12 hours. It is guaranteed that this is only executed on one Naksha instances at a given time, so
   * there is no concurrent execution.
   */
  @Deprecated
  default void maintain(@NotNull List<CollectionInfo> collectionInfoList) {
    throw new UnsupportedOperationException();
  }

  /**
   * Create default transaction settings.
   *
   * @return New transaction settings.
   */
  @Deprecated
  default @NotNull ITransactionSettings createSettings() {
    throw new UnsupportedOperationException();
  }

  /**
   * Opens a read-only transaction, preferably from a replication node; if no replication node is available, then returns a transaction to
   * the master node.
   *
   * @param settings Optional settings for the transaction.
   * @return the read transaction.
   */
  @Deprecated
  default @NotNull IReadTransaction openReplicationTransaction(@NotNull ITransactionSettings settings) {
    throw new UnsupportedOperationException();
  }

  /**
   * Opens a read/write transaction to the master node, all writes require at least a valid
   * {@link ITransactionSettings#withAppId(String) application-id} being set in the {@link ITransactionSettings}.
   *
   * @param settings Optional settings for the transaction.
   * @return The mutator transaction.
   */
  @Deprecated
  default @NotNull IMasterTransaction openMasterTransaction(@NotNull ITransactionSettings settings) {
    throw new UnsupportedOperationException();
  }

  /**
   * Add a listener to be called, when something changes in the storage.
   *
   * @param listener The change listener to invoke, receiving the transaction set.
   */
  @Deprecated
  default void addListener(@NotNull Pe1<@NotNull TxSignalSet> listener) {
    throw new UnsupportedOperationException();
  }

  /**
   * Remove the given listener.
   *
   * @param listener the change listener to remove.
   * @return {@code true} if the listener was removed; {@code false} otherwise.
   */
  @Deprecated
  default boolean removeListener(@NotNull Pe1<@NotNull TxSignalSet> listener) {
    throw new UnsupportedOperationException();
  }

  /**
   * Shutdown the storage instance. The method will block until the storage is down including all open connections, statements and cursors.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  @Override
  default void close() {
    while (true) {
      try {
        shutdown(null).get();
        return;
      } catch (InterruptedException ignore) {
      } catch (Exception e) {
        throw unchecked(e);
      }
    }
  }

  /**
   * Shutdown the storage instance asynchronously. This method returns asynchronously whatever the given {@code onShutdown} handler returns.
   * If no shutdown handler given, then {@code null} is returned.
   *
   * @param onShutdown The (optional) method to call when the shutdown is done.
   * @return The future when the shutdown will be done.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  <T> @NotNull Future<T> shutdown(@Nullable Fe1<T, IStorage> onShutdown);
}
