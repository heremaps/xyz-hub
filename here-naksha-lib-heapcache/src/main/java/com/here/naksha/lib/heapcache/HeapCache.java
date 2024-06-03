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
package com.here.naksha.lib.heapcache;

import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.lambdas.Fe1;
import com.here.naksha.lib.core.lambdas.Pe1;
import com.here.naksha.lib.core.models.TxSignalSet;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.storage.CollectionInfo;
import com.here.naksha.lib.core.storage.IMasterTransaction;
import com.here.naksha.lib.core.storage.IReadSession;
import com.here.naksha.lib.core.storage.IReadTransaction;
import com.here.naksha.lib.core.storage.IStorage;
import com.here.naksha.lib.core.storage.ITransactionSettings;
import com.here.naksha.lib.core.storage.IWriteSession;
import com.here.naksha.lib.core.util.fib.FibSet;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class HeapCache implements IStorage {

  private final List<WeakReference<CacheChangeListener>> listeners = new ArrayList<>();

  public HeapCache(@NotNull HeapCacheConfig config) {
    this.config = config;
  }

  public void addListener(@NotNull CacheChangeListener listener) {
    listeners.add(new WeakReference<>(listener));
  }

  public void removeListener(@NotNull CacheChangeListener listener) {
    listeners.remove(listener);
  }

  private void notifyEntryAdded(String key, XyzFeature feature) {
    for (WeakReference<CacheChangeListener> reference : listeners) {
      CacheChangeListener listener = reference.get();
      if (listener != null) {
        listener.onCacheEntryAdded(key, feature);
      }
    }
  }

  private void notifyEntryUpdated(String key, XyzFeature feature) {
    for (WeakReference<CacheChangeListener> reference : listeners) {
      CacheChangeListener listener = reference.get();
      if (listener != null) {
        listener.onCacheEntryUpdated(key, feature);
      }
    }
  }

  private void notifyEntryRemoved(String key) {
    for (WeakReference<CacheChangeListener> reference : listeners) {
      CacheChangeListener listener = reference.get();
      if (listener != null) {
        listener.onCacheEntryRemoved(key);
      }
    }
  }

  public void putCacheEntry(String key, XyzFeature feature) {
    CacheEntry entry = cache.putWeak(key);
    entry.setValue(feature);
    // Trigger notification
    notifyEntryAdded(key, feature);
  }

  public void updateCacheEntry(String key, XyzFeature feature) {
    CacheEntry entry = cache.get(key);
    if (entry != null) {
      entry.setValue(feature);
      // Notify listeners
      notifyEntryUpdated(key, feature);
    }
  }

  public void removeCacheEntry(String key) {
    CacheEntry entry = cache.remove(key);
    if (entry != null) {
      // Notify listeners
      notifyEntryRemoved(key);
    }
  }

  protected final @NotNull HeapCacheConfig config;
  protected final @NotNull FibSet<String, CacheEntry> cache = new FibSet<>(CacheEntry::new);

  @Override
  public void init() {}

  @Override
  public void maintain(@NotNull List<CollectionInfo> collectionInfoList) {
    if (config.storage != null) {
      config.storage.maintain(collectionInfoList);
    }
  }

  @Override
  public @NotNull ITransactionSettings createSettings() {
    // assert config.storage != null;
    // return config.storage.createSettings();
    return null;
  }

  @Override
  public @NotNull IReadTransaction openReplicationTransaction(@NotNull ITransactionSettings settings) {
    return new HeapReadTx(this);
  }

  @Override
  public @NotNull IMasterTransaction openMasterTransaction(@NotNull ITransactionSettings settings) {
    return new HeapMasterTx(this);
  }

  @Override
  public void addListener(@NotNull Pe1<@NotNull TxSignalSet> listener) {}

  @Override
  public boolean removeListener(@NotNull Pe1<@NotNull TxSignalSet> listener) {
    return false;
  }

  @Override
  public void close() {}

  /**
   * Initializes the storage, create the transaction table, install needed scripts and extensions.
   */
  @Override
  public void initStorage() {}

  /**
   * Starts the maintainer thread that will take about history garbage collection, sequencing and other background jobs.
   */
  @Override
  public void startMaintainer() {}

  /**
   * Blocking call to perform maintenance tasks right now. One-time maintenance.
   */
  @Override
  public void maintainNow() {}

  /**
   * Stops the maintainer thread.
   */
  @Override
  public void stopMaintainer() {}

  /**
   * Open a new write-session, optionally to a master-node (when being in a multi-writer cluster).
   *
   * @param context   the {@link NakshaContext} to which to link the session.
   * @param useMaster {@code true} if the master-node should be connected to; false if any writer is okay.
   * @return the write-session.
   */
  @Override
  public @NotNull IWriteSession newWriteSession(@Nullable NakshaContext context, boolean useMaster) {
    return IStorage.super.newWriteSession(context, useMaster);
  }

  /**
   * Open a new read-session, optionally to a master-node to prevent replication lags.
   *
   * @param context   the {@link NakshaContext} to which to link the session.
   * @param useMaster {@code true} if the master-node should be connected to, to avoid replication lag; false if any reader is okay.
   * @return the read-session.
   */
  @Override
  public @NotNull IReadSession newReadSession(@Nullable NakshaContext context, boolean useMaster) {
    return IStorage.super.newReadSession(context, useMaster);
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
    // actually the method is never called in HeapCache, but I don't want to return null in @NotNull tagged method.
    return Executors.newSingleThreadExecutor().submit(() -> null);
  }
}
