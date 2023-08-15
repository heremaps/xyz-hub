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
package com.here.naksha.lib.heapcache;

import com.here.naksha.lib.core.lambdas.Pe1;
import com.here.naksha.lib.core.models.TxSignalSet;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.storage.*;
import com.here.naksha.lib.core.util.fib.FibSet;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public class HeapCache implements IStorage {

  private final List<WeakReference<CacheChangeListener>> listeners = new ArrayList<>();

  public HeapCache(@NotNull HeapCacheConfig config) {
    this.config = config;
    // if full, then directly read everything from config.storage
    // we need to implement listeners and other mechanisms to always keep up with the storage.
  }

  public void addListener(@NotNull CacheChangeListener listener) {
    listeners.add(new WeakReference<>(listener));
  }

  public void removeListener(@NotNull CacheChangeListener listener) {
    listeners.remove(listener);
    // Remove the listener from the list
    // Return true if successful, false if not found
    // Implement the removal logic based on your specific needs
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
    // Add the cache entry
    // Trigger notification
    notifyEntryAdded(key, feature);
  }

  public void updateCacheEntry(String key, XyzFeature feature) {
    // Update the cache entry
    // Trigger notification
    notifyEntryUpdated(key, feature);
  }

  public void removeCacheEntry(String key) {
    // Remove the cache entry
    // Trigger notification
    notifyEntryRemoved(key);
  }

  protected final @NotNull HeapCacheConfig config;
  protected final @NotNull FibSet<String, CacheEntry> cache = new FibSet<>(CacheEntry::new);

  static void gc(@NotNull WeakReference<?> ref) {
    System.gc();
    while (ref.get() != null) {
      Thread.yield();
      System.gc();
    }
  }

  // For Cache Eviction
  /*public void setWeakReference(@NotNull Object){
  gc(new WeakReference<>(new Object()));
  }*/

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
}
