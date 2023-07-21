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
import com.here.naksha.lib.core.storage.IMasterTransaction;
import com.here.naksha.lib.core.storage.IReadTransaction;
import com.here.naksha.lib.core.storage.IStorage;
import com.here.naksha.lib.core.storage.ITransactionSettings;
import com.here.naksha.lib.core.util.fib.FibSet;
import org.jetbrains.annotations.NotNull;

public class HeapCache implements IStorage {

  public HeapCache(@NotNull HeapCacheConfig config) {
    this.config = config;
    // if full, then directly read everything from config.storage
    // we need to implement listeners and other mechanisms to always keep up with the storage.
  }

  protected final @NotNull HeapCacheConfig config;
  protected final @NotNull FibSet<String, CacheEntry> cache = new FibSet<>(CacheEntry::new);

  @Override
  public void maintain() {
    if (config.storage != null) {
      config.storage.maintain();
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
