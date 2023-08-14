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

import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.storage.ClosableIterator;
import com.here.naksha.lib.core.storage.CollectionInfo;
import com.here.naksha.lib.core.storage.IFeatureReader;
import com.here.naksha.lib.core.storage.IReadTransaction;
import com.here.naksha.lib.core.storage.ITransactionSettings;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class HeapReadTx implements IReadTransaction {

  HeapReadTx(@NotNull HeapCache cache) {
    this.cache = cache;
  }

  final @NotNull HeapCache cache;

  @Override
  public @NotNull String transactionNumber() {
    return null;
  }

  @Override
  public @NotNull ITransactionSettings settings() {
    // Implement transaction settings logic
    return cache.createSettings(); // Use cache's createSettings()
  }

  @Override
  public void close() {}

  @Override
  public @NotNull ClosableIterator<@NotNull CollectionInfo> iterateCollections() {
    // Implement collection iteration logic
    return new ClosableIterator<@NotNull CollectionInfo>() {
      @Override
      public boolean hasNext() {
        // Implement hasNext logic
        return false; // Replace with actual logic
      }

      @Override
      public @NotNull CollectionInfo next() {
        // Implement next logic
        return new CollectionInfo("example", 1L); // Replace with actual logic
      }

      @Override
      public void close() {
        // Implement resource closing logic here
        // For example, close any open resources or connections
        // You can also release any references or perform cleanup
        // This method will be called when the transaction is finished
        // Don't forget to close any open resources to prevent leaks
      }
    };
  }

  @Override
  public @Nullable CollectionInfo getCollectionById(@NotNull String id) {
    return null;
  }

  @Override
  public @NotNull <FEATURE extends XyzFeature> IFeatureReader<FEATURE> readFeatures(
      @NotNull Class<FEATURE> featureClass, @NotNull CollectionInfo collection) {
    return new HeapFeatureReader<>(cache, featureClass, collection);
  }
}
