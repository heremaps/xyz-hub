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
import com.here.naksha.lib.core.storage.CollectionInfo;
import com.here.naksha.lib.core.storage.IFeatureReader;
import com.here.naksha.lib.core.storage.IResultSet;
import java.util.ArrayList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class HeapFeatureReader<F extends XyzFeature> implements IFeatureReader<F> {

  HeapFeatureReader(@NotNull HeapCache cache, @NotNull Class<F> featureClass, @NotNull CollectionInfo collection) {
    this.cache = cache;
    this.featureClass = featureClass;
    this.collection = collection;
  }

  final @NotNull HeapCache cache;
  final @NotNull Class<F> featureClass;
  final @NotNull CollectionInfo collection;

  @Override
  public @NotNull IResultSet<F> getFeaturesById(@NotNull String... ids) {
    final ArrayList<F> features = new ArrayList<>();
    for (final String id : ids) {
      final CacheEntry entry = cache.cache.get(id);
      if (entry != null && featureClass.isInstance(entry.getValue())) {
        features.add(featureClass.cast(entry.getValue()));
      }
    }
    return new CacheResultSet<>(featureClass, features);
  }

  @Override
  public @Nullable F getFeatureById(@NotNull String id) {
    final CacheEntry entry = cache.cache.get(id);
    F feature = null;
    if (entry != null && featureClass.isInstance(entry.getValue())) {
      feature = featureClass.cast(entry.getValue());
    }
    return feature;
  }

  @Override
  public @NotNull IResultSet<F> getAll(int skip, int limit) {
    final ArrayList<F> features = new ArrayList<>();
    final ArrayList<CacheEntry> entries = new ArrayList<>(cache.cache.getAll());
    for (final CacheEntry entry : entries) {
      if (entry != null && featureClass.isInstance(entry.getValue())) {
        features.add(featureClass.cast(entry.getValue()));
      }
    }
    return new CacheResultSet<>(featureClass, features);
  }
}
