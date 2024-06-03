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

import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.storage.AbstractResultSet;
import java.util.List;
import java.util.NoSuchElementException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CacheResultSet<F extends XyzFeature> extends AbstractResultSet<F> {

  /**
   * Create a new result-set for the given feature-type.
   *
   * @param fClass the class of the feature-type.
   */
  protected CacheResultSet(@NotNull Class<F> fClass, @NotNull List<@NotNull F> features) {
    super(fClass);
    this.features = features;
  }

  final @NotNull List<@NotNull F> features;
  int i;

  @Override
  public boolean next() {
    return i < features.size();
  }

  @Override
  public @NotNull String getId() {
    if (i >= features.size()) throw new NoSuchElementException();
    return getFeature().getId();
  }

  @Override
  public @NotNull String getUuid() {
    if (i >= features.size()) throw new NoSuchElementException();
    final String uuid = getFeature().getProperties().getXyzNamespace().getUuid();
    assert uuid != null;
    return uuid;
  }

  @Override
  public @NotNull String getJson() {
    if (i >= features.size()) throw new NoSuchElementException();
    return jsonOf(getFeature());
  }

  @Override
  public @Nullable String getGeometry() {
    if (i >= features.size()) throw new NoSuchElementException();
    return geometryOf(getFeature());
  }

  @Override
  public @NotNull F getFeature() {
    if (i >= features.size()) throw new NoSuchElementException();
    return features.get(i);
  }
}
