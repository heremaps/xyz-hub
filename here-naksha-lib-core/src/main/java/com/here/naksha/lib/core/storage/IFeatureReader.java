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

import static com.here.naksha.lib.core.NakshaVersion.v2_0_5;

import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import java.util.List;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * API to read features from a collection.
 */
@AvailableSince(v2_0_5)
public interface IFeatureReader<FEATURE extends XyzFeature> {

  /**
   * Returns a single feature by its identifier.
   *
   * @param id The identifier.
   * @return The feature or null, if no such feature exists.
   */
  @AvailableSince(v2_0_5)
  @Nullable
  default FEATURE getFeatureById(@NotNull String id) {
    try (final IResultSet<FEATURE> rs = getFeaturesById(id)) {
      if (rs.next()) {
        return rs.getFeature();
      }
      return null;
    }
  }

  /**
   * Returns a result reader for all features with the given identifier from the HEAD collection. Beware that the returned result-set has no
   * specific order and may contain fewer features than requested.
   *
   * @param ids The identifiers of the features to read.
   * @return The result-reader.
   */
  @AvailableSince(v2_0_5)
  default @NotNull IResultSet<FEATURE> getFeaturesById(@NotNull List<@NotNull String> ids) {
    return getFeaturesById(ids.toArray(new String[0]));
  }

  /**
   * Returns a result reader for all features with the given identifier from the HEAD collection. Beware that the returned result-set has no
   * specific order and may contain fewer features than requested.
   *
   * @param ids The identifiers of the features to read.
   * @return The result-reader.
   */
  @AvailableSince(v2_0_5)
  @NotNull
  IResultSet<FEATURE> getFeaturesById(@NotNull String... ids);

  /**
   * Returns all features.
   *
   * @param skip  The amount of features to skip.
   * @param limit The maximal amount of feature to return.
   * @return The result-reader.
   */
  @NotNull
  IResultSet<FEATURE> getAll(int skip, int limit);
}
