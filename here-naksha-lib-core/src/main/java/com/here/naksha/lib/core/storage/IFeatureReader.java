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

import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * API to read features from a collection.
 */
public interface IFeatureReader<FEATURE extends XyzFeature> {

  /**
   * Returns a single feature by its identifier.
   *
   * @param id the identifier.
   * @return the feature or null, if no such feature exists.
   */
  @Nullable
  FEATURE getFeatureById(@NotNull String id);

  /**
   * Returns a list of features with the given identifier from the HEAD collection.
   *
   * @param ids the identifiers of the features to read.
   * @return the list of read features, the order is insignificant. Features that where not found, are simply not part of the result-set.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  @NotNull
  List<@NotNull FEATURE> getFeaturesById(@NotNull List<@NotNull String> ids) throws Exception;
}
