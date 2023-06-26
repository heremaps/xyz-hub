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
package com.here.naksha.lib.psql;

import com.here.naksha.lib.core.models.features.StorageCollection;
import com.here.naksha.lib.core.models.geojson.implementation.Feature;
import com.here.naksha.lib.core.storage.IFeatureReader;
import java.sql.SQLException;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class PsqlFeatureReader<FEATURE extends Feature> implements IFeatureReader<FEATURE> {

  PsqlFeatureReader(
      @NotNull PsqlTxReader storageReader,
      @NotNull Class<FEATURE> featureClass,
      @NotNull StorageCollection collection) {
    this.storageReader = storageReader;
    this.featureClass = featureClass;
    this.collection = collection;
  }

  final @NotNull PsqlTxReader storageReader;
  final @NotNull Class<FEATURE> featureClass;
  final @NotNull StorageCollection collection;

  @Override
  public @Nullable FEATURE getFeatureById(@NotNull String id) {
    throw new UnsupportedOperationException("getFeatureById");
  }

  @Override
  public @NotNull List<@NotNull FEATURE> getFeaturesById(@NotNull List<@NotNull String> ids) throws SQLException {
    throw new UnsupportedOperationException("getFeaturesById");
  }
}
