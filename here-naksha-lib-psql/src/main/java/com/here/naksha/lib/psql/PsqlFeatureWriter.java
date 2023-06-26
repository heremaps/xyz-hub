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

import com.here.naksha.lib.core.models.geojson.implementation.Feature;
import com.here.naksha.lib.core.storage.CollectionInfo;
import com.here.naksha.lib.core.storage.IFeatureWriter;
import com.here.naksha.lib.core.storage.ModifyFeaturesReq;
import com.here.naksha.lib.core.storage.ModifyFeaturesResp;
import java.sql.SQLException;
import org.jetbrains.annotations.NotNull;

public class PsqlFeatureWriter<FEATURE extends Feature> extends PsqlFeatureReader<FEATURE>
    implements IFeatureWriter<FEATURE> {

  PsqlFeatureWriter(
      @NotNull PsqlTxWriter storageWriter,
      @NotNull Class<FEATURE> featureClass,
      @NotNull CollectionInfo collection) {
    super(storageWriter, featureClass, collection);
    this.storageWriter = storageWriter;
    assert this.storageReader == this.storageWriter;
  }

  final @NotNull PsqlTxWriter storageWriter;

  @Override
  public @NotNull ModifyFeaturesResp<FEATURE> modifyFeatures(@NotNull ModifyFeaturesReq<FEATURE> req)
      throws SQLException {
    throw new UnsupportedOperationException("modifyFeatures");
  }
}
