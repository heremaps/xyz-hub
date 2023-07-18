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

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;

import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.storage.CollectionInfo;
import com.here.naksha.lib.core.storage.IFeatureReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.jetbrains.annotations.NotNull;

public class PsqlFeatureReader<FEATURE extends XyzFeature, TX extends PsqlTxReader> implements IFeatureReader<FEATURE> {

  PsqlFeatureReader(@NotNull TX tx, @NotNull Class<FEATURE> featureClass, @NotNull CollectionInfo collection) {
    this.tx = tx;
    this.featureClass = featureClass;
    this.collection = collection;
  }

  final @NotNull TX tx;
  final @NotNull Class<FEATURE> featureClass;
  final @NotNull CollectionInfo collection;

  @Override
  public @NotNull PsqlResultSet<FEATURE> getFeaturesById(@NotNull String... ids) {
    try {
      final StringBuilder sb = new StringBuilder();
      sb.append(
          "SELECT jsondata->>'id', jsondata->'properties'->'@ns:com:here:xyz'->>'uuid', jsondata::jsonb, geo::geometry FROM ");
      PsqlStorage.escapeId(sb, collection.getId());
      sb.append(" WHERE jsondata->>'id' = ANY(?)");
      final String SQL = sb.toString();
      final PreparedStatement stmt = tx.preparedStatement(SQL);
      stmt.setArray(1, tx.conn().createArrayOf("text", ids));
      final ResultSet rs = stmt.executeQuery();
      return new PsqlResultSet<>(stmt, rs, featureClass);
    } catch (final Throwable t) {
      throw unchecked(t);
    }
  }
}
