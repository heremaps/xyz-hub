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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzGeometry;
import com.here.naksha.lib.core.storage.CollectionInfo;
import com.here.naksha.lib.core.storage.IFeatureWriter;
import com.here.naksha.lib.core.storage.ModifyFeaturesReq;
import com.here.naksha.lib.core.storage.ModifyFeaturesResp;
import com.here.naksha.lib.core.util.json.Json;
import com.here.naksha.lib.core.view.ViewSerialize.Storage;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public class PsqlFeatureWriter<FEATURE extends XyzFeature> extends PsqlFeatureReader<FEATURE, PsqlTxWriter>
    implements IFeatureWriter<FEATURE> {

  PsqlFeatureWriter(
      @NotNull PsqlTxWriter txWriter, @NotNull Class<FEATURE> featureClass, @NotNull CollectionInfo collection) {
    super(txWriter, featureClass, collection);
  }

  @Override
  public @NotNull ModifyFeaturesResp<FEATURE> modifyFeatures(@NotNull ModifyFeaturesReq<FEATURE> req)
      throws SQLException {
    try (final Json json = Json.open()) {
      final ObjectWriter featureWriter = json.writer(Storage.class).forType(XyzFeature.class);
      final ArrayList<String> features = new ArrayList<>();
      final ArrayList<Geometry> geometries = new ArrayList<>();
      final ArrayList<String> expected_uuids = new ArrayList<>();
      final ArrayList<String> ops = new ArrayList<>();
      final List<@NotNull FEATURE> inserts = req.insert();
      for (final @NotNull FEATURE feature : inserts) {
        final XyzGeometry nkGeometry = feature.getGeometry();
        feature.setGeometry(null);
        try {
          final Geometry jtsGeometry;
          if (nkGeometry != null) {
            jtsGeometry = nkGeometry.getJTSGeometry();
            assure3d(jtsGeometry.getCoordinates());
          } else {
            jtsGeometry = null;
          }
          features.add(featureWriter.writeValueAsString(feature));
          geometries.add(jtsGeometry);
          expected_uuids.add(null);
          ops.add("INSERT");
        } catch (JsonProcessingException e) {
          // TODO: Fix me!!!!
          e.printStackTrace();
        } finally {
          feature.setGeometry(nkGeometry);
        }
      }
      try (final PreparedStatement stmt =
          tx.conn().prepareStatement("SELECT * FROM naksha_modify_features(?,?,?,?,?);")) {
        stmt.setString(1, collection.getId());
        stmt.setArray(2, tx.conn().createArrayOf("jsonb", features.toArray()));
        stmt.setArray(3, tx.conn().createArrayOf("geometry", geometries.toArray()));
        stmt.setArray(4, tx.conn().createArrayOf("text", expected_uuids.toArray()));
        stmt.setArray(5, tx.conn().createArrayOf("naksha_op", ops.toArray()));
        final ResultSet rs = stmt.executeQuery();
      }
    }
    return null;
  }

  private static void assure3d(@NotNull Coordinate @NotNull [] coords) {
    for (final @NotNull Coordinate coord : coords) {
      if (coord.z != coord.z) { // if coord.z is NaN
        coord.z = 0;
      }
    }
  }
}
