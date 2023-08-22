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
import static com.here.naksha.lib.core.util.json.Json.toJsonString;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.here.naksha.lib.core.models.geojson.coordinates.JTSHelper;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzGeometry;
import com.here.naksha.lib.core.storage.CollectionInfo;
import com.here.naksha.lib.core.storage.DeleteOp;
import com.here.naksha.lib.core.storage.IFeatureWriter;
import com.here.naksha.lib.core.storage.ModifyFeaturesReq;
import com.here.naksha.lib.core.storage.ModifyFeaturesResp;
import com.here.naksha.lib.core.util.ILike;
import com.here.naksha.lib.core.util.json.Json;
import com.here.naksha.lib.core.view.ViewDeserialize;
import com.here.naksha.lib.core.view.ViewSerialize;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.postgresql.util.PGobject;

public class PsqlFeatureWriter<FEATURE extends XyzFeature> extends PsqlFeatureReader<FEATURE, PsqlTxWriter>
    implements IFeatureWriter<FEATURE> {

  PsqlFeatureWriter(
      @NotNull PsqlTxWriter txWriter, @NotNull Class<FEATURE> featureClass, @NotNull CollectionInfo collection) {
    super(txWriter, featureClass, collection);
  }

  private static void assure3d(@NotNull Coordinate @NotNull [] coords) {
    for (final @NotNull Coordinate coord : coords) {
      if (coord.z != coord.z) { // if coord.z is NaN
        coord.z = 0;
      }
    }
  }

  public static byte[][] geometryListTo2DimByteArray(@NotNull List<Geometry> geometries) {
    final byte[][] twoDArray = new byte[geometries.size()][];
    final WKBWriter wkbWriter = new WKBWriter(3);
    int idx = 0;
    for (final Geometry geo : geometries) {
      twoDArray[idx++] = (geo == null) ? null : wkbWriter.write(geo);
    }
    return twoDArray;
  }

  private void addFeatures(
      final @NotNull ObjectWriter featureWriter,
      final @NotNull NakshaOp op,
      final List<@NotNull FEATURE> source,
      final @NotNull ArrayList<String> features,
      final @NotNull ArrayList<Geometry> geometries,
      final @NotNull ArrayList<String> expected_uuids,
      final @NotNull ArrayList<String> ops) {
    for (final FEATURE feature : source) {
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
        // TODO: Fix this !!!
        // final String serialized = featureWriter.writeValueAsString(feature);
        final String serialized = feature.serialize();
        features.add(serialized);
        geometries.add(jtsGeometry);
        expected_uuids.add(
            op == NakshaOp.INSERT
                ? null
                : feature.getProperties().getXyzNamespace().getUuid());
        ops.add(op.toString());
      } catch (final Throwable t) {
        throw unchecked(t);
      } finally {
        feature.setGeometry(nkGeometry);
      }
    }
  }

  @Override
  public @NotNull ModifyFeaturesResp modifyFeatures(@NotNull ModifyFeaturesReq<FEATURE> req) {
    try (final Json json = Json.get()) {
      final ObjectWriter writer = json.writer(ViewSerialize.Storage.class);
      final ObjectWriter featureWriter = writer.forType(XyzFeature.class);
      final WKBReader wkbReader = new WKBReader(new GeometryFactory(new PrecisionModel(), 4326));

      final int size = req.totalSize();
      final ArrayList<String> featuresJson = new ArrayList<>(size);
      final ArrayList<Geometry> geometries = new ArrayList<>(size);
      final ArrayList<String> expected_uuids = new ArrayList<>(size);
      final ArrayList<String> ops = new ArrayList<>(size);
      addFeatures(featureWriter, NakshaOp.INSERT, req.insert(), featuresJson, geometries, expected_uuids, ops);
      addFeatures(featureWriter, NakshaOp.UPDATE, req.update(), featuresJson, geometries, expected_uuids, ops);
      addFeatures(featureWriter, NakshaOp.UPSERT, req.upsert(), featuresJson, geometries, expected_uuids, ops);
      final StringBuilder sb = new StringBuilder();
      final List<@NotNull DeleteOp> delete = req.delete();
      for (final DeleteOp deleteOp : delete) {
        sb.setLength(0);
        sb.append("{\"id\":");
        toJsonString(deleteOp.id(), sb);
        sb.append("}");
        featuresJson.add(sb.toString());
        geometries.add(null);
        expected_uuids.add(deleteOp.uuid());
        ops.add(NakshaOp.DELETE.toString());
      }
      final ModifyFeaturesResp response = new ModifyFeaturesResp();
      try (final PreparedStatement stmt =
          tx.conn().prepareStatement("SELECT * FROM naksha_modify_features(?,?,?,?,?,?);")) {
        stmt.setString(1, collection.getId());
        stmt.setArray(2, tx.conn().createArrayOf("jsonb", featuresJson.toArray()));
        stmt.setArray(3, tx.conn().createArrayOf("bytea", geometryListTo2DimByteArray(geometries)));
        stmt.setArray(4, tx.conn().createArrayOf("text", expected_uuids.toArray()));
        stmt.setArray(5, tx.conn().createArrayOf("naksha_op", ops.toArray()));
        stmt.setBoolean(6, req.read_results());
        try (final ResultSet rs = stmt.executeQuery()) {
          if (req.read_results()) {
            int i = -1;
            final ObjectReader reader = json.reader(ViewDeserialize.Storage.class);
            while (rs.next()) {
              i++;
              final String op = rs.getString(1);
              final PGobject pgFeature = (PGobject) rs.getObject(2);
              assert pgFeature != null && pgFeature.getValue() != null;
              final XyzFeature feature = reader.readValue(pgFeature.getValue(), XyzFeature.class);
              if (ILike.equals(op, NakshaOp.INSERT)) {
                final Geometry geometry = geometries.get(i);
                if (geometry != null) {
                  feature.setGeometry(JTSHelper.fromGeometry(geometry));
                }
                response.inserted().add(feature);
              } else if (ILike.equals(op, NakshaOp.UPDATE)) {
                final Geometry geometry = geometries.get(i);
                if (geometry != null) {
                  feature.setGeometry(JTSHelper.fromGeometry(geometry));
                }
                response.updated().add(feature);
              } else if (ILike.equals(op, NakshaOp.DELETE)) {
                final byte[] bytes = rs.getBytes(3);
                if (bytes != null) {
                  final Geometry geometry = wkbReader.read(bytes);
                  feature.setGeometry(JTSHelper.fromGeometry(geometry));
                }
                response.deleted().add(feature);
              } else {
                throw new SQLException(
                    "Unexpected operation returned by naksha_modify_features at index " + i + ": "
                        + op);
              }
            }
          }
          return response;
        } catch (final Throwable t) {
          throw unchecked(t);
        }
      } catch (final Throwable t) {
        throw unchecked(t);
      }
    }
  }
}
