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
package com.here.naksha.lib.psql.statement;

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;

import com.fasterxml.jackson.databind.ObjectReader;
import com.here.naksha.lib.core.models.geojson.coordinates.JTSHelper;
import com.here.naksha.lib.core.models.geojson.implementation.XyzGeometry;
import com.here.naksha.lib.core.models.naksha.NakshaFeature;
import com.here.naksha.lib.core.models.storage.ExecutedOp;
import com.here.naksha.lib.core.models.storage.WriteFeatures;
import com.here.naksha.lib.core.models.storage.WriteOpResult;
import com.here.naksha.lib.core.models.storage.WriteResult;
import com.here.naksha.lib.core.util.json.Json;
import com.here.naksha.lib.psql.mapper.FeatureWriteInputParameters;
import com.here.naksha.lib.psql.mapper.PsqlFeatureWriterMapper;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public class PsqlFeatureWriter extends StatementCreator {

  public PsqlFeatureWriter(@NotNull Connection connection, @NotNull Duration timeout) {
    super(connection, timeout);
  }

  public @NotNull WriteResult writeFeatures(@NotNull WriteFeatures writeFeatures) {
    try (final PreparedStatement stmt = preparedStatement("SELECT naksha_write_features(?,?::jsonb,?,?);")) {
      PsqlFeatureWriterMapper parametersMapper = new PsqlFeatureWriterMapper();
      FeatureWriteInputParameters psqlParameters =
          parametersMapper.prepareFeatureWriteInputParameters(writeFeatures.queries);
      stmt.setString(1, writeFeatures.collectionId);
      stmt.setObject(
          2, createArrayOf("jsonb", psqlParameters.getOperationsJson().toArray()));
      stmt.setObject(
          3, createArrayOf("bytea", psqlParameters.getGeometries().toArray()));
      stmt.setObject(4, writeFeatures.noResults);
      final ResultSet rs = stmt.executeQuery();
      List<WriteOpResult<NakshaFeature>> writeOps = toFeatureWriteOps(rs);
      return new WriteResult(writeOps);
    } catch (final Throwable t) {
      throw unchecked(t);
    }
  }

  private List<WriteOpResult<NakshaFeature>> toFeatureWriteOps(ResultSet rs) throws SQLException {
    List<WriteOpResult<NakshaFeature>> operations = new LinkedList<>();
    while (rs.next()) {
      String featureJson = rs.getString("r_feature");
      String operation = rs.getString("r_op");
      byte[] geometry = rs.getBytes("r_geo");
      try (final Json json = Json.get()) {
        ObjectReader reader = json.reader();
        NakshaFeature feature = reader.readValue(featureJson, NakshaFeature.class);
        XyzGeometry xyzGeometry = JTSHelper.fromGeometry(json.wkbReader.read(geometry));
        feature.setGeometry(xyzGeometry);

        operations.add(new WriteOpResult<>(ExecutedOp.valueOf(operation), feature));
      } catch (final Throwable t) {
        throw unchecked(t);
      }
    }
    return operations;
  }
}
