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

package com.here.xyz.psql.query.helpers;

import static com.here.xyz.psql.query.GetFeaturesByBBox.GEOMETRY_DECIMAL_DIGITS;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.Event;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.psql.QueryRunner;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.query.XyzEventBasedQueryRunner;
import com.here.xyz.psql.query.helpers.FetchExistingFeatures.FetchExistingFeaturesInput;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class FetchExistingFeatures extends QueryRunner<FetchExistingFeaturesInput, List<Feature>> {

  public FetchExistingFeatures(FetchExistingFeaturesInput input) throws SQLException, ErrorResponseException {
    super(input);
  }

  @Override
  protected SQLQuery buildQuery(FetchExistingFeaturesInput input) throws SQLException, ErrorResponseException {
    return new SQLQuery("SELECT jsondata, replace(ST_AsGeojson(ST_Force3D(geo), " + GEOMETRY_DECIMAL_DIGITS + "), 'nan', '0') as geo "
        + "FROM ${schema}.${table} WHERE id = ANY(#{ids})")
        .withVariable(SCHEMA, getSchema())
        .withVariable(TABLE, XyzEventBasedQueryRunner.readTableFromEvent(input.event))
        .withNamedParameter("ids", input.ids.toArray(new String[0]));
  }

  @Override
  public List<Feature> handle(ResultSet rs) throws SQLException {
    List<Feature> existingFeatures = null;
    FeatureCollection fc = dbHandler.defaultFeatureResultSetHandler(rs);

    if (fc != null) {
      try {
        existingFeatures = fc.getFeatures();
      }
      catch (JsonProcessingException e) {
        throw new SQLException("Error parsing features");
      }
    }

    return existingFeatures;
  }

  public static class FetchExistingFeaturesInput {
    private Event event;

    private List<String> ids;

    public FetchExistingFeaturesInput(Event event, List<String> ids) {
      this.event = event;
      this.ids = ids;
    }
  }
}