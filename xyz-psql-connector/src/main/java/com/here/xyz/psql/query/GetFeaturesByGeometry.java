/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

package com.here.xyz.psql.query;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.models.geojson.coordinates.WKTHelper;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.SQLQuery;
import java.sql.SQLException;

public class GetFeaturesByGeometry extends Spatial<GetFeaturesByGeometryEvent> {

  public GetFeaturesByGeometry(GetFeaturesByGeometryEvent event, DatabaseHandler dbHandler) throws SQLException, ErrorResponseException {
    super(event, dbHandler);
  }

  @Override
  protected SQLQuery buildQuery(GetFeaturesByGeometryEvent event) throws SQLException {
    final int radius = event.getRadius();

    SQLQuery geoFilter = new SQLQuery(event.getH3Index() != null
        ? "(h3ToGeoBoundaryDeg(('x' || '" + event.getH3Index() + "')::bit(60)::bigint))"
        : "ST_GeomFromText('" + WKTHelper.geometryToWKB(event.getGeometry()) + "'" + (radius != 0 ? "" : ", 4326") + ")");

    if (radius != 0) {
      //Wrap the geoFilter with ST_Buffer to enlarge the input geometry
      geoFilter.setText("ST_Buffer(" + geoFilter.text() + "::geography, #{radius})::geometry");
      geoFilter.setNamedParameter("radius", radius);
    }

    SQLQuery geoQuery = new SQLQuery("ST_Intersects(geo, ${{geoFilter}})");
    geoQuery.setQueryFragment("geoFilter", geoFilter);

    SQLQuery query = super.buildQuery(event);

    //Override the geo fragment by a clipped version
    query.setQueryFragment("geo", buildClippedGeoFragment(event, geoFilter));

    SQLQuery filterWhereClause = new SQLQuery("${{indexedQuery}} AND ${{searchQuery}}");
    filterWhereClause.setQueryFragment("indexedQuery", geoQuery);
    //Use the existing clause as searchQuery (from the base query)
    filterWhereClause.setQueryFragment("searchQuery", query.getQueryFragment("filterWhereClause"));
    query.setQueryFragment("filterWhereClause", filterWhereClause);
    //TODO: For composite spaces also set the filterWhereClause in the sub fragments recursively! (Maybe not necessary if the filterWhereClause gets inherited from the parent queries - write unit tests for SQLQuery!)

    return query;
  }

  @Override
  protected SQLQuery buildClippedGeoFragment(final GetFeaturesByGeometryEvent event, SQLQuery geoFilter) {
    if (!event.getClip())
      return super.buildGeoFragment(event);

    SQLQuery clippedGeo = new SQLQuery("ST_Intersection(ST_MakeValid(geo), ${{geoFilter}})");
    clippedGeo.setQueryFragment("geoFilter", geoFilter);
    return super.buildGeoFragment(event, clippedGeo);
  }
}
