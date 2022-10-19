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

    SQLQuery geoFilter = event.getH3Index() != null
        ? new SQLQuery("(h3ToGeoBoundaryDeg(('x' || #{h3Index})::bit(60)::bigint))").withNamedParameter("h3Index", event.getH3Index())
        : new SQLQuery("ST_GeomFromText(#{wkbGeometry}" + (radius != 0 ? "" : ", 4326") + ")")
            .withNamedParameter("wkbGeometry", WKTHelper.geometryToWKB(event.getGeometry()));

    if (radius != 0)
      //Wrap the geoFilter with ST_Buffer to enlarge the input geometry
      geoFilter = new SQLQuery("ST_Buffer(${{wrappedGeoFilter}}::geography, #{radius})::geometry")
          .withQueryFragment("wrappedGeoFilter", geoFilter)
          .withNamedParameter("radius", radius);

    SQLQuery query = super.buildQuery(event);
    SQLQuery geoQuery = new SQLQuery("ST_Intersects(geo, ${{geoFilter}})").withQueryFragment("geoFilter", geoFilter);

    SQLQuery filterWhereClause = new SQLQuery("${{geoQuery}} AND ${{searchQuery}}")
        .withQueryFragment("geoQuery", geoQuery)
        //Use the existing clause as searchQuery (from the base query)
        .withQueryFragment("searchQuery", query.getQueryFragment("filterWhereClause"));

    query
        .withQueryFragment("filterWhereClause", filterWhereClause)
        //Override the geo fragment by a clipped version
        .withQueryFragment("geo", buildClippedGeoFragment(event, geoFilter));

    return query;
  }

  @Override
  protected SQLQuery buildClippedGeoFragment(final GetFeaturesByGeometryEvent event, SQLQuery geoFilter) {
    if (!event.getClip())
      return super.buildGeoFragment(event);

    SQLQuery clippedGeo = new SQLQuery("ST_Intersection(ST_MakeValid(geo), ${{geoFilter}})")
        .withQueryFragment("geoFilter", geoFilter);

    return super.buildGeoFragment(event, clippedGeo);
  }
}
