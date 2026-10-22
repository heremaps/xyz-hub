/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.models.geojson.implementation.GeometryCollection;
import com.here.xyz.models.geojson.implementation.MultiLineString;
import com.here.xyz.models.geojson.implementation.MultiPoint;
import com.here.xyz.models.geojson.implementation.MultiPolygon;
import com.here.xyz.util.db.SQLQuery;
import java.sql.SQLException;

public class GetFeaturesByGeometry extends Spatial<GetFeaturesByGeometryEvent, FeatureCollection> {

  public GetFeaturesByGeometry(GetFeaturesByGeometryEvent event) throws SQLException, ErrorResponseException {
    super(event);
  }

  @Override
  protected SQLQuery buildGeoFilter(GetFeaturesByGeometryEvent event) {
    if (event.getGeometry() == null && event.getH3Index() == null)
      return null;

    final int radius = event.getRadius();

    SQLQuery geoFilter = event.getH3Index() != null
            ? new SQLQuery("(h3ToGeoBoundaryDeg(('x' || #{h3Index})::bit(60)::bigint))").withNamedParameter("h3Index", event.getH3Index())
            : new SQLQuery("st_force3d(ST_GeomFromText(#{wktGeometry}" + (radius != 0 ? "" : ", 4326") + "))")
            .withNamedParameter("wktGeometry", WKTHelper.geometryToWKT2d(event.getGeometry()));

    if (radius != 0)
      //Wrap the geoFilter with ST_Buffer to enlarge the input geometry
      geoFilter = new SQLQuery("ST_Buffer(${{wrappedGeoFilter}}::geography, #{radius})::geometry")
              .withQueryFragment("wrappedGeoFilter", geoFilter)
              .withNamedParameter("radius", radius);
    return geoFilter;
  }

  @Override
  protected SQLQuery buildGeoPredicate(GetFeaturesByGeometryEvent event, SQLQuery geoFilter) {
    if (!isMultiPartFilter(event))
      return super.buildGeoPredicate(event, geoFilter);

    //The bounding box of a multi-part geometry with parts spread over a large area covers much more than the parts.
    //Probe the index once per part instead of once with that bounding box. The exact test stays on the whole geoFilter.
    //_ST_Intersects is the non-indexed variant of ST_Intersects, so the planner can't fall back to the whole-bbox probe.
    //(SELECT ...) evaluates the geoFilter once per (sub-)query instead of once per candidate row, also in generic plans.
    return new SQLQuery("""
        (ST_isvalid(${{geoFilter}})
          AND geo && ANY(ARRAY(SELECT (ST_Dump(${{geoFilter}})).geom))
          AND _ST_Intersects(geo, (SELECT ${{geoFilter}})))""")
        .withQueryFragment("geoFilter", geoFilter);
  }

  static boolean isMultiPartFilter(GetFeaturesByGeometryEvent event) {
    //h3Index takes precedence over the geometry, see buildGeoFilter()
    if (event.getH3Index() != null)
      return false;
    Geometry geometry = event.getGeometry();
    return geometry instanceof MultiPoint || geometry instanceof MultiLineString || geometry instanceof MultiPolygon
        || geometry instanceof GeometryCollection;
  }
}
