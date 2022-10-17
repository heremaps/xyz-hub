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
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.SpatialQueryEvent;
import com.here.xyz.models.geojson.coordinates.WKTHelper;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.SQLQueryBuilder;
import java.sql.SQLException;

public class GetFeaturesByGeometry extends Spatial<GetFeaturesByGeometryEvent> {

  public GetFeaturesByGeometry(GetFeaturesByGeometryEvent event, DatabaseHandler dbHandler) throws SQLException, ErrorResponseException {
    super(event, dbHandler);
  }

  @Override
  protected SQLQuery buildQuery(GetFeaturesByGeometryEvent event) throws SQLException {
    //if (isExtendedSpace(event) && event.getContext() == DEFAULT)

    final int radius = event.getRadius();

    String geoFilter = event.getH3Index() != null
        ? "(h3ToGeoBoundaryDeg(('x' || '" + event.getH3Index() + "')::bit(60)::bigint))"
        : "ST_GeomFromText('" + WKTHelper.geometryToWKB(event.getGeometry()) + "'" + (radius != 0 ? "" : ", 4326") + ")";

    SQLQuery geoQuery = new SQLQuery(radius != 0
        ? "ST_Intersects(geo, ST_Buffer(${{geoFilter}}::geography, #{radius})::geometry)"
        : "ST_Intersects(geo, ${{geoFilter}})");
    geoQuery.setQueryFragment("geoFilter", geoFilter);
    if (radius != 0)
      geoQuery.setNamedParameter("radius", radius);

    return generateCombinedQuery(event, geoQuery);
  }

  //TODO: Can be removed after completion of refactoring
  @Deprecated
  public static SQLQuery generateCombinedQueryBWC(GetFeaturesByBBoxEvent event, SQLQuery indexedQuery) {
    indexedQuery.replaceUnnamedParameters();
    SQLQuery query = generateCombinedQuery(event, indexedQuery);
    if (query != null)
      query.replaceNamedParameters();
    return query;
  }

  private static SQLQuery generateCombinedQuery(SpatialQueryEvent event, SQLQuery indexedQuery) {
    final SQLQuery query = new SQLQuery(
        "SELECT ${{selection}}, ${{geo}}"
        + "    FROM ${schema}.${table} ${{tableSample}}"
        + "    WHERE ${{filterWhereClause}} ${{orderBy}} ${{limit}}"
    );

    query.setQueryFragment("selection", buildSelectionFragment(event));
    query.setQueryFragment("geo", buildClippedGeoFragment(event));
    query.setQueryFragment("tableSample", ""); //Can be overridden by caller

    SQLQuery filterWhereClause = new SQLQuery("${{indexedQuery}} AND ${{searchQuery}}");

    filterWhereClause.setQueryFragment("indexedQuery", indexedQuery);
    SQLQuery searchQuery = generateSearchQuery(event);
    if (searchQuery == null)
      filterWhereClause.setQueryFragment("searchQuery", "TRUE");
    else
      filterWhereClause.setQueryFragment("searchQuery", searchQuery);

    query.setQueryFragment("filterWhereClause", filterWhereClause);
    query.setQueryFragment("orderBy", ""); //Can be overridden by caller
    query.setQueryFragment("limit", buildLimitFragment(event.getLimit()));

    return query;
  }

  private static SQLQuery buildClippedGeoFragment(final GetFeaturesByGeometryEvent event) {
    //Clip=true =>  Use input Geometry for clipping
    final Geometry geometry = event.getGeometry();
    //If h3Index is given - use it as input geometry
    final String h3Index =  event.getH3Index();

    String hexBinGeo = "(h3ToGeoBoundaryDeg(('x' || '" + h3Index + "')::bit(60)::bigint))";

    String wktGeom = h3Index == null ? "ST_GeomFromText('" + WKTHelper.geometryToWKB(geometry) + "',4326)" : hexBinGeo ;

    //If radius is not null
    if(event.getRadius() != 0){
      //Enlarge input geometry with ST_Buffer
      wktGeom = h3Index == null ?
          "ST_Buffer(ST_GeomFromText('" + WKTHelper.geometryToWKB(geometry) + "')::geography, #{radius} )::geometry"
          : "ST_Buffer(" + hexBinGeo + "::geography, #{radius})::geometry";
    }
    return new SQLQuery("replace(ST_AsGeoJson(ST_Intersection(ST_MakeValid(geo)," + wktGeom + "), "
        + SQLQueryBuilder.GEOMETRY_DECIMAL_DIGITS + "),'nan','0') as geo");
  }

  /**
   * Returns the geo-query, which will return the clipped geometry objects
   * depending on whether clipping is active or not.
   */
  protected static SQLQuery buildClippedGeoFragment(final SpatialQueryEvent event) {
    if (!event.getClip())
      return Spatial.buildClippedGeoFragment(event);

    if (event instanceof GetFeaturesByBBoxEvent)
      return GetFeaturesByBBox.buildClippedGeoFragment((GetFeaturesByBBoxEvent) event);
    else if (event instanceof GetFeaturesByGeometryEvent)
      return buildClippedGeoFragment((GetFeaturesByGeometryEvent) event);

    return null; //Should never happen
  }
}
