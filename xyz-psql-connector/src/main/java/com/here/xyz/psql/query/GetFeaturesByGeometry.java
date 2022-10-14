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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.events.GetFeaturesByTileEvent.ResponseType.GEO_JSON;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.SpatialQueryEvent;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.coordinates.WKTHelper;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.SQLQueryBuilder;
import com.here.xyz.psql.tools.DhString;
import java.sql.SQLException;

public class GetFeaturesByGeometry extends SearchForFeatures<GetFeaturesByGeometryEvent> {

  public GetFeaturesByGeometry(GetFeaturesByGeometryEvent event, DatabaseHandler dbHandler) throws SQLException, ErrorResponseException {
    super(event, dbHandler);
  }

  @Override
  protected SQLQuery buildQuery(GetFeaturesByGeometryEvent event) throws SQLException {
    //if (isExtendedSpace(event) && event.getContext() == DEFAULT)

    final int radius = event.getRadius();
    final Geometry geometry = event.getGeometry();

    final SQLQuery geoQuery;

    if(event.getH3Index() != null){
      if(radius != 0)
        geoQuery = new SQLQuery("ST_Intersects( geo, ST_Buffer(hexbin::geography,? )::geometry)", radius);
      else
        geoQuery = new SQLQuery("ST_Intersects( geo, hexbin)");
    }else{
      geoQuery = radius != 0 ? new SQLQuery("ST_Intersects(geo, ST_Buffer(ST_GeomFromText('"
          + WKTHelper.geometryToWKB(geometry) + "')::geography, ? )::geometry)", radius) : new SQLQuery("ST_Intersects(geo, ST_GeomFromText('"
          + WKTHelper.geometryToWKB(geometry) + "',4326))");
    }
    return generateCombinedQuery(event, geoQuery);
  }

  //TODO: Can be removed after completion of refactoring
  @Deprecated
  public static SQLQuery generateCombinedQueryBWC(SpatialQueryEvent event, SQLQuery indexedQuery) {
    SQLQuery query = generateCombinedQuery(event, indexedQuery);
    if (query != null)
      query.replaceNamedParameters();
    return query;
  }

  private static SQLQuery generateCombinedQuery(SpatialQueryEvent event, SQLQuery indexedQuery) {
    SQLQuery searchQuery = generateSearchQueryBWC(event);
    boolean bConvertGeo2Geojson = event instanceof GetFeaturesByGeometryEvent || SQLQueryBuilder.getResponseType((GetFeaturesByBBoxEvent) event) == GEO_JSON;
    String h3Index = event instanceof GetFeaturesByGeometryEvent ? ((GetFeaturesByGeometryEvent) event).getH3Index() : null;

      final SQLQuery query = new SQLQuery();

      if(h3Index != null)
          query.append("WITH h AS (SELECT h3ToGeoBoundaryDeg( ('x' || '"+h3Index+"' )::bit(60)::bigint ) as hexbin )");

      query.append("SELECT ");

      query.append(SQLQuery.selectJson(event));
      query.append(",");
      query.append(" ${{geo}} ");
      query.setQueryFragment("geo", geometrySelectorForEvent(event, bConvertGeo2Geojson));

      if(h3Index != null)
          query.append("FROM ${schema}.${table} ${{tableSample}}, h WHERE");
      else
          query.append("FROM ${schema}.${table} ${{tableSample}} WHERE");
      query.setQueryFragment("tableSample", ""); //Can be overridden by caller

      query.append(indexedQuery);

      if( searchQuery != null )
      { query.append(" and ");
          query.append(searchQuery);
      }

      query.append(" ${{orderBy}} ");
      query.setQueryFragment("orderBy", ""); //Can be overridden by caller

      query.append("LIMIT ?", event.getLimit());
      return query;
  }

  /**
   * Returns the query, which will contains the geometry object.
   */
  private static SQLQuery geometrySelectorForEvent(final SpatialQueryEvent event, boolean bGeoJson) {
      String forceMode = SQLQueryBuilder.getForceMode(event.isForce2D());

      if(!event.getClip()){
          return (bGeoJson ?
                  new SQLQuery("replace(ST_AsGeojson("+forceMode+"(geo),?::INTEGER),'nan','0') as geo", SQLQueryBuilder.GEOMETRY_DECIMAL_DIGITS)
                  : new SQLQuery(forceMode+"(geo) as geo"));
      }else{
          if(event instanceof GetFeaturesByBBoxEvent){
              final BBox bbox = ((GetFeaturesByBBoxEvent)event).getBbox();
              String geoSqlAttrib = (bGeoJson ? DhString.format("replace(ST_AsGeoJson(ST_Intersection(ST_MakeValid(geo),ST_MakeEnvelope(?,?,?,?,4326)),%d),'nan','0') as geo", SQLQueryBuilder.GEOMETRY_DECIMAL_DIGITS)
                      : "ST_Intersection( ST_MakeValid(geo),ST_MakeEnvelope(?,?,?,?,4326) ) as geo");

              return new SQLQuery(geoSqlAttrib, bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat());
          }else if(event instanceof GetFeaturesByGeometryEvent){
              //Clip=true =>  Use input Geometry for clipping
              final Geometry geometry = ((GetFeaturesByGeometryEvent)event).getGeometry();
              //If h3Index is given - use it as input geometry
              final String h3Index =  ((GetFeaturesByGeometryEvent)event).getH3Index();

              String wktGeom = h3Index == null ? "ST_GeomFromText('" + WKTHelper.geometryToWKB(geometry) + "',4326)" : "hexbin" ;

              //If radius is not null
              if(((GetFeaturesByGeometryEvent)event).getRadius() != 0){
                  //Enlarge input geometry with ST_Buffer
                  wktGeom = (h3Index == null ?
                          DhString.format("ST_Buffer(ST_GeomFromText('" + WKTHelper.geometryToWKB(geometry) + "')::geography, %d )::geometry", ((GetFeaturesByGeometryEvent)event).getRadius())
                          : DhString.format("ST_Buffer(hexbin::geography,%d)::geometry", ((GetFeaturesByGeometryEvent)event).getRadius()));
              }
              return new SQLQuery("replace(ST_AsGeoJson(ST_Intersection(ST_MakeValid(geo),"+wktGeom+"),?::INTEGER),'nan','0') as geo", SQLQueryBuilder.GEOMETRY_DECIMAL_DIGITS);
          }
      }
      //Should not happen (currently only used with GetFeaturesByBBoxEvent / GetFeaturesByGeometryEvent)
      return new SQLQuery("ST_AsGeojson(geo) as geo");
  }
}
