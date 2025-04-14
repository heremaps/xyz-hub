/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

import static com.here.xyz.events.GetFeaturesByTileEvent.ResponseType.GEO_JSON;
import static com.here.xyz.events.GetFeaturesByTileEvent.ResponseType.MVT;
import static com.here.xyz.events.GetFeaturesByTileEvent.ResponseType.MVT_FLATTENED;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.GetFeaturesByTileEvent.ResponseType;
import com.here.xyz.models.geojson.HQuad;
import com.here.xyz.models.geojson.WebMercatorTile;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.responses.BinaryResponse;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.db.SQLQuery;
import java.sql.ResultSet;
import java.sql.SQLException;

public class GetFeaturesByBBox<E extends GetFeaturesByBBoxEvent, R extends XyzResponse> extends Spatial<E, R> {

  private static final String APPLICATION_VND_MAPBOX_VECTOR_TILE = "application/vnd.mapbox-vector-tile";
  private static String mvtPropertiesFlattenSql = "( select jsonb_object_agg('properties.' || jkey,jval) from prj_flatten( jsonb_set((jsondata)->'properties','{id}', to_jsonb( jsondata->>'id' )) ))";
  private static String
      mvtPropertiesSql        = "( select jsonb_object_agg(key, case when jsonb_typeof(value) in ('object', 'array') then to_jsonb(value::text) else value end) from jsonb_each(jsonb_set((jsondata)->'properties','{id}', to_jsonb(jsondata->>'id'))))";
  private boolean isMvtRequested;

  public GetFeaturesByBBox(E event) throws SQLException, ErrorResponseException {
    super(event);
  }

  @Override
  protected SQLQuery buildQuery(E event) throws SQLException, ErrorResponseException {
    if (event.getBbox().widthInDegree(false) >= (360d / 4d) || event.getBbox().heightInDegree() >= (180d / 4d)) //Is it a "big" query?
      //Check if Properties are indexed
      checkCanSearchFor(event);

    SQLQuery query = super.buildQuery(event);

    if (isMvtRequested = isMvtRequested(event))
      return buildMvtEncapsuledQuery((GetFeaturesByTileEvent) event, query);
    return query;
  }

  @Override
  public R handle(ResultSet rs) throws SQLException {
    return isMvtRequested ? (R) mvtResultSetHandler(rs) : super.handle(rs);
  }

  protected static BinaryResponse mvtResultSetHandler(ResultSet rs) throws SQLException {
    BinaryResponse br = new BinaryResponse()
        .withMimeType(APPLICATION_VND_MAPBOX_VECTOR_TILE);

    if (rs.next())
      br.setBytes(rs.getBytes("bin"));

    if (br.getBytes() != null && br.getBytes().length > MAX_RESULT_SIZE)
      throw new SQLException("Maximum response byte limit of " + MAX_RESULT_SIZE + " reached");

    return br;
  }

  @Override
  protected SQLQuery buildGeoFilter(GetFeaturesByBBoxEvent event) {
    return buildGeoFilterFromBbox(event.getBbox());
  }

  protected static SQLQuery buildGeoFilterFromBbox(BBox bbox) {
    SQLQuery geoFilter = new SQLQuery("ST_MakeEnvelope(#{minLon}, #{minLat}, #{maxLon}, #{maxLat}, 4326)")
        .withNamedParameter("minLon", bbox.minLon())
        .withNamedParameter("minLat", bbox.minLat())
        .withNamedParameter("maxLon", bbox.maxLon())
        .withNamedParameter("maxLat", bbox.maxLat());
    return geoFilter;
  }

  protected SQLQuery buildMvtEncapsuledQuery(GetFeaturesByTileEvent event, SQLQuery dataQry) {
    return buildMvtEncapsuledQuery(null, event, dataQry);
  }

  protected SQLQuery buildMvtEncapsuledQuery(String tableName, GetFeaturesByTileEvent event, SQLQuery dataQuery) {
    WebMercatorTile mvtTile = !event.getHereTileFlag() ? WebMercatorTile.forWeb(event.getLevel(), event.getX(), event.getY()) : null;
    HQuad hereTile = event.getHereTileFlag() ? new HQuad(event.getX(), event.getY(), event.getLevel()) : null;
    boolean isFlattened = event.getResponseType() == MVT_FLATTENED;
    String spaceIdOrTableName = tableName != null ? tableName : event.getSpace(); //TODO: Streamline function ST_AsMVT() so it only takes one or the other
    BBox eventBbox = event.getBbox();
      int extent = 4096, buffer = extent / WebMercatorTile.TileSizeInPixel * event.getMargin();
      BBox tileBbox = mvtTile != null ? mvtTile.getBBox(false) : (hereTile != null ? hereTile.getBoundingBox() : eventBbox); // pg ST_AsMVTGeom expects tiles bbox without buffer.

      SQLQuery outerQuery = new SQLQuery(
        """
          with tile as (select ${{bounds}} as bounds, #{extent} as extent, #{buffer} as buffer, true as clip_geom), 
          mvtdata as 
          ( 
           select ${{mvtProperties}} as mproperties, ST_AsMVTGeom(st_force2d(${{geoFrag}}), t.bounds, t.extent::integer, t.buffer::integer, t.clip_geom) as mgeo 
           from 
           (${{dataQuery}}) data , tile t 
          ) 
          select ST_AsMVT( mvtdata , #{spaceIdOrTableName} ) as bin from mvtdata where mgeo is not null 
        """
          )
          .withQueryFragment("bounds", new SQLQuery(hereTile == null ? "st_transform(${{tileBbox}}, 3857)" : "${{tileBbox}}")
          .withQueryFragment("tileBbox", buildGeoFilterFromBbox(tileBbox)))
          .withQueryFragment("mvtProperties", !isFlattened ? mvtPropertiesSql : mvtPropertiesFlattenSql)
          .withNamedParameter("extent", extent)
          .withNamedParameter("buffer", buffer)
          .withQueryFragment("geoFrag", hereTile == null ? "st_transform(geo, 3857)" : "geo")
          .withQueryFragment("dataQuery", dataQuery)
          .withNamedParameter("spaceIdOrTableName", spaceIdOrTableName);
      return outerQuery;
    }

  @Override
  protected SQLQuery buildGeoJsonExpression(E event) {
    if (getResponseType(event) == GEO_JSON)
      return super.buildGeoJsonExpression(event);
    //Do not use the GeoJSON representation if the geo fragment will be transformed to MVT
    return buildRawGeoExpression(event);
  }

  public ResponseType getResponseType(GetFeaturesByBBoxEvent event) {
    if (event instanceof GetFeaturesByTileEvent)
      return ((GetFeaturesByTileEvent) event).getResponseType();
    return GEO_JSON;
  }

  protected boolean isMvtRequested(GetFeaturesByBBoxEvent event) {
    ResponseType responseType = getResponseType(event);
    return responseType == MVT || responseType == MVT_FLATTENED;
  }

  //---------------------------

  public static WebMercatorTile getTileFromBbox(BBox bbox)
    {
     /* Quadkey calc */
     final int lev = WebMercatorTile.getZoomFromBBOX(bbox);
     double lon2 = bbox.minLon() + ((bbox.maxLon() - bbox.minLon()) / 2);
     double lat2 = bbox.minLat() + ((bbox.maxLat() - bbox.minLat()) / 2);

     return WebMercatorTile.getTileFromLatLonLev(lat2, lon2, lev);
    }

  protected static int samplingStrengthFromText(String sampling, boolean fiftyOnUnset)
  {
   int strength = 0;
   switch( sampling.toLowerCase() )
   { case "low"     : strength =  10;  break;
     case "lowmed"  : strength =  30;  break;
     case "med"     : strength =  50;  break;
     case "medhigh" : strength =  75;  break;
     case "high"    : strength = 100;  break;
     default: if( fiftyOnUnset ) strength = 50;  break;
   }

   return strength;

  }
}
