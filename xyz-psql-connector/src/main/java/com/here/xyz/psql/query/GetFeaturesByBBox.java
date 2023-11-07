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

package com.here.xyz.psql.query;

import static com.here.xyz.events.GetFeaturesByTileEvent.ResponseType.GEO_JSON;
import static com.here.xyz.events.GetFeaturesByTileEvent.ResponseType.MVT;
import static com.here.xyz.events.GetFeaturesByTileEvent.ResponseType.MVT_FLATTENED;
import static com.here.xyz.psql.DatabaseHandler.HEAD_TABLE_SUFFIX;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.GetFeaturesByTileEvent.ResponseType;
import com.here.xyz.models.geojson.HQuad;
import com.here.xyz.models.geojson.WebMercatorTile;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.PSQLXyzConnector;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.factory.TweaksSQL;
import com.here.xyz.psql.tools.DhString;
import com.here.xyz.responses.BinaryResponse;
import com.here.xyz.responses.XyzResponse;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

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
      checkCanSearchFor(event, PSQLXyzConnector.getInstance());

    SQLQuery query = super.buildQuery(event);

    if (isMvtRequested = isMvtRequested(event))
      return buildMvtEncapsuledQuery((GetFeaturesByTileEvent) event, query);
    return query;
  }

  @Override
  public R handle(ResultSet rs) throws SQLException {
    return isMvtRequested ? (R) defaultBinaryResultSetHandler(rs) : super.handle(rs);
  }

  protected static BinaryResponse defaultBinaryResultSetHandler(ResultSet rs) throws SQLException {
    BinaryResponse br = new BinaryResponse()
        .withMimeType(APPLICATION_VND_MAPBOX_VECTOR_TILE);

    if (rs.next())
      br.setBytes(rs.getBytes(1));

    if (br.getBytes() != null && br.getBytes().length > DatabaseHandler.MAX_RESULT_CHARS)
      throw new SQLException(DhString.format("Maximum bytes limit (%d) reached", DatabaseHandler.MAX_RESULT_CHARS));

    return br;
  }

  @Override
  protected SQLQuery buildGeoFilter(GetFeaturesByBBoxEvent event) {
    return buildGeoFilterFromBbox(event.getBbox());
  }

  protected SQLQuery buildGeoFilterFromBbox(BBox bbox) {
    SQLQuery geoFilter = new SQLQuery("ST_MakeEnvelope(#{minLon}, #{minLat}, #{maxLon}, #{maxLat}, 4326)")
        .withNamedParameter("minLon", bbox.minLon())
        .withNamedParameter("minLat", bbox.minLat())
        .withNamedParameter("maxLon", bbox.maxLon())
        .withNamedParameter("maxLat", bbox.maxLat());
    return geoFilter;
  }

  private SQLQuery generateCombinedQuery(GetFeaturesByBBoxEvent event, SQLQuery indexedQuery) {
    final SQLQuery query = new SQLQuery(
        "SELECT ${{selection}}, ${{geo}}"
            + "    FROM ${schema}.${headTable} ${{tableSample}}"
            + "    WHERE ${{filterWhereClause}} ${{orderBy}} ${{limit}}"
    )
        .withVariable(SCHEMA, getSchema())
        .withVariable("headTable", getDefaultTable((E) event) + HEAD_TABLE_SUFFIX);

    query.setQueryFragment("selection", buildSelectionFragment(event));
    query.setQueryFragment("geo", buildClippedGeoFragment((E) event, buildGeoFilter(event)));
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
          "with tile as (select ${{bounds}} as bounds, #{extent} as extent, #{buffer} as buffer, true as clip_geom), "
              + "mvtdata as "
              + "( "
              + " select ${{mvtProperties}} as mproperties, ST_AsMVTGeom(st_force2d(${{geoFrag}}), t.bounds, t.extent::integer, t.buffer::integer, t.clip_geom) as mgeo "
              + " from "
              + " (${{dataQuery}}) data , tile t "
              + ") "
              + "select ST_AsMVT( mvtdata , #{spaceIdOrTableName} ) as bin from mvtdata where mgeo is not null")
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
  protected SQLQuery buildClippedGeoFragment(E event, SQLQuery geoFilter) {
    boolean convertToGeoJson = getResponseType(event) == GEO_JSON;
    if (!event.getClip())
      return buildGeoFragment(event, convertToGeoJson);

    SQLQuery clippedGeo = new SQLQuery("ST_Intersection(ST_MakeValid(geo), ${{geoFilter}})")
        .withQueryFragment("geoFilter", geoFilter);

    return super.buildGeoFragment(event, convertToGeoJson, clippedGeo);
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


  /***************************************** TWEAKS **************************************************/

  public static ResponseType getResponseType(GetFeaturesByBBoxEvent event) {
    if (event instanceof GetFeaturesByTileEvent)
      return ((GetFeaturesByTileEvent) event).getResponseType();
    return GEO_JSON;
  }

  protected static boolean isMvtRequested(GetFeaturesByBBoxEvent event) {
    ResponseType responseType = getResponseType(event);
    return responseType == MVT || responseType == MVT_FLATTENED;
  }

  private static String map2MvtGeom( GetFeaturesByBBoxEvent event, BBox bbox, String tweaksGeoSql )
  {
   boolean bExtentTweaks = // -> 2048 only if tweaks or viz been specified explicit
    ( "viz".equals(event.getOptimizationMode()) || (event.getTweakParams() != null && event.getTweakParams().size() > 0 ) );

   int extent = ( bExtentTweaks ? 2048 : 4096 ), extentPerMargin = extent / WebMercatorTile.TileSizeInPixel, extentWithMargin = extent, level = -1, tileX = -1, tileY = -1, margin = 0;
   boolean hereTile = false;

   if( event instanceof GetFeaturesByTileEvent )
   { GetFeaturesByTileEvent tevnt = (GetFeaturesByTileEvent) event;
     level = tevnt.getLevel();
     tileX = tevnt.getX();
     tileY = tevnt.getY();
     margin = tevnt.getMargin();
     extentWithMargin = extent + (margin * extentPerMargin);
     hereTile = tevnt.getHereTileFlag();
   }
   else
   { final WebMercatorTile tile = getTileFromBbox(bbox);
     level = tile.level;
     tileX = tile.x;
     tileY = tile.y;
   }

   double wgs3857width = 20037508.342789244d, wgs4326width = 180d,
          wgsWidth = 2 * ( !hereTile ? wgs3857width : wgs4326width ),
          xwidth = wgsWidth,
          ywidth = wgsWidth,
          gridsize = (1L << level),
          stretchFactor = 1.0 + ( margin / ((double) WebMercatorTile.TileSizeInPixel)), // xyz-hub uses margin for tilesize of 256 pixel.
          xProjShift = (!hereTile ? (tileX - gridsize/2 + 0.5) * (xwidth / gridsize)      : -180d + (tileX + 0.5) * (xwidth / gridsize) ) ,
          yProjShift = (!hereTile ? (tileY - gridsize/2 + 0.5) * (ywidth / gridsize) * -1 :  -90d + (tileY + 0.5) * (ywidth / gridsize) ) ;

   String
    box2d   = DhString.format( DhString.format("ST_MakeEnvelope(%%.%1$df,%%.%1$df,%%.%1$df,%%.%1$df, 4326)", 14 /*GEOMETRY_DECIMAL_DIGITS*/), bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat() ),
      // 1. build mvt
    mvtgeom = DhString.format( (!hereTile ? "st_asmvtgeom(st_force2d(st_transform(%1$s,3857)), st_transform(%2$s,3857),%3$d,0,true)"
                                          : "st_asmvtgeom(st_force2d(%1$s), %2$s,%3$d,0,true)")
                               , tweaksGeoSql, box2d, extentWithMargin);
      // 2. project the mvt to tile
    mvtgeom = DhString.format( (!hereTile ? "st_setsrid(st_translate(st_scale(st_translate(%1$s, %2$d , %2$d, 0.0), st_makepoint(%3$f,%4$f,1.0) ), %5$f , %6$f, 0.0 ), 3857)"
                                          : "st_setsrid(st_translate(st_scale(st_translate(%1$s, %2$d , %2$d, 0.0), st_makepoint(%3$f,%4$f,1.0) ), %5$f , %6$f, 0.0 ), 4326)")
                               ,mvtgeom
                               ,-extentWithMargin/2 // => shift to stretch from tilecenter
                               ,stretchFactor*(xwidth / (gridsize*extent)), stretchFactor * (ywidth / (gridsize*extent)) * -1 // stretch tile to proj. size
                               ,xProjShift, yProjShift // shift to proj. position
                             );
      // 3 project tile to wgs84 and map invalid geom to null

    mvtgeom = DhString.format( (!hereTile ? "(select case st_isvalid(g) when true then g else null end from st_transform(%1$s,4326) g)"
                                          : "(select case st_isvalid(g) when true then g else null end from %1$s g)")
                               , mvtgeom );

      // 4. assure intersect with origin bbox in case of mapping errors
    mvtgeom  = DhString.format("ST_Intersection(%1$s,st_setsrid(%2$s,4326))", mvtgeom, box2d);
      // 5. map non-null but empty polygons to null - e.g. avoid -> "geometry": { "type": "Polygon", "coordinates": [] }
    mvtgeom = DhString.format("(select case st_isempty(g) when false then g else null end from %1$s g)", mvtgeom );

    // if geom = point | multipoint then no mvt <-> geo conversion should be done
    mvtgeom = DhString.format("case strpos(ST_GeometryType( geo ), 'Point') > 0 when true then geo else %1$s end", mvtgeom );

    return mvtgeom;
  }

  private static String clipProjGeom(BBox bbox, String tweaksGeoSql )
  {
   String fmt =  DhString.format(  " case st_within( %%1$s, ST_MakeEnvelope(%%2$.%1$df,%%3$.%1$df,%%4$.%1$df,%%5$.%1$df, 4326) ) "
                               + "  when true then %%1$s "
                               + "  else ST_Intersection(ST_MakeValid(%%1$s),ST_MakeEnvelope(%%2$.%1$df,%%3$.%1$df,%%4$.%1$df,%%5$.%1$df, 4326))"
                               + " end " , 14 /*GEOMETRY_DECIMAL_DIGITS*/ );
   return DhString.format( fmt, tweaksGeoSql, bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat());
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

  public SQLQuery buildSamplingTweaksQuery(GetFeaturesByBBoxEvent event, Map tweakParams) {
    BBox bbox = event.getBbox();
    boolean bSortByHashedValue = (boolean) tweakParams.get("sortByHashedValue");
   int strength = 0;
   boolean bDistribution  = true,
           bDistribution2 = false,
           bConvertGeo2Geojson = getResponseType(event) == GEO_JSON;

   if( tweakParams != null )
   {
    if( tweakParams.get(TweaksSQL.SAMPLING_STRENGTH) instanceof Integer )
     strength = (int) tweakParams.get(TweaksSQL.SAMPLING_STRENGTH);
    else
     strength = samplingStrengthFromText( (String) tweakParams.getOrDefault(TweaksSQL.SAMPLING_STRENGTH,"default"), true );

     switch(((String) tweakParams.getOrDefault(TweaksSQL.SAMPLING_ALGORITHM, TweaksSQL.SAMPLING_ALGORITHM_DST)).toLowerCase() )
     {
       case TweaksSQL.SAMPLING_ALGORITHM_SZE  : bDistribution = false; break;
       case TweaksSQL.SAMPLING_ALGORITHM_DST2 : bDistribution2 = true;
       case TweaksSQL.SAMPLING_ALGORITHM_DST  :
       default                                : bDistribution = true; break;
     }
   }

   boolean bEnsureMode = TweaksSQL.ENSURE.equalsIgnoreCase(event.getTweakType());

   float tblSampleRatio = ( (strength > 0 && bDistribution2) ? TweaksSQL.tableSampleRatio(strength) : -1f);

   final String sCondition = ( ((bEnsureMode && strength == 0) || (tblSampleRatio >= 0.0)) ? "1 = 1" : TweaksSQL.strengthSql(strength,bDistribution)  ),
                twqry = DhString.format(DhString.format("ST_Intersects(geo, ST_MakeEnvelope(%%.%1$df,%%.%1$df,%%.%1$df,%%.%1$df, 4326) ) and %%s", 14 /*GEOMETRY_DECIMAL_DIGITS*/), bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat(), sCondition );

   final SQLQuery tweakQuery = new SQLQuery(twqry);

   if( !bEnsureMode || !bConvertGeo2Geojson ) {
     SQLQuery combinedQuery = generateCombinedQuery(event, tweakQuery);
     if (tblSampleRatio > 0.0)
       combinedQuery.setQueryFragment("tableSample", DhString.format("tablesample system(%.6f) repeatable(499)", 100.0 * tblSampleRatio));
     if (bSortByHashedValue)
       combinedQuery.setQueryFragment("orderBy", "ORDER BY " + TweaksSQL.distributionFunctionIndexExpression());
     return combinedQuery;
   }


   /* TweaksSQL.ENSURE and geojson requested (no mvt) */
   boolean bTestTweaksGeoIfNull = false;
   String tweaksGeoSql = clipProjGeom(bbox,"geo");
   tweaksGeoSql = map2MvtGeom( event, bbox, tweaksGeoSql );
   //convert to geojson
   tweaksGeoSql = ( bConvertGeo2Geojson ? DhString.format("replace(ST_AsGeojson(" + getForceMode(event.isForce2D()) + "( %s ),%d),'nan','0')",tweaksGeoSql,GEOMETRY_DECIMAL_DIGITS)
                                        : DhString.format( getForceMode(event.isForce2D()) + "( %s )",tweaksGeoSql ) );

   return generateCombinedQueryTweaks(event, tweakQuery , tweaksGeoSql, bTestTweaksGeoIfNull, tblSampleRatio, bSortByHashedValue );
}

  private static double cToleranceFromStrength(int strength )
  {
   double tolerance = 0.0001;
   if(  strength > 0  && strength <= 25 )      tolerance = 0.0001 + ((0.001-0.0001)/25.0) * (strength -  1);
   else if(  strength > 25 && strength <= 50 ) tolerance = 0.001  + ((0.01 -0.001) /25.0) * (strength - 26);
   else if(  strength > 50 && strength <= 75 ) tolerance = 0.01   + ((0.1  -0.01)  /25.0) * (strength - 51);
   else /* [76 - 100 ] */                      tolerance = 0.1    + ((1.0  -0.1)   /25.0) * (strength - 76);

   return tolerance;
  }

  public SQLQuery buildSimplificationTweaksQuery(GetFeaturesByBBoxEvent event, Map tweakParams) throws SQLException
  {
    BBox bbox = event.getBbox();
   int strength = 0,
       iMerge = 0;
   String tweaksGeoSql = "geo";
   boolean bStrength = true, bTestTweaksGeoIfNull = true, convertGeo2Geojson = getResponseType(event) == GEO_JSON, bMvtRequested = !convertGeo2Geojson;

   if( tweakParams != null )
   {
    if( tweakParams.get(TweaksSQL.SIMPLIFICATION_STRENGTH) instanceof Integer )
     strength = (int) tweakParams.get(TweaksSQL.SIMPLIFICATION_STRENGTH);
    else
     switch(((String) tweakParams.getOrDefault(TweaksSQL.SIMPLIFICATION_STRENGTH,"default")).toLowerCase() )
     { case "low"     : strength =  20;  break;
       case "lowmed"  : strength =  40;  break;
       case "med"     : strength =  60;  break;
       case "medhigh" : strength =  80;  break;
       case "high"    : strength = 100; break;
       case "default" : bStrength = false;
       default: strength  = 50; break;
     }

     String tweaksAlgorithm = ((String) tweakParams.getOrDefault(TweaksSQL.SIMPLIFICATION_ALGORITHM,"default")).toLowerCase();

     // do clip before simplification -- preventing extrem large polygon for further working steps (except for mvt with gridbytilelevel, redundancy)
     if (event.getClip() && !( TweaksSQL.SIMPLIFICATION_ALGORITHM_A05.equals( tweaksAlgorithm ) && bMvtRequested ) )
      tweaksGeoSql = clipProjGeom( bbox,tweaksGeoSql );

     //SIMPLIFICATION_ALGORITHM
     int hint = 0;
     String[] pgisAlgorithm = { "ST_SnapToGrid", "ftm_SimplifyPreserveTopology", "ftm_Simplify" };

     switch( tweaksAlgorithm )
     {
       case TweaksSQL.SIMPLIFICATION_ALGORITHM_A03 : hint++;
       case TweaksSQL.SIMPLIFICATION_ALGORITHM_A02 : hint++;
       case TweaksSQL.SIMPLIFICATION_ALGORITHM_A01 :
       {
        double tolerance = ( bStrength ? cToleranceFromStrength( strength ) : Math.abs( bbox.maxLon() - bbox.minLon() ) / 4096 );
        tweaksGeoSql = DhString.format("%s(%s, %f)", pgisAlgorithm[hint], tweaksGeoSql, tolerance );
       }
       break;

       case TweaksSQL.SIMPLIFICATION_ALGORITHM_A05 : // gridbytilelevel - convert to/from mvt
       {
        if(!bMvtRequested)
         tweaksGeoSql = map2MvtGeom( event, bbox, tweaksGeoSql );
        bTestTweaksGeoIfNull = false;
       }
       break;

       case TweaksSQL.SIMPLIFICATION_ALGORITHM_A06 : iMerge++;
       case TweaksSQL.SIMPLIFICATION_ALGORITHM_A04 : iMerge++; break;

       default: break;
     }

     //convert to geojson
     tweaksGeoSql = ( convertGeo2Geojson ? DhString.format("replace(ST_AsGeojson(" + getForceMode(event.isForce2D()) + "( %s ),%d),'nan','0')",tweaksGeoSql,GEOMETRY_DECIMAL_DIGITS)
                                          : DhString.format( getForceMode(event.isForce2D()) + "( %s )",tweaksGeoSql) );
   }

     final String bboxqry = DhString.format( DhString.format("ST_Intersects(geo, ST_MakeEnvelope(%%.%1$df,%%.%1$df,%%.%1$df,%%.%1$df, 4326) )", 14 /*GEOMETRY_DECIMAL_DIGITS*/), bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat() );

     if (iMerge == 0)
       return generateCombinedQueryTweaks(event, new SQLQuery(bboxqry), tweaksGeoSql, bTestTweaksGeoIfNull, -1.0f, false );

     // Merge Algorithm - only using low, med, high

     int minGeoHashLenToMerge = 0,
         minGeoHashLenForLineMerge = 3;

     if      ( strength <= 20 ) { minGeoHashLenToMerge = 7; minGeoHashLenForLineMerge = 7; } //low
     else if ( strength <= 40 ) { minGeoHashLenToMerge = 6; minGeoHashLenForLineMerge = 6; } //lowmed
     else if ( strength <= 60 ) { minGeoHashLenToMerge = 6; minGeoHashLenForLineMerge = 5; } //med
     else if ( strength <= 80 ) {                           minGeoHashLenForLineMerge = 4; } //medhigh

     if( "geo".equals(tweaksGeoSql) ) // formal, just in case
      tweaksGeoSql = ( convertGeo2Geojson ? DhString.format("replace(ST_AsGeojson(" + getForceMode(event.isForce2D()) + "( %s ),%d),'nan','0')",tweaksGeoSql,GEOMETRY_DECIMAL_DIGITS)
                                           : DhString.format(getForceMode(event.isForce2D()) + "( %s )",tweaksGeoSql) );

     if( convertGeo2Geojson )
      tweaksGeoSql = DhString.format("(%s)::jsonb", tweaksGeoSql);


     SQLQuery query = buildSimplificationTweaksMergeQuery(event, iMerge, tweaksGeoSql, minGeoHashLenToMerge, minGeoHashLenForLineMerge, bboxqry, convertGeo2Geojson);
     final SQLQuery searchQuery = generateSearchQuery(event);
     if (searchQuery != null)
       query.setQueryFragment("searchQuery", new SQLQuery("AND ${{sq}}").withQueryFragment("sq", searchQuery));
     return query;
}

private SQLQuery buildSimplificationTweaksMergeQuery(GetFeaturesByBBoxEvent event, int iMerge, String tweaksGeoSql, int minGeoHashLenToMerge, int minGeoHashLenForLineMerge, String bboxQuery, boolean convertGeo2Geojson) {
    SQLQuery query;
    if (iMerge == 1) {
      query = new SQLQuery("select jsondata, geo "
          +"from "
          +"( "
          +" select jsonb_set('{\"type\": \"Feature\"}'::jsonb,'{properties}', jsonb_set( case when (ginfo->>1)::integer = 1 then jsonb_set( '{}'::jsonb,'{ids}', ids ) else '{}'::jsonb end , '{groupInfo}', ginfo )) as jsondata, ${{tweaksGeo}} as geo"
          +" from "
          +" ( "
          +"  select jsonb_build_array(left(md5( gh || gsz ), 12), row_number() over w, count(1) over w, nrobj ) as ginfo, * "
          +"  from "
          +"  ( "
          +"   select gh, case length(gh) > #{minGeoHashLenToMerge} when true then 0 else i end as gsz, count(1) as nrobj, jsonb_agg(id) as ids, (st_dump( st_union(oo.geo) )).geom as geo "
          +"   from "
          +"   ( "
          +"    select ST_GeoHash(geo) as gh, i, id , geo "
          +"    from "
          +"    ( "
          +"     select i, id, geo "  // fetch objects
          +"     from ${schema}.${table} "
          +"     where 1 = 1 "
          +"       and ${{bboxQuery}} ${{searchQuery}} " + "    ) o "
          +"   ) oo "
          +"   group by gsz, gh"
          +"  ) ooo window w as (partition by gh, gsz ) "
          +" ) oooo "
          +") ooooo "
          +"where geo IS NOT NULL AND ${{geoCheck}} LIMIT #{limit}")
          .withQueryFragment("geoCheck", convertGeo2Geojson
              ? "geo->>'type' != 'GeometryCollection' and jsonb_array_length(geo->'coordinates') > 0 "
              : "geometrytype(geo) != 'GEOMETRYCOLLECTION' and not st_isempty(geo) ")
          .withNamedParameter("minGeoHashLenToMerge", minGeoHashLenToMerge);
    }
    else {
      query = new SQLQuery("with "
          +"indata as "
          +"( select i, ${{geo}} as geo from ${schema}.${table} "
          +"  where 1 = 1 "
          +"    and ${{bboxQuery}} ${{searchQuery}}), "
          +"cx2ids as "
          +"( select left( gid, #{minGeoHashLenForLineMerge} ) as region, ids "
          +"  from "
          +"  ( select gid, array_agg( i ) as ids "
          +"    from "
          +"    ( select i, unnest( array[ ST_GeoHash( st_startpoint(geo),9 ) , ST_GeoHash( st_endpoint(geo), 9 ) ] ) as gid from indata where ( geometrytype(geo) = 'LINESTRING' ) ) o "
          +"    group by gid "
          +"  ) o	"
          +"  where 1 = 1 "
          +"    and cardinality(ids) = 2 "
          +"), "
          +"cxlist as "
          +"( select count(1) over ( PARTITION BY region ) as rcount, array[(row_number() over ( PARTITION BY region ))::integer] as rids, region, ids from cx2ids ), "
          +"mergedids as "
          +"( with recursive mrgdids( step, region, rcount, rids, ids ) as "
          +"  ( "
          +"	  select 1, region, rcount, rids, ids from cxlist "
          +"	 union all "
          +"		select distinct on (region, rids[1] ) * "
          +"		from "
          +"		( select l.step+1 as step, l.region, l.rcount, array( select unnest( l.rids || r.rids ) order by 1 )  as rids, l.ids || r.ids as ids "
          +"		  from mrgdids l join cxlist r on ( l.region = r.region and not (l.rids @> r.rids) and  (l.ids && r.ids ) ) "
          +"		  where 1 = 1 "
          +"		) i1 "
          +"	) "
          +"  select l.region, l.rcount, l.step, l.rids, array( select distinct unnest( l.ids ) ) as ids "
          +"  from mrgdids l left join mrgdids r on ( l.region = r.region and l.step < r.step and l.rids <@ r.rids ) "
          +"  where 1 = 1 "
          +"    and r.region is null "
          +"), "
          +"ccxuniqid as "
          +"( select distinct unnest(ids) as id from cx2ids ), "
          +"iddata as "
          +"(  select step, ids from mergedids "
          +"  union "
          +"   select 0 as step, array[i] as ids from indata where not i in (select id from ccxuniqid ) "
          +"), "
          +"finaldata as "
          +"(	select "
          +"   case when step = 0 "
          +"    then ( SELECT ${{selection}} FROM ${schema}.${table} where i = ids[1] ) "
          +"    else ( select jsonb_set( jsonb_set('{\"type\":\"Feature\",\"properties\":{}}'::jsonb,'{id}', to_jsonb( max(jsondata->>'id') )),'{properties,ids}', jsonb_agg( jsondata->>'id' )) from ${schema}.${table} where i in ( select unnest( ids ) ) ) "
          +"   end as jsondata, "
          +"   case when step = 0 "
          +"    then ( select geo from ${schema}.${table} where i = ids[1] ) "
          +"    else ( select ST_LineMerge( st_collect( geo ) ) from ${schema}.${table} where i in ( select unnest( ids )) ) "
          +"   end as geo "
          +"  from iddata "
          +") "
          +"select jsondata, ${{tweaksGeo}} as geo from finaldata LIMIT #{limit}")
          .withQueryFragment("geo", "geo") //(event.getClip() ? clipProjGeom(bbox,"geo") : "geo")
          .withQueryFragment("selection", buildSelectionFragment(event))
          .withNamedParameter("minGeoHashLenForLineMerge", minGeoHashLenForLineMerge);

    }
    return query
        .withVariable(SCHEMA, getSchema())
        .withVariable(TABLE, readTableFromEvent(event))
        .withQueryFragment("tweaksGeo", tweaksGeoSql)
        .withQueryFragment("bboxQuery", bboxQuery)
        .withNamedParameter("limit", event.getLimit());

}

  /** ###################################################################################### */

  private SQLQuery generateCombinedQueryTweaks(GetFeaturesByBBoxEvent event, SQLQuery indexedQuery, String tweaksgeo, boolean testTweaksGeoIfNull, float sampleRatio, boolean sortByHashedValue)
  {
    SQLQuery searchQuery = generateSearchQuery(event);

    final SQLQuery query = new SQLQuery("SELECT * FROM (SELECT ${{selection}}${{geo}} FROM ${schema}.${table} ${{sampling}} WHERE ${{indexedQuery}} ${{searchQuery}} ${{orderBy}}) tw ${{outerWhereClause}} LIMIT #{limit}")
        .withVariable(SCHEMA, getSchema())
        .withVariable(TABLE, readTableFromEvent(event) + HEAD_TABLE_SUFFIX)
        .withQueryFragment("selection", buildSelectionFragment(event))
        .withQueryFragment("geo", DhString.format(",%s as geo", tweaksgeo))
        .withQueryFragment("sampling", "")
        .withQueryFragment("indexedQuery", indexedQuery)
        .withQueryFragment("searchQuery", "")
        .withQueryFragment("orderBy", "")
        .withQueryFragment("outerWhereClause", "")
        .withNamedParameter("limit", event.getLimit());

    if (sampleRatio > 0) {
      SQLQuery sampling = new SQLQuery("tablesample system(#{samplePercentage}) repeatable(499)")
          .withNamedParameter("samplePercentage", 100f * sampleRatio);
      query.setQueryFragment("sampling", sampling);
    }

    if (searchQuery != null)
      query.setQueryFragment("searchQuery", new SQLQuery("AND ${{sq}}").withQueryFragment("sq", searchQuery));

    if (sortByHashedValue)
      query.setQueryFragment("orderBy", "ORDER BY " + TweaksSQL.distributionFunctionIndexExpression());

    if (testTweaksGeoIfNull)
      query.setQueryFragment("outerWhereClause", "geo IS NOT NULL");

    return query;
  }

  private static String getForceMode(boolean isForce2D) {
    return isForce2D ? "ST_Force2D" : "ST_Force3D";
  }
}
