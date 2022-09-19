/*
 * Copyright (C) 2017-2021 HERE Europe B.V.
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
package com.here.xyz.psql;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.events.CountFeaturesEvent;
import com.here.xyz.events.Event;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.GetHistoryStatisticsEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.IterateHistoryEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.PropertyQuery;
import com.here.xyz.events.QueryEvent;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.events.SearchForFeaturesOrderByEvent;
import com.here.xyz.events.TagList;
import com.here.xyz.events.TagsQuery;
import com.here.xyz.models.geojson.HQuad;
import com.here.xyz.models.geojson.WebMercatorTile;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.coordinates.WKTHelper;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.models.hub.Space;
import com.here.xyz.psql.config.PSQLConfig;
import com.here.xyz.psql.factory.H3SQL;
import com.here.xyz.psql.factory.IterateSortSQL;
import com.here.xyz.psql.factory.QuadbinSQL;
import com.here.xyz.psql.factory.TweaksSQL;
import com.here.xyz.util.DhString;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

public class SQLQueryBuilder {
    public static final long GEOMETRY_DECIMAL_DIGITS = 8;
    private static final String IDX_STATUS_TABLE = "xyz_config.xyz_idxs_status";
    private static final Integer BIG_SPACE_THRESHOLD = 10000;

    public static SQLQuery buildGetStatisticsQuery(Event event, PSQLConfig config, boolean historyMode) throws Exception {
        String function;
        if(event instanceof GetHistoryStatisticsEvent)
            function = "xyz_statistic_history";
        else
            function = "xyz_statistic_space";
        final String schema = config.getDatabaseSettings().getSchema();
        final String table = config.readTableFromEvent(event) + (!historyMode ? "" : "_hst");

        return new SQLQuery("SELECT * from " + schema + "."+function+"('" + schema + "','" + table + "')");
    }

    public static SQLQuery buildGetNextVersionQuery(String table) throws Exception {
        return new SQLQuery("SELECT nextval('${schema}.\"" + table.replaceAll("-","_") + "_hst_seq\"')");
    }

    public static SQLQuery buildGetFeaturesByGeometryQuery(GetFeaturesByGeometryEvent event, DataSource dataSource)
        throws SQLException{
        final int radius = event.getRadius();
        final Geometry geometry = event.getGeometry();

        final SQLQuery searchQuery = generateSearchQuery(event, dataSource);
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
        return generateCombinedQuery(event, geoQuery, searchQuery, dataSource, true, event.getH3Index());
    }

    public static SQLQuery buildGetFeaturesByBBoxQuery(final GetFeaturesByBBoxEvent event, boolean isBigQuery, DataSource dataSource)
        throws SQLException{
        final BBox bbox = event.getBbox();

        final SQLQuery searchQuery = generateSearchQuery(event, dataSource);
        final SQLQuery geoQuery = new SQLQuery("ST_Intersects(geo, ST_MakeEnvelope(?, ?, ?, ?, 4326))",
                bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat());

        boolean bConvertGeo2GeoJson = ( mvtTypeRequested(event) == 0 );

        return generateCombinedQuery(event, geoQuery, searchQuery,dataSource, bConvertGeo2GeoJson, null);
    }

    protected static SQLQuery buildCountFeaturesQuery(CountFeaturesEvent event, DataSource dataSource, String schema, String table)
        throws SQLException{

        final SQLQuery searchQuery = generateSearchQuery(event, dataSource);
        final String schemaTable = SQLQuery.sqlQuote(schema) + "." + SQLQuery.sqlQuote(table);
        final SQLQuery query;

        if (searchQuery != null) {
            query = new SQLQuery("SELECT count(*) FROM ${schema}.${table} WHERE");
            query.append(searchQuery);
        } else {
            query = new SQLQuery("SELECT CASE WHEN reltuples < ?");
            query.append("THEN (SELECT count(*) FROM ${schema}.${table})");
            query.append("ELSE reltuples END AS count");
            query.append("FROM pg_class WHERE oid =?::regclass", BIG_SPACE_THRESHOLD, schemaTable);
        }
        return query;
    }

    /***************************************** CLUSTERING ******************************************************/

    private static int evalH3Resolution( Map<String, Object> clusteringParams, int defaultResForLevel )
    {
     int h3res = defaultResForLevel, overzoomingRes = 2; // restrict to "defaultResForLevel + 2" as maximum resolution per level

     if( clusteringParams == null ) return h3res;
/** deprecated */
     if( clusteringParams.get(H3SQL.HEXBIN_RESOLUTION) != null )
      h3res = Math.min((Integer) clusteringParams.get(H3SQL.HEXBIN_RESOLUTION), defaultResForLevel + overzoomingRes);
/***/
     if( clusteringParams.get(H3SQL.HEXBIN_RESOLUTION_ABSOLUTE) != null )
      h3res = Math.min((Integer) clusteringParams.get(H3SQL.HEXBIN_RESOLUTION_ABSOLUTE), defaultResForLevel + overzoomingRes);

     if( clusteringParams.get(H3SQL.HEXBIN_RESOLUTION_RELATIVE) != null )
      h3res += Math.max(-2, Math.min( 2, (Integer) clusteringParams.get(H3SQL.HEXBIN_RESOLUTION_RELATIVE)));

     return Math.min( Math.min( h3res, defaultResForLevel + overzoomingRes ) , 13 ); // cut to maximum res
    }

    public static SQLQuery buildHexbinClusteringQuery(
            GetFeaturesByBBoxEvent event, BBox bbox,
            Map<String, Object> clusteringParams, DataSource dataSource) throws Exception {

        int zLevel = (event instanceof GetFeaturesByTileEvent ? ((GetFeaturesByTileEvent) event).getLevel() : H3SQL.bbox2zoom(bbox)),
            defaultResForLevel = H3SQL.zoom2resolution(zLevel),
            h3res = evalH3Resolution( clusteringParams, defaultResForLevel );

        if( zLevel == 1)  // prevent ERROR:  Antipodal (180 degrees long) edge detected!
         if( bbox.minLon() == 0.0 )
          bbox.setEast( bbox.maxLon() - 0.0001 );
         else
          bbox.setWest( bbox.minLon() + 0.0001);

        String statisticalProperty = (String) clusteringParams.get(H3SQL.HEXBIN_PROPERTY);
        boolean statisticalPropertyProvided = (statisticalProperty != null && statisticalProperty.length() > 0),
                h3cflip = (clusteringParams.get(H3SQL.HEXBIN_POINTMODE) == Boolean.TRUE);
               /** todo: replace format %.14f with parameterized version*/
        final String expBboxSql = String
                .format("st_envelope( st_buffer( ST_MakeEnvelope(%.14f,%.14f,%.14f,%.14f, 4326)::geography, ( 2.5 * edgeLengthM( %d )) )::geometry )",
                        bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat(), h3res);

        /*clippedGeo - passed bbox is extended by "margin" on service level */
        String clippedGeo = (!event.getClip() ? "geo" : String
                .format("ST_Intersection(st_makevalid(geo),ST_MakeEnvelope(%.14f,%.14f,%.14f,%.14f,4326) )", bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat())),
                fid = (!event.getClip() ? "h3" : DhString.format("h3 || %f || %f", bbox.minLon(), bbox.minLat())),
                filterEmptyGeo = (!event.getClip() ? "" : DhString.format(" and not st_isempty( %s ) ", clippedGeo));

        final SQLQuery searchQuery = generateSearchQuery(event, dataSource);

        String aggField = (statisticalPropertyProvided ? "jsonb_set('{}'::jsonb, ? , agg::jsonb)::json" : "agg");

        boolean bConvertGeo2Geojson = ( mvtTypeRequested(event) == 0 );

        final SQLQuery query = new SQLQuery(DhString.format(H3SQL.h3sqlBegin, h3res,
                !h3cflip ? "st_centroid(geo)" : "geo",
                DhString.format( (bConvertGeo2Geojson ? "st_asgeojson( %1$s, 7 )::json" : "(%1$s)" ), (h3cflip ? "st_centroid(geo)" : clippedGeo) ),
                statisticalPropertyProvided ? ", min, max, sum, avg, median" : "",
                zLevel,
                !h3cflip ? "centroid" : "hexagon",
                aggField,
                fid,
                expBboxSql));

        if (statisticalPropertyProvided) {
            ArrayList<String> jpath = new ArrayList<>();
            jpath.add(statisticalProperty);
            query.addParameter(jpath.toArray(new String[]{}));
        }

        int pxSize = H3SQL.adjPixelSize( h3res, defaultResForLevel );

        String h3sqlMid = H3SQL.h3sqlMid( clusteringParams.get(H3SQL.HEXBIN_SINGLECOORD) == Boolean.TRUE );

        int samplingStrength = samplingStrengthFromText((String) clusteringParams.getOrDefault(H3SQL.HEXBIN_SAMPLING, "off"),false);
        String samplingCondition =  ( samplingStrength <= 0 ? "1 = 1" : TweaksSQL.strengthSql( samplingStrength, true) );

        if (!statisticalPropertyProvided) {
            query.append(new SQLQuery(DhString.format(h3sqlMid, h3res, "(0.0)::numeric", zLevel, pxSize,expBboxSql,samplingCondition)));
        } else {
            ArrayList<String> jpath = new ArrayList<>();
            jpath.add("properties");
            jpath.addAll(Arrays.asList(statisticalProperty.split("\\.")));

            query.append(new SQLQuery(DhString.format(h3sqlMid, h3res, "(jsondata#>> ?)::numeric", zLevel, pxSize,expBboxSql,samplingCondition)));
            query.addParameter(jpath.toArray(new String[]{}));
        }

        if (searchQuery != null) {
            query.append(" and ");
            query.append(searchQuery);
        }

        query.append(DhString.format(H3SQL.h3sqlEnd, filterEmptyGeo));
        query.append("LIMIT ?", event.getLimit());

        return query;
    }

    private static WebMercatorTile getTileFromBbox(BBox bbox)
    {
     /* Quadkey calc */
     final int lev = WebMercatorTile.getZoomFromBBOX(bbox);
     double lon2 = bbox.minLon() + ((bbox.maxLon() - bbox.minLon()) / 2);
     double lat2 = bbox.minLat() + ((bbox.maxLat() - bbox.minLat()) / 2);

     return WebMercatorTile.getTileFromLatLonLev(lat2, lon2, lev);
    }

    public static SQLQuery buildQuadbinClusteringQuery(GetFeaturesByBBoxEvent event,
                                                          BBox bbox, int relResolution, int absResolution, String countMode,
                                                          PSQLConfig config, boolean noBuffer) {
        boolean isTileRequest = (event instanceof GetFeaturesByTileEvent) && ((GetFeaturesByTileEvent) event).getMargin() == 0,
                clippedOnBbox = (!isTileRequest && event.getClip());

        final WebMercatorTile tile = getTileFromBbox(bbox);

        if( (absResolution - tile.level) >= 0 )  // case of valid absResolution convert it to a relative resolution and add both resolutions
         relResolution = Math.min( relResolution + (absResolution - tile.level), 5);

        SQLQuery propQuery;
        String propQuerySQL = null;
        final PropertiesQuery propertiesQuery = event.getPropertiesQuery();


        if (propertiesQuery != null) {
            propQuery = generatePropertiesQuery(propertiesQuery);

            if (propQuery != null) {
                propQuerySQL = propQuery.text();
                for (Object param : propQuery.parameters()) {
                    propQuerySQL = propQuerySQL.replaceFirst("\\?", "'" + param + "'");
                }
            }
        }
        boolean bConvertGeo2Geojson = ( mvtTypeRequested(event) == 0 );

        return QuadbinSQL.generateQuadbinClusteringSQL(config.getDatabaseSettings().getSchema(), config.readTableFromEvent(event), relResolution, countMode, propQuerySQL, tile, bbox, isTileRequest, clippedOnBbox, noBuffer, bConvertGeo2Geojson);
    }

    /***************************************** CLUSTERING END **************************************************/

    /***************************************** TWEAKS **************************************************/

    public static int mvtTypeRequested(GetFeaturesByBBoxEvent event) {
      if (event instanceof GetFeaturesByTileEvent && ((GetFeaturesByTileEvent) event).getResponseType() != null) {
        switch (((GetFeaturesByTileEvent) event).getResponseType()) {
          case MVT:
            return 1;
          case MVT_FLATTENED:
            return 2;
        }
      }
      return 0;
    }

    private static String map2MvtGeom( GetFeaturesByBBoxEvent event, BBox bbox, String tweaksGeoSql )
    {
     boolean bExtendTweaks = // -> 2048 only if tweaks or viz been specified explicit
      ( "viz".equals(event.getOptimizationMode()) || (event.getTweakParams() != null && event.getTweakParams().size() > 0 ) );

     int extend = ( bExtendTweaks ? 2048 : 4096 ), extendPerMargin = extend / WebMercatorTile.TileSizeInPixel, extendWithMargin = extend, level = -1, tileX = -1, tileY = -1, margin = 0;
     boolean hereTile = false;

     if( event instanceof GetFeaturesByTileEvent )
     { GetFeaturesByTileEvent tevnt = (GetFeaturesByTileEvent) event;
       level = tevnt.getLevel();
       tileX = tevnt.getX();
       tileY = tevnt.getY();
       margin = tevnt.getMargin();
       extendWithMargin = extend + (margin * extendPerMargin);
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
                                 , tweaksGeoSql, box2d, extendWithMargin);
        // 2. project the mvt to tile
      mvtgeom = DhString.format( (!hereTile ? "st_setsrid(st_translate(st_scale(st_translate(%1$s, %2$d , %2$d, 0.0), st_makepoint(%3$f,%4$f,1.0) ), %5$f , %6$f, 0.0 ), 3857)"
                                            : "st_setsrid(st_translate(st_scale(st_translate(%1$s, %2$d , %2$d, 0.0), st_makepoint(%3$f,%4$f,1.0) ), %5$f , %6$f, 0.0 ), 4326)")
                                 ,mvtgeom
                                 ,-extendWithMargin/2 // => shift to stretch from tilecenter
                                 ,stretchFactor*(xwidth / (gridsize*extend)), stretchFactor * (ywidth / (gridsize*extend)) * -1 // stretch tile to proj. size
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

    private static int samplingStrengthFromText( String sampling, boolean fiftyOnUnset )
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

    public static SQLQuery buildSamplingTweaksQuery(GetFeaturesByBBoxEvent event, BBox bbox, Map tweakParams, boolean bSortByHashedValue, DataSource dataSource) throws SQLException
    {
     int strength = 0;
     boolean bDistribution  = true,
             bDistribution2 = false,
             bConvertGeo2Geojson = ( mvtTypeRequested(event) == 0 );

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

     boolean bEnsureMode = TweaksSQL.ENSURE.equals( event.getTweakType().toLowerCase() );

     float tblSampleRatio = ( (strength > 0 && bDistribution2) ? TweaksSQL.tableSampleRatio(strength) : -1f);

     final String sCondition = ( ((bEnsureMode && strength == 0) || (tblSampleRatio >= 0.0)) ? "1 = 1" : TweaksSQL.strengthSql(strength,bDistribution)  ),
                  twqry = DhString.format(DhString.format("ST_Intersects(geo, ST_MakeEnvelope(%%.%1$df,%%.%1$df,%%.%1$df,%%.%1$df, 4326) ) and %%s", 14 /*GEOMETRY_DECIMAL_DIGITS*/), bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat(), sCondition );

     final SQLQuery searchQuery = generateSearchQuery(event,dataSource),
                    tweakQuery = new SQLQuery(twqry);

     if( !bEnsureMode || !bConvertGeo2Geojson )
      return generateCombinedQuery(event, tweakQuery, searchQuery , dataSource, bConvertGeo2Geojson, null, tblSampleRatio, bSortByHashedValue );

     /* TweaksSQL.ENSURE and geojson requested (no mvt) */
     boolean bTestTweaksGeoIfNull = false;
     String tweaksGeoSql = clipProjGeom(bbox,"geo");
     tweaksGeoSql = map2MvtGeom( event, bbox, tweaksGeoSql );
     //convert to geojson
     tweaksGeoSql = ( bConvertGeo2Geojson ? DhString.format("replace(ST_AsGeojson(" + getForceMode(event.isForce2D()) + "( %s ),%d),'nan','0')",tweaksGeoSql,GEOMETRY_DECIMAL_DIGITS)
                                          : DhString.format( getForceMode(event.isForce2D()) + "( %s )",tweaksGeoSql ) );

     return generateCombinedQueryTweaks(event, tweakQuery, searchQuery , tweaksGeoSql, bTestTweaksGeoIfNull, dataSource, tblSampleRatio, bSortByHashedValue );
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

    public static SQLQuery buildSimplificationTweaksQuery(GetFeaturesByBBoxEvent event, BBox bbox, Map tweakParams, DataSource dataSource) throws SQLException
    {
     int strength = 0,
         iMerge = 0;
     String tweaksGeoSql = "geo";
     boolean bStrength = true, bTestTweaksGeoIfNull = true, bConvertGeo2Geojson = ( mvtTypeRequested(event) == 0 ), bMvtRequested = !bConvertGeo2Geojson;

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
       tweaksGeoSql = ( bConvertGeo2Geojson ? DhString.format("replace(ST_AsGeojson(" + getForceMode(event.isForce2D()) + "( %s ),%d),'nan','0')",tweaksGeoSql,GEOMETRY_DECIMAL_DIGITS)
                                            : DhString.format( getForceMode(event.isForce2D()) + "( %s )",tweaksGeoSql) );
     }

       final String bboxqry = DhString.format( DhString.format("ST_Intersects(geo, ST_MakeEnvelope(%%.%1$df,%%.%1$df,%%.%1$df,%%.%1$df, 4326) )", 14 /*GEOMETRY_DECIMAL_DIGITS*/), bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat() );

       final SQLQuery searchQuery = generateSearchQuery(event,dataSource);

       if( iMerge == 0 )
        return generateCombinedQueryTweaks(event, new SQLQuery(bboxqry), searchQuery , tweaksGeoSql, bTestTweaksGeoIfNull, dataSource);

       // Merge Algorithm - only using low, med, high

       int minGeoHashLenToMerge = 0,
           minGeoHashLenForLineMerge = 3;

       if      ( strength <= 20 ) { minGeoHashLenToMerge = 7; minGeoHashLenForLineMerge = 7; } //low
       else if ( strength <= 40 ) { minGeoHashLenToMerge = 6; minGeoHashLenForLineMerge = 6; } //lowmed
       else if ( strength <= 60 ) { minGeoHashLenToMerge = 6; minGeoHashLenForLineMerge = 5; } //med
       else if ( strength <= 80 ) {                           minGeoHashLenForLineMerge = 4; } //medhigh

       if( "geo".equals(tweaksGeoSql) ) // formal, just in case
        tweaksGeoSql = ( bConvertGeo2Geojson ? DhString.format("replace(ST_AsGeojson(" + getForceMode(event.isForce2D()) + "( %s ),%d),'nan','0')",tweaksGeoSql,GEOMETRY_DECIMAL_DIGITS)
                                             : DhString.format(getForceMode(event.isForce2D()) + "( %s )",tweaksGeoSql) );

       if( bConvertGeo2Geojson )
        tweaksGeoSql = DhString.format("(%s)::jsonb", tweaksGeoSql);

        SQLQuery query =
         ( iMerge == 1 ? new SQLQuery( DhString.format( TweaksSQL.mergeBeginSql, tweaksGeoSql, minGeoHashLenToMerge, bboxqry ) )
                       : new SQLQuery( DhString.format( TweaksSQL.linemergeBeginSql, /*(event.getClip() ? clipProjGeom(bbox,"geo") : "geo")*/ "geo" , bboxqry ) ));  // use clipped geom as input (?)

       if (searchQuery != null)
       { query.append(" and ");
         query.append(searchQuery);
       }

       if( iMerge == 1 )
        query.append( TweaksSQL.mergeEndSql(bConvertGeo2Geojson) );
       else
       { query.append( DhString.format( TweaksSQL.linemergeEndSql1, minGeoHashLenForLineMerge ) );
         query.append(SQLQuery.selectJson(event));
         query.append( DhString.format( TweaksSQL.linemergeEndSql2, tweaksGeoSql ) );
       }

       query.append("LIMIT ?", event.getLimit());

       return query;
	}




    public static SQLQuery buildEstimateSamplingStrengthQuery( GetFeaturesByBBoxEvent event, BBox bbox, String relTuples )
    {
     int level, tileX, tileY, margin = 0;

     if( event instanceof GetFeaturesByTileEvent )
     { GetFeaturesByTileEvent tevnt = (GetFeaturesByTileEvent) event;
       level = tevnt.getLevel();
       tileX = tevnt.getX();
       tileY = tevnt.getY();
       margin = tevnt.getMargin();
     }
     else
     { final WebMercatorTile tile = getTileFromBbox(bbox);
       level = tile.level;
       tileX = tile.x;
       tileY = tile.y;
     }

     ArrayList<BBox> listOfBBoxes = new ArrayList<BBox>();
     int nrTilesXY = 1 << level;

     for( int dy = -1; dy < 2; dy++ )
      for( int dx = -1; dx < 2; dx++ )
       if( (dy == 0) && (dx == 0) ) listOfBBoxes.add(bbox);  // centerbox, this is already extended by margin
       else if( ((tileY + dy) > 0) && ((tileY + dy) < nrTilesXY) )
        listOfBBoxes.add( WebMercatorTile.forWeb(level, ((nrTilesXY +(tileX + dx)) % nrTilesXY) , (tileY + dy)).getExtendedBBox(margin) );

     int flag = 0;
     StringBuilder sb = new StringBuilder();
     for (BBox b : listOfBBoxes)
      sb.append(DhString.format("%s%s",( flag++ > 0 ? "," : ""),DhString.format( TweaksSQL.requestedTileBoundsSql , b.minLon(), b.minLat(), b.maxLon(), b.maxLat() )));

     String estimateSubSql = ( relTuples == null ? TweaksSQL.estWithPgClass : DhString.format( TweaksSQL.estWithoutPgClass, relTuples ) );

     return new SQLQuery( DhString.format( TweaksSQL.estimateCountByBboxesSql, sb.toString(), estimateSubSql ) );
    }

    public static SQLQuery buildMvtEncapsuledQuery( String spaceId, SQLQuery dataQry, WebMercatorTile mvtTile, HQuad hereTile, BBox eventBbox, int mvtMargin, boolean bFlattend )
    {
      int extend = 4096, buffer = (extend / WebMercatorTile.TileSizeInPixel) * mvtMargin;
      BBox b = ( mvtTile != null ? mvtTile.getBBox(false) : ( hereTile != null ? hereTile.getBoundingBox() : eventBbox) ); // pg ST_AsMVTGeom expects tiles bbox without buffer.

      SQLQuery r = new SQLQuery( DhString.format( hereTile == null ? TweaksSQL.mvtBeginSql : TweaksSQL.hrtBeginSql ,
                                   DhString.format( TweaksSQL.requestedTileBoundsSql , b.minLon(), b.minLat(), b.maxLon(), b.maxLat() ),
                                   (!bFlattend) ? TweaksSQL.mvtPropertiesSql : TweaksSQL.mvtPropertiesFlattenSql,
                                   extend,
                                   buffer )
                               );
      r.append(dataQry);
      r.append( DhString.format( TweaksSQL.mvtEndSql, spaceId ));
      return r;
    }

    /***************************************** TWEAKS END **************************************************/

    public static SQLQuery buildFeaturesQuery(final SearchForFeaturesEvent event, final boolean isIterate, final boolean hasHandle,
                                                 final boolean hasSearch, final long start, DataSource dataSource)
            throws Exception {

        final SQLQuery query = new SQLQuery("SELECT");
        query.append(SQLQuery.selectJson(event));
        query.append(", replace(ST_AsGeojson(" + getForceMode(event.isForce2D()) + "(geo),"+GEOMETRY_DECIMAL_DIGITS+"),'nan','0'), i FROM ${schema}.${table}");
        final SQLQuery searchQuery = generateSearchQuery(event, dataSource);

        if (hasSearch || hasHandle) {
            query.append("WHERE");
        }

        if (hasSearch) {
            query.append(searchQuery);
        }

        if (hasHandle) {
            if (hasSearch) {
                query.append("OFFSET ?", start);
            } else {
                query.append("i > ?", start);
            }
        }

        if (isIterate && !hasSearch) {
            query.append("ORDER BY i");
        }

        query.append("LIMIT ?", event.getLimit());
        return query;
    }

    public static SQLQuery buildFeaturesSortQuery(final SearchForFeaturesOrderByEvent event, DataSource dataSource) throws Exception
    {
        SQLQuery searchQuery = generateSearchQuery(event, dataSource);

        SQLQuery innerQry = IterateSortSQL.innerSortedQry(searchQuery, event.getSort(), event.getPart(), event.getHandle(), event.getLimit());

        final SQLQuery query = new SQLQuery( DhString.format("%s select",IterateSortSQL.pg_hint_plan));
        query.append(SQLQuery.selectJson(event));
        query.append(", replace(ST_AsGeojson(" + getForceMode(event.isForce2D()) + "(geo)," + GEOMETRY_DECIMAL_DIGITS + "),'nan','0'), nxthandle from ( ");
        query.append(innerQry);
        query.append(" ) o");

        return query;
    }

    public static SQLQuery buildGetIterateHandlesQuery( int nrHandles ) throws Exception
    { return IterateSortSQL.getIterateHandles(nrHandles);  }

    public static SQLQuery buildDeleteFeaturesByTagQuery(boolean includeOldStates, SQLQuery searchQuery) {

        final SQLQuery query;

        if (searchQuery != null) {
            query = new SQLQuery("DELETE FROM ${schema}.${table} WHERE");
            query.append(searchQuery);
        } else {
            query = new SQLQuery("TRUNCATE ${schema}.${table}");
        }

        if (searchQuery != null && includeOldStates)
            query.append(" RETURNING jsondata->'id' as id, replace(ST_AsGeojson(ST_Force3D(geo),"+GEOMETRY_DECIMAL_DIGITS+"),'nan','0') as geometry");

        return query;
    }

    public static SQLQuery buildHistoryQuery(IterateHistoryEvent event) {
        SQLQuery query = new SQLQuery(
                "SELECT vid," +
                "   jsondata->>'id' as id," +
                "   jsondata || jsonb_strip_nulls(jsonb_build_object('geometry',ST_AsGeoJSON(geo)::jsonb)) As feature,"+
                "   ( CASE " +
                "      WHEN (COALESCE((jsondata->'properties'->'@ns:com:here:xyz'->>'deleted')::boolean, false) IS true) THEN 'DELETED'" +
                "      WHEN (jsondata->'properties'->'@ns:com:here:xyz'->'puuid' IS NULL" +
                "          AND" +
                "          (COALESCE((jsondata->'properties'->'@ns:com:here:xyz'->>'deleted')::boolean, false) IS NOT true)" +
                "      ) THEN 'INSERTED'" +
                "      ELSE 'UPDATED' " +
                "   END) as operation," +
                "   jsondata->'properties'->'@ns:com:here:xyz'->>'version' as version" +
                "       FROM ${schema}.${hsttable}" +
                "           WHERE 1=1");

        if (event.getPageToken() != null) {
            query.append(
               "   AND vid > ?",event.getPageToken());
        }

        if(event.getStartVersion() != 0) {
            query.append("  AND jsondata->'properties'->'@ns:com:here:xyz'->'version' >= to_jsonb(?::numeric)",event.getStartVersion());
        }

        if(event.getEndVersion() != 0)
            query.append("  AND jsondata->'properties'->'@ns:com:here:xyz'->'version' <= to_jsonb(?::numeric)", event.getEndVersion());

        query.append(" ORDER BY jsondata->'properties'->'@ns:com:here:xyz'->'version' , " +
                "jsondata->>'id'");

        if(event.getLimit() != 0)
            query.append("LIMIT ?", event.getLimit());

        return query;
    }

    public static SQLQuery buildSquashHistoryQuery(IterateHistoryEvent event){
        SQLQuery query = new SQLQuery(
                "SELECT distinct ON (jsondata->>'id') jsondata->>'id'," +
                "   jsondata->>'id' as id," +
                "   jsondata || jsonb_strip_nulls(jsonb_build_object('geometry',ST_AsGeoJSON(geo)::jsonb)) As feature,"+
                "   ( CASE " +
                "      WHEN (COALESCE((jsondata->'properties'->'@ns:com:here:xyz'->>'deleted')::boolean, false) IS true) THEN 'DELETED'" +
                "      WHEN (jsondata->'properties'->'@ns:com:here:xyz'->'puuid' IS NULL" +
                "          AND" +
                "          (COALESCE((jsondata->'properties'->'@ns:com:here:xyz'->>'deleted')::boolean, false) IS NOT true)" +
                "      ) THEN 'INSERTED'" +
                "      ELSE 'UPDATED' " +
                "   END) as operation, " +
                "   jsondata->'properties'->'@ns:com:here:xyz'->>'version' as version" +
                "       FROM ${schema}.${hsttable}" +
                "           WHERE 1=1");

        if (event.getPageToken() != null) {
            query.append(
                    "   AND jsondata->>'id' > ?",event.getPageToken());
        }

        if(event.getStartVersion() != 0) {
            query.append(
                "  AND jsondata->'properties'->'@ns:com:here:xyz'->'version' >= to_jsonb(?::numeric)",event.getStartVersion());
        }

        if(event.getEndVersion() != 0)
            query.append(
                "  AND jsondata->'properties'->'@ns:com:here:xyz'->'version' <= to_jsonb(?::numeric)", event.getEndVersion());

        query.append(
                "   order by jsondata->>'id'," +
                "jsondata->'properties'->'@ns:com:here:xyz'->'version' DESC");

        if(event.getLimit() != 0)
            query.append("LIMIT ?", event.getLimit());

        return query;
    }

    public static SQLQuery buildLatestHistoryQuery(IterateFeaturesEvent event) {
        SQLQuery query = new SQLQuery("select jsondata#-'{properties,@ns:com:here:xyz,lastVersion}' as jsondata, geo, id " +
                "FROM(" +
                "   select distinct ON (jsondata->>'id') jsondata->>'id' as id," +
                "   jsondata->'properties'->'@ns:com:here:xyz'->'deleted' as deleted," +
                "   jsondata," +
                "   replace(ST_AsGeojson(ST_Force3D(geo),"+GEOMETRY_DECIMAL_DIGITS+"),'nan','0') as geo"+
                "       FROM ${schema}.${hsttable}" +
                "   WHERE 1=1" );

        if (event.getHandle() != null) {
            query.append(
                    "   AND jsondata->>'id' > ?",event.getHandle());
        }

        query.append(
                "   AND((" +
                "       jsondata->'properties'->'@ns:com:here:xyz'->'version' <= to_jsonb(?::numeric)", event.getV());
        query.append(
                "       AND " +
                "       jsondata->'properties'->'@ns:com:here:xyz'->'version' > '0'::jsonb "+
                "   )"+
                "OR( "+
                "       jsondata->'properties'->'@ns:com:here:xyz'->'lastVersion' <= to_jsonb(?::numeric)", event.getV());
        query.append(
                "       AND " +
                "       jsondata->'properties'->'@ns:com:here:xyz'->'version' = '0'::jsonb " +
                "))");
        query.append(
                "   order by jsondata->>'id'," +
                        "jsondata->'properties'->'@ns:com:here:xyz'->'version' DESC ");
        query.append(
                ")A WHERE deleted IS NULL  ");
        if(event.getLimit() != 0)
            query.append("LIMIT ?", event.getLimit());

        return query;
    }

    private static class IdxManual {
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public Map<String, Boolean> searchableProperties;
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public List<List<Object>> sortableProperties;

        IdxManual( Map<String, Boolean> searchableProperties, List<List<Object>> sortableProperties ){
          this.searchableProperties = searchableProperties;
          this.sortableProperties = sortableProperties;
          if(this.searchableProperties != null) {
              //Remove entries with null values
              Map<String, Boolean> clearedSearchableProperties = new HashMap<>(searchableProperties);
              for (String property : searchableProperties.keySet()) {
                  if (searchableProperties.get(property) == null)
                      clearedSearchableProperties.remove(property);
              }
              this.searchableProperties = clearedSearchableProperties;
          }
        }
    }

    private static String _buildSearchablePropertiesUpsertQuery(Space spaceDefinition, ModifySpaceEvent.Operation operation, String schema, String table) throws SQLException
    {
        Map<String, Boolean> searchableProperties = spaceDefinition.getSearchableProperties();
        List<List<Object>> sortableProperties = spaceDefinition.getSortableProperties();
        Boolean enableAutoIndexing = spaceDefinition.isEnableAutoSearchableProperties();

        String idx_manual_json;

        try{
             idx_manual_json = (new ObjectMapper()).writeValueAsString( new IdxManual(searchableProperties, sortableProperties) );
        }
        catch (JsonProcessingException e)  { throw new SQLException("_buildSearchablePropertiesUpsertQuery", e); }

        idx_manual_json = ( idx_manual_json == null ? "null" : DhString.format("'%s'::jsonb", idx_manual_json ));

           /* update xyz_idx_status table with searchabelProperties information */
        String query = "INSERT INTO  "+IDX_STATUS_TABLE+" as x_s (spaceid,schem,idx_creation_finished,idx_manual "
                        + (enableAutoIndexing != null ? ",auto_indexing) ": ") ")
                        + "		VALUES ('" + table + "', '" + schema + "', false, " + idx_manual_json + (enableAutoIndexing != null ? ","+enableAutoIndexing : " ")+") "
                        + "ON CONFLICT (spaceid) DO "
                        + "		UPDATE SET schem='" + schema + "', "
                        + "    			idx_manual = " + idx_manual_json + ", "
                        + "				idx_creation_finished = false "
                        + (enableAutoIndexing != null ? " ,auto_indexing = " + enableAutoIndexing : "")
                        + "		WHERE x_s.spaceid = '" + table + "' AND x_s.schem='"+schema+"'";

        return query;
    }

    public static SQLQuery buildSearchablePropertiesUpsertQuery(Space spaceDefinition, ModifySpaceEvent.Operation operation,
                                                                   String schema, String table) throws SQLException
    { return new SQLQuery( _buildSearchablePropertiesUpsertQuery(spaceDefinition, operation,schema, table)  );  }



    public static SQLQuery buildDeleteIDXConfigEntryQuery(String schema, String table){
        final SQLQuery query = new SQLQuery("");
        query.append(
                "DO $$ " +
                        "    BEGIN  "+
                        "        IF EXISTS " +
                        "            (SELECT 1 " +
                        "              FROM   information_schema.tables " +
                        "              WHERE  table_schema = 'xyz_config'" +
                        "              AND    table_name = 'xyz_idxs_status')" +
                        "        THEN" +
                        "            delete from "+IDX_STATUS_TABLE+" where spaceid = '"+table+"' AND schem='"+schema+"';" +
                        "        END IF ;" +
                        "    END" +
                        "$$ ;");
        return query;
    }
/** ###################################################################################### */
    private static SQLQuery generatePropertiesQuery(PropertiesQuery properties) {
        if (properties == null || properties.size() == 0) {
            return null;
        }
        // TODO: This is only a hot-fix for the connector. The issue is caused in the service and the code below will be removed after the next XYZ Hub deployment
        if (properties.get(0).size() == 0 || properties.get(0).size() == 1 && properties.get(0).get(0) == null) {
            return null;
        }
        // List with the outer OR combined statements
        List<SQLQuery> disjunctionQueries = new ArrayList<>();
        properties.forEach(conjunctions -> {

            // List with the AND combined statements
            final List<SQLQuery> conjunctionQueries = new ArrayList<>();
            conjunctions.forEach(propertyQuery -> {

                // List with OR combined statements for one property key
                final List<SQLQuery> keyDisjunctionQueries = new ArrayList<>();
                propertyQuery.getValues().forEach(v -> {
                    final String psqlOperation;
                    PropertyQuery.QueryOperation op = propertyQuery.getOperation();

                    String  key = propertyQuery.getKey(),
                            value = SQLQuery.getValue(v, op, key);
                    SQLQuery q = SQLQuery.createKey(key);

                    if(v == null){
                        //Overrides all operations e.g: p.foo=lte=.null => p.foo=.null
                        //>> [not] (jsondata->?->? is not null and jsondata->?->? != 'null'::jsonb )
                        SQLQuery q1 = new SQLQuery( op.equals(PropertyQuery.QueryOperation.NOT_EQUALS) ? "((" : "not ((" );
                        q1.append(q);
                        q1.append("is not null");

                        if(! ("id".equals(key) || "geometry.type".equals(key)) )
                        { q1.append("and"); q1.append(q); q1.append("!= 'null'::jsonb" ); }

                        q1.append("))");
                        q = q1;
                    }else{
                        psqlOperation = SQLQuery.getOperation(op);
                        q.append(new SQLQuery(psqlOperation + (value == null ? "" : value), v));
                    }
                    keyDisjunctionQueries.add(q);
                });
                conjunctionQueries.add(SQLQuery.join(keyDisjunctionQueries, "OR", true));
            });
            disjunctionQueries.add(SQLQuery.join(conjunctionQueries, "AND", false));
        });
        return SQLQuery.join(disjunctionQueries, "OR", false);
    }

    private static SQLQuery generateTagsQuery(TagsQuery tags)
            throws SQLException {
        if (tags == null || tags.size() == 0) {
            return null;
        }

        SQLQuery query;
        StringBuilder andQuery = new StringBuilder("jsondata->'properties'->'@ns:com:here:xyz'->'tags' ??& ?");
        boolean hasAnd = tags.get(0).size() > 1;

        for (int i = 1; i < tags.size(); i++) {
            if (tags.get(i).size() > 1) {
                hasAnd = true;
            }
            andQuery.append(" OR jsondata->'properties'->'@ns:com:here:xyz'->'tags' ??& ?");
        }

        if (!hasAnd) {
            String[] orList = new String[tags.size()];
            for (int i = 0; i < tags.size(); i++) {
                orList[i] = tags.get(i).get(0);
            }
            query = new SQLQuery(" (jsondata->'properties'->'@ns:com:here:xyz'->'tags' ??| ?)", (Object) orList);
        } else {
            query = new SQLQuery("(" + andQuery + ")");
            for (TagList tag : tags) {
                query.addParameter(tag.toArray(new String[0]));
            }
        }

        return query;
    }

    private static SQLQuery generateCombinedQueryTweaks(SearchForFeaturesEvent event, SQLQuery indexedQuery, SQLQuery secondaryQuery, String tweaksgeo, boolean bTestTweaksGeoIfNull, DataSource dataSource, float sampleRatio, boolean bSortByHashedValue) throws SQLException
    {
     String tSample = ( sampleRatio <= 0.0 ? "" : DhString.format("tablesample system(%.6f) repeatable(499)", 100.0 * sampleRatio) );

     final SQLQuery query = new SQLQuery();

     query.append("select * from ( SELECT");

     query.append(SQLQuery.selectJson(event));

     query.append(DhString.format(",%s as geo",tweaksgeo));

     query.append( DhString.format("FROM ${schema}.${table} %s WHERE",tSample) );
     query.append(indexedQuery);

     if( secondaryQuery != null )
     { query.append(" and ");
       query.append(secondaryQuery);
     }

     if( bSortByHashedValue )
      query.append( DhString.format( " order by %s ", TweaksSQL.distributionFunctionIndexExpression() ) );

     query.append(DhString.format(" ) tw where %s ", bTestTweaksGeoIfNull ? "geo is not null" : "1 = 1" ) );

     query.append("LIMIT ?", event.getLimit());
     return query;
    }

    private static SQLQuery generateCombinedQueryTweaks(SearchForFeaturesEvent event, SQLQuery indexedQuery, SQLQuery secondaryQuery, String tweaksgeo, boolean bTestTweaksGeoIfNull, DataSource dataSource) throws SQLException
    { return generateCombinedQueryTweaks(event, indexedQuery, secondaryQuery, tweaksgeo, bTestTweaksGeoIfNull, dataSource, -1.0f, false );  }

    private static SQLQuery generateCombinedQuery(SearchForFeaturesEvent event, SQLQuery indexedQuery, SQLQuery secondaryQuery, DataSource dataSource, boolean bConvertGeo2Geojson, String h3Index, float sampleRatio, boolean bSortByHashedValue ) throws SQLException
    {
        String tSample = ( sampleRatio <= 0.0 ? "" : DhString.format("tablesample system(%.6f) repeatable(499)", 100.0 * sampleRatio) );

        final SQLQuery query = new SQLQuery();

        if(h3Index != null)
            query.append("WITH h AS (SELECT h3ToGeoBoundaryDeg( ('x' || '"+h3Index+"' )::bit(60)::bigint ) as hexbin )");

        query.append("SELECT ");

        query.append(SQLQuery.selectJson(event));
        query.append(",");
        query.append(geometrySelectorForEvent( event, bConvertGeo2Geojson, indexedQuery));

        if(h3Index != null)
            query.append(DhString.format("FROM ${schema}.${table} %s,h WHERE",tSample));
        else
            query.append(DhString.format("FROM ${schema}.${table} %s WHERE",tSample));

        query.append(indexedQuery);

        if( secondaryQuery != null )
        { query.append(" and ");
            query.append(secondaryQuery);
        }

        if( bSortByHashedValue )
         query.append( DhString.format( " order by %s ", TweaksSQL.distributionFunctionIndexExpression() ) );

        query.append("LIMIT ?", event.getLimit());
        return query;
    }

    private static SQLQuery generateCombinedQuery(SearchForFeaturesEvent event, SQLQuery indexedQuery, SQLQuery secondaryQuery, DataSource dataSource, boolean bConvertGeo2Geojson, String h3Index ) throws SQLException
    { return generateCombinedQuery(event,indexedQuery,secondaryQuery,dataSource,bConvertGeo2Geojson,h3Index, -1.0f, false); }

    /**
     * Returns the query, which will contains the geometry object.
     */
    private static SQLQuery geometrySelectorForEvent(final QueryEvent event, boolean bGeoJson, SQLQuery indexedQuery) {
        String forceMode = null;
        boolean isClipped = false;

        if(event instanceof GetFeaturesByBBoxEvent){
            isClipped = ((GetFeaturesByBBoxEvent)event).getClip();
            forceMode = getForceMode(((GetFeaturesByBBoxEvent)event).isForce2D());
        }
        else if(event instanceof  GetFeaturesByGeometryEvent){
            isClipped = ((GetFeaturesByGeometryEvent)event).getClip();
            forceMode = getForceMode(((GetFeaturesByGeometryEvent)event).isForce2D());
        }

        if(!isClipped){
            return (bGeoJson ?
                    new SQLQuery("replace(ST_AsGeojson("+forceMode+"(geo),?::INTEGER),'nan','0') as geo", GEOMETRY_DECIMAL_DIGITS)
                    : new SQLQuery(forceMode+"(geo) as geo"));
        }else{
            if(event instanceof GetFeaturesByBBoxEvent){
                final BBox bbox = ((GetFeaturesByBBoxEvent)event).getBbox();
                String geoSqlAttrib = (bGeoJson ? DhString.format("replace(ST_AsGeoJson(ST_Intersection(ST_MakeValid(geo),ST_MakeEnvelope(?,?,?,?,4326)),%d),'nan','0') as geo", GEOMETRY_DECIMAL_DIGITS)
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
                return new SQLQuery("replace(ST_AsGeoJson(ST_Intersection(ST_MakeValid(geo),"+wktGeom+"),?::INTEGER),'nan','0') as geo", GEOMETRY_DECIMAL_DIGITS);
            }
        }
        //Should not happen (currently only used with GetFeaturesByBBoxEvent / GetFeaturesByGeometryEvent)
        return new SQLQuery("ST_AsGeojson(geo) as geo");
    }

    protected static SQLQuery generateSearchQuery(final QueryEvent event, final DataSource dataSource)
            throws SQLException {
        final SQLQuery propertiesQuery = generatePropertiesQuery(event.getPropertiesQuery());
        final SQLQuery tagsQuery = generateTagsQuery(event.getTags());

        return SQLQuery.join("AND", propertiesQuery, tagsQuery);
    }

    protected static SQLQuery generateLoadOldFeaturesQuery(final String[] idsToFetch, final DataSource dataSource)
            throws SQLException {
        return new SQLQuery("SELECT jsondata, replace(ST_AsGeojson(ST_Force3D(geo),"+GEOMETRY_DECIMAL_DIGITS+"),'nan','0') FROM ${schema}.${table} WHERE jsondata->>'id' = ANY(?)", (Object) idsToFetch);
    }

    protected static SQLQuery generateLoadExistingIdsQuery(final String[] idsToFetch, final DataSource dataSource)
        throws SQLException {
      return new SQLQuery("SELECT jsondata->>'id' id FROM ${schema}.${table} WHERE jsondata->>'id' = ANY(?)", (Object) idsToFetch);
    }

    protected static SQLQuery generateIDXStatusQuery(final String space){
        return new SQLQuery("SELECT idx_available FROM "+IDX_STATUS_TABLE+" WHERE spaceid=? AND count >=?", space, BIG_SPACE_THRESHOLD);
    }

    protected static String insertStmtSQL(final String schema, final String table, boolean withDeletedColumn) {
        String insertStmtSQL ="INSERT INTO ${schema}.${table} (jsondata, geo" + (withDeletedColumn ? ", deleted" : "")
            + ") VALUES(?::jsonb, ST_Force3D(ST_GeomFromWKB(?,4326))" + (withDeletedColumn ? ", ?" : "") + ")";
        return SQLQuery.replaceVars(insertStmtSQL, schema, table);
    }

    protected static String insertWithoutGeometryStmtSQL(final String schema, final String table, boolean withDeletedColumn) {
        String insertWithoutGeometryStmtSQL = "INSERT INTO ${schema}.${table} (jsondata, geo" + (withDeletedColumn ? ", deleted" : "")
            + ") VALUES(?::jsonb, NULL" + (withDeletedColumn ? ", ?" : "") + ")";
        return SQLQuery.replaceVars(insertWithoutGeometryStmtSQL, schema, table);
    }

    protected static String updateStmtSQL(final String schema, final String table, final boolean handleUUID, boolean withDeletedColumn) {
        String updateStmtSQL = "UPDATE ${schema}.${table} SET jsondata = ?::jsonb, geo=ST_Force3D(ST_GeomFromWKB(?,4326))"
            + (withDeletedColumn ? ", deleted=?" : "") + " WHERE jsondata->>'id' = ?";
        if (handleUUID)
            updateStmtSQL += " AND jsondata->'properties'->'@ns:com:here:xyz'->>'uuid' = ?";
        return SQLQuery.replaceVars(updateStmtSQL, schema, table);
    }

    protected static String updateWithoutGeometryStmtSQL(final String schema, final String table, final boolean handleUUID, boolean withDeletedColumn) {
        String updateWithoutGeometryStmtSQL = "UPDATE ${schema}.${table} SET  jsondata = ?::jsonb, geo=NULL"
            + (withDeletedColumn ? ", deleted=?" : "") + " WHERE jsondata->>'id' = ?";
        if (handleUUID)
            updateWithoutGeometryStmtSQL += " AND jsondata->'properties'->'@ns:com:here:xyz'->>'uuid' = ?";
        return SQLQuery.replaceVars(updateWithoutGeometryStmtSQL, schema, table);
    }

    protected static String deleteStmtSQL(final String schema, final String table, final boolean handleUUID){
        String deleteStmtSQL = "DELETE FROM ${schema}.${table} WHERE jsondata->>'id' = ?";
        if(handleUUID) {
            deleteStmtSQL += " AND jsondata->'properties'->'@ns:com:here:xyz'->>'uuid' = ?";
        }
        return SQLQuery.replaceVars(deleteStmtSQL, schema, table);
    }

    protected static String versionedDeleteStmtSQL(final String schema, final String table, final boolean handleUUID){
        /** Use Update instead of Delete to inject a version. The delete gets performed afterwards from the trigger behind. */

        String updateStmtSQL = "UPDATE  ${schema}.${table} "
            +"SET jsondata = jsonb_set( jsondata, '{properties,@ns:com:here:xyz}', "
                +"( (jsondata->'properties'->'@ns:com:here:xyz')::jsonb "
                +"|| format('{\"uuid\": \"%s_deleted\"}',jsondata->'properties'->'@ns:com:here:xyz'->>'uuid')::jsonb ) "
                +"|| format('{\"version\": %s}', ? )::jsonb "
                +"|| format('{\"updatedAt\": %s}', (extract(epoch from now()) * 1000)::bigint )::jsonb "
                +"|| '{\"deleted\": true }'::jsonb) "
                +"where jsondata->>'id' = ? ";
        if(handleUUID) {
            updateStmtSQL += " AND jsondata->'properties'->'@ns:com:here:xyz'->>'uuid' = ?";
        }
        return SQLQuery.replaceVars(updateStmtSQL, schema, table);
    }

    public static String deleteOldHistoryEntries(final String schema, final String table, long maxAllowedVersion){
        /** Delete rows which have a too old version - only used if maxVersionCount is set */

        String deleteOldHistoryEntriesSQL =
                "DELETE FROM ${schema}.${table} t " +
                "USING (" +
                "   SELECT vid " +
                "   FROM   ${schema}.${table} " +
                "   WHERE  1=1 " +
                "     AND jsondata->'properties'->'@ns:com:here:xyz'->'version' = '0'::jsonb " +
                "     AND jsondata->>'id' IN ( " +
                "     SELECT jsondata->>'id' FROM ${schema}.${table} " +
                "        WHERE 1=1    " +
                "        AND (jsondata->'properties'->'@ns:com:here:xyz'->'version' <= '"+maxAllowedVersion+"'::jsonb " +
                "        AND jsondata->'properties'->'@ns:com:here:xyz'->'version' > '0'::jsonb)" +
                ")" +
                "   ORDER  BY vid" +
                "   FOR    UPDATE" +
                "   ) del " +
                "WHERE  t.vid = del.vid;";

        return SQLQuery.replaceVars(deleteOldHistoryEntriesSQL, schema, table);
    }

    public static String flagOutdatedHistoryEntries(final String schema, final String table, long maxAllowedVersion){
        /** Set version=0 for objects which are too old - only used if maxVersionCount is set */

        String flagOutdatedHistoryEntries =
                "UPDATE ${schema}.${table} " +
                "SET jsondata = jsonb_set(jsondata,'{properties,@ns:com:here:xyz}', ( " +
                "   (jsondata->'properties'->'@ns:com:here:xyz')::jsonb) " +
                "   || format('{\"lastVersion\" : %s }',(jsondata->'properties'->'@ns:com:here:xyz'->'version'))::jsonb"+
                "   || '{\"version\": 0}'::jsonb" +
                ") " +
                "    WHERE " +
                "1=1" +
                "AND jsondata->'properties'->'@ns:com:here:xyz'->'version' <= '"+maxAllowedVersion+"'::jsonb " +
                "AND jsondata->'properties'->'@ns:com:here:xyz'->'version' > '0'::jsonb;";

        return SQLQuery.replaceVars(flagOutdatedHistoryEntries, schema, table);
    }

    public static String deleteHistoryEntriesWithDeleteFlag(final String schema, final String table){
        /** Remove deleted objects with version 0 - only used if maxVersionCount is set */
        String deleteHistoryEntriesWithDeleteFlag =
                "DELETE FROM ${schema}.${table} " +
                "WHERE " +
                "   1=1" +
                "   AND jsondata->'properties'->'@ns:com:here:xyz'->'version' = '0'::jsonb " +
                "   AND jsondata->'properties'->'@ns:com:here:xyz'->'deleted' = 'true'::jsonb;";

        return SQLQuery.replaceVars(deleteHistoryEntriesWithDeleteFlag, schema, table);
    }

    protected static String deleteIdArrayStmtSQL(final String schema, final String table, final boolean handleUUID){
        String deleteIdArrayStmtSQL = "DELETE FROM ${schema}.${table} WHERE jsondata->>'id' = ANY(?) ";
        if(handleUUID) {
            deleteIdArrayStmtSQL += " AND jsondata->'properties'->'@ns:com:here:xyz'->>'uuid' = ANY(?)";
        }
        return SQLQuery.replaceVars(deleteIdArrayStmtSQL, schema, table);
    }

    protected static String[] deleteHistoryTriggerSQL(final String schema, final String table){
        String[] sqls= new String[2];
        /** Old naming */
        String oldDeleteHistoryTriggerSQL = "DROP TRIGGER IF EXISTS TR_"+table.replaceAll("-","_")+"_HISTORY_WRITER ON  ${schema}.${table};";
        /** New naming */
        String deleteHistoryTriggerSQL = "DROP TRIGGER IF EXISTS \"TR_"+table.replaceAll("-","_")+"_HISTORY_WRITER\" ON  ${schema}.${table};";

        sqls[0] = SQLQuery.replaceVars(oldDeleteHistoryTriggerSQL, schema, table);
        sqls[1] = SQLQuery.replaceVars(deleteHistoryTriggerSQL, schema, table);
        return sqls;
    }

    protected static String addHistoryTriggerSQL(final String schema, final String table, final Integer maxVersionCount, final boolean compactHistory, final boolean isEnableGlobalVersioning){
        String triggerSQL = "";
        String triggerFunction = "xyz_trigger_historywriter";
        String triggerActions = "UPDATE OR DELETE ON";
        String tiggerEvent = "BEFORE";

        if(isEnableGlobalVersioning == true){
            triggerFunction = "xyz_trigger_historywriter_versioned";
            tiggerEvent = "AFTER";
            triggerActions = "INSERT OR UPDATE ON";
        }else{
            if(compactHistory == false){
                triggerFunction = "xyz_trigger_historywriter_full";
                triggerActions = "INSERT OR UPDATE OR DELETE ON";
            }
        }

        triggerSQL = "CREATE TRIGGER \"TR_"+table.replaceAll("-","_")+"_HISTORY_WRITER\" " +
                tiggerEvent+" "+triggerActions+" ${schema}.${table} " +
                " FOR EACH ROW " +
                "EXECUTE PROCEDURE "+triggerFunction;
        triggerSQL+=
                maxVersionCount == null ? "()" : "('"+maxVersionCount+"')";

        return SQLQuery.replaceVars(triggerSQL, schema, table);
    }

    public static String getForceMode(boolean isForce2D) {
      return isForce2D ? "ST_Force2D" : "ST_Force3D";
    }
}
