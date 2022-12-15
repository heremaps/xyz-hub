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
package com.here.xyz.psql;

import static com.here.xyz.events.GetFeaturesByTileEvent.ResponseType.GEO_JSON;

import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.events.Event;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.GetFeaturesByTileEvent.ResponseType;
import com.here.xyz.events.GetHistoryStatisticsEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.IterateHistoryEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.QueryEvent;
import com.here.xyz.models.geojson.HQuad;
import com.here.xyz.models.geojson.WebMercatorTile;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.psql.config.PSQLConfig;
import com.here.xyz.psql.factory.H3SQL;
import com.here.xyz.psql.factory.QuadbinSQL;
import com.here.xyz.psql.factory.TweaksSQL;
import com.here.xyz.psql.query.GetFeaturesByBBox;
import com.here.xyz.psql.query.ModifySpace;
import com.here.xyz.psql.query.SearchForFeatures;
import com.here.xyz.psql.tools.DhString;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

public class SQLQueryBuilder {
    public static final long GEOMETRY_DECIMAL_DIGITS = 8;
  private static final Integer BIG_SPACE_THRESHOLD = 10000;

    public static SQLQuery buildGetStatisticsQuery(Event event, PSQLConfig config, boolean historyMode) {
        String function;
        if(event instanceof GetHistoryStatisticsEvent)
            function = "xyz_statistic_history";
        else
            function = "xyz_statistic_space";
        final String schema = config.getDatabaseSettings().getSchema();
        final String table = config.readTableFromEvent(event) + (!historyMode ? "" : "_hst");

        return new SQLQuery("SELECT * from " + schema + "."+function+"('" + schema + "','" + table + "')");
    }

    public static SQLQuery buildGetNextVersionQuery(String table) {
        return new SQLQuery("SELECT nextval('${schema}.\"" + table.replaceAll("-","_") + "_hst_seq\"')");
    }

  public static SQLQuery buildGetFeaturesByBBoxQuery(final GetFeaturesByBBoxEvent event)
        throws SQLException{
        final BBox bbox = event.getBbox();

        final SQLQuery geoQuery = new SQLQuery("ST_Intersects(geo, ST_MakeEnvelope(?, ?, ?, ?, 4326))",
                bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat());

        return generateCombinedQuery(event, geoQuery);
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
            Map<String, Object> clusteringParams) {

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

        final SQLQuery searchQuery = generateSearchQuery(event);

        String aggField = (statisticalPropertyProvided ? "jsonb_set('{}'::jsonb, ? , agg::jsonb)::json" : "agg");

        final SQLQuery query = new SQLQuery(DhString.format(H3SQL.h3sqlBegin, h3res,
                !h3cflip ? "st_centroid(geo)" : "geo",
                DhString.format( (getResponseType(event) == GEO_JSON ? "st_asgeojson( %1$s, 7 )::json" : "(%1$s)" ), (h3cflip ? "st_centroid(geo)" : clippedGeo) ),
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

        if (event.getPropertiesQuery() != null) {
            propQuery = generatePropertiesQuery(event);

            if (propQuery != null) {
                propQuerySQL = propQuery.text();
                for (Object param : propQuery.parameters()) {
                    propQuerySQL = propQuerySQL.replaceFirst("\\?", "'" + param + "'");
                }
            }
        }
        return QuadbinSQL.generateQuadbinClusteringSQL(config.getDatabaseSettings().getSchema(), config.readTableFromEvent(event), relResolution, countMode, propQuerySQL, tile, bbox, isTileRequest, clippedOnBbox, noBuffer, getResponseType(event) == GEO_JSON);
    }

    /***************************************** CLUSTERING END **************************************************/

    /***************************************** TWEAKS **************************************************/

    public static ResponseType getResponseType(GetFeaturesByBBoxEvent event) {
      if (event instanceof GetFeaturesByTileEvent)
        return ((GetFeaturesByTileEvent) event).getResponseType();
      return GEO_JSON;
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

    public static SQLQuery buildSamplingTweaksQuery(GetFeaturesByBBoxEvent event, BBox bbox, Map tweakParams, boolean bSortByHashedValue) throws SQLException
    {
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

    public static SQLQuery buildSimplificationTweaksQuery(GetFeaturesByBBoxEvent event, BBox bbox, Map tweakParams) throws SQLException
    {
     int strength = 0,
         iMerge = 0;
     String tweaksGeoSql = "geo";
     boolean bStrength = true, bTestTweaksGeoIfNull = true, bConvertGeo2Geojson = getResponseType(event) == GEO_JSON, bMvtRequested = !bConvertGeo2Geojson;

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

       if( iMerge == 0 )
        return generateCombinedQueryTweaks(event, new SQLQuery(bboxqry), tweaksGeoSql, bTestTweaksGeoIfNull);

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

      final SQLQuery searchQuery = generateSearchQuery(event);
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
      //TODO: The following is a workaround for backwards-compatibility and can be removed after completion of refactoring
      dataQry.replaceUnnamedParameters();
      dataQry.replaceFragments();
      dataQry.replaceNamedParameters();
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

  /** ###################################################################################### */

    private static SQLQuery generatePropertiesQuery(QueryEvent event) {
      return SearchForFeatures.generatePropertiesQueryBWC(event);
    }

    private static SQLQuery generateCombinedQueryTweaks(GetFeaturesByBBoxEvent event, SQLQuery indexedQuery, String tweaksgeo, boolean bTestTweaksGeoIfNull, float sampleRatio, boolean bSortByHashedValue)
    {
      SQLQuery searchQuery = generateSearchQuery(event);
     String tSample = ( sampleRatio <= 0.0 ? "" : DhString.format("tablesample system(%.6f) repeatable(499)", 100.0 * sampleRatio) );

     final SQLQuery query = new SQLQuery("select * from ( SELECT");

     query.append(SQLQuery.selectJson(event));

     query.append(DhString.format(",%s as geo",tweaksgeo));

     query.append( DhString.format("FROM ${schema}.${table} %s WHERE",tSample) );
     query.append(indexedQuery);

     if( searchQuery != null )
     { query.append(" and ");
       query.append(searchQuery);
     }

     if( bSortByHashedValue )
      query.append( DhString.format( " order by %s ", TweaksSQL.distributionFunctionIndexExpression() ) );

     query.append(DhString.format(" ) tw where %s ", bTestTweaksGeoIfNull ? "geo is not null" : "1 = 1" ) );

     query.append("LIMIT ?", event.getLimit());
     return query;
    }

    private static SQLQuery generateCombinedQueryTweaks(GetFeaturesByBBoxEvent event, SQLQuery indexedQuery, String tweaksgeo, boolean bTestTweaksGeoIfNull)
    { return generateCombinedQueryTweaks(event, indexedQuery, tweaksgeo, bTestTweaksGeoIfNull, -1.0f, false );  }

  private static SQLQuery generateCombinedQuery(GetFeaturesByBBoxEvent event, SQLQuery indexedQuery) {
      return GetFeaturesByBBox.generateCombinedQueryBWC(event, indexedQuery);
  }


  protected static SQLQuery generateSearchQuery(final QueryEvent event) {
      return SearchForFeatures.generateSearchQueryBWC(event);
  }

    protected static SQLQuery generateLoadOldFeaturesQuery(ModifyFeaturesEvent event, final String[] idsToFetch) {
        return new SQLQuery("SELECT jsondata, replace(ST_AsGeojson(ST_Force3D(geo),"+GEOMETRY_DECIMAL_DIGITS+"),'nan','0') FROM ${schema}.${table} WHERE " + buildIdFragment(event) + " = ANY(?)", (Object) idsToFetch);
    }

    private static String buildIdFragment(ContextAwareEvent event) {
      return DatabaseHandler.readVersionsToKeep(event) < 1 ? "jsondata->>'id'" : "id";
    }

    protected static SQLQuery generateIDXStatusQuery(final String space){
        return new SQLQuery("SELECT idx_available FROM "+ ModifySpace.IDX_STATUS_TABLE+" WHERE spaceid=? AND count >=?", space, BIG_SPACE_THRESHOLD);
    }

  protected static SQLQuery buildMultiModalInsertStmtQuery(DatabaseHandler dbHandler, ModifyFeaturesEvent event) {
      return new SQLQuery("SELECT xyz_write_versioned_modification_operation(#{id}, #{version}, #{operation}, #{jsondata}, #{geo}, "
          + "#{schema}, #{table}, #{concurrencyCheck})")
          .withNamedParameter("schema", dbHandler.config.getDatabaseSettings().getSchema())
          .withNamedParameter("table", dbHandler.config.readTableFromEvent(event))
          .withNamedParameter("concurrencyCheck", event.getEnableUUID());
  }

  protected static SQLQuery buildInsertStmtQuery(DatabaseHandler dbHandler, ModifyFeaturesEvent event) {
    //NOTE: The following is a temporary implementation for backwards compatibility for old table structures
    boolean oldTableStyle = DatabaseHandler.readVersionsToKeep(event) < 1;
    boolean withDeletedColumn = oldTableStyle && DatabaseHandler.isForExtendingSpace(event);
    return setWriteQueryComponents(new SQLQuery("${{geoWith}} INSERT INTO ${schema}.${table} (id, version, operation, jsondata, geo" + (withDeletedColumn ? ", deleted" : "") + ") "
        + "VALUES("
        + "#{id}, "
        + "#{version}, "
        + "#{operation}, "
        + "#{jsondata}::jsonb, "
        + "${{geo}}"
        //NOTE: The following is a temporary implementation for backwards compatibility for old table structures
        + (withDeletedColumn ? ", (#{operation} = 'D')" : "")
        + ")"), dbHandler, event);
  }

  protected static SQLQuery buildUpdateStmtQuery(DatabaseHandler dbHandler, ModifyFeaturesEvent event) {
    //NOTE: The following is a temporary implementation for backwards compatibility for old table structures
    boolean oldTableStyle = DatabaseHandler.readVersionsToKeep(event) < 1;
    boolean withDeletedColumn = oldTableStyle && DatabaseHandler.isForExtendingSpace(event);
      return setWriteQueryComponents(new SQLQuery("${{geoWith}} UPDATE ${schema}.${table} SET "
          + "version = #{version}, "
          + "operation = #{operation}, "
          + "jsondata = #{jsondata}::jsonb, "
          + "geo = (${{geo}}) "
          //NOTE: The following is a temporary implementation for backwards compatibility for old table structures
          + (withDeletedColumn ? ", deleted = (#{operation} = 'D') " : "")
          + "WHERE ${{idColumn}} = #{id} ${{uuidCheck}}"), dbHandler, event)
          .withQueryFragment("uuidCheck", buildUuidCheckFragment(event))
          .withQueryFragment("idColumn", buildIdFragment(event));
  }

  private static SQLQuery setWriteQueryComponents(SQLQuery writeQuery, DatabaseHandler dbHandler, ModifyFeaturesEvent event) {
      return setTableVariables(writeQuery
          .withQueryFragment("geoWith", "WITH in_params AS (SELECT #{geo} as geo)")
          .withQueryFragment("geo", "CASE WHEN (SELECT geo FROM in_params)::geometry IS NULL THEN NULL ELSE "
              + "ST_Force3D(ST_GeomFromWKB((SELECT geo FROM in_params)::BYTEA, 4326)) END"), dbHandler, event);
  }

  private static SQLQuery setTableVariables(SQLQuery writeQuery, DatabaseHandler dbHandler, ModifyFeaturesEvent event) {
    return writeQuery
        .withVariable("schema", dbHandler.config.getDatabaseSettings().getSchema())
        .withVariable("table", dbHandler.config.readTableFromEvent(event));
  }

  protected static SQLQuery buildDeleteStmtQuery(DatabaseHandler dbHandler, ModifyFeaturesEvent event, int version) {
      //If versioning is enabled, perform an update instead of a deletion. The trigger will finally delete the row.
      //NOTE: The following is a temporary implementation for backwards compatibility for old table structures
      boolean oldTableStyle = DatabaseHandler.readVersionsToKeep(event) < 1;
      SQLQuery query =
          version == -1 || !oldTableStyle && !event.isEnableGlobalVersioning()
          ? new SQLQuery("DELETE FROM ${schema}.${table} WHERE ${{idColumn}} = #{id} ${{uuidCheck}}")
              .withQueryFragment("idColumn", buildIdFragment(event))
          //Use UPDATE instead of DELETE to inject a version and the deleted flag. The deletion gets performed afterwards by the trigger.
          : new SQLQuery("UPDATE ${schema}.${table} "
              + "SET jsondata = jsonb_set(jsondata, '{properties,@ns:com:here:xyz}', "
              + "((jsondata->'properties'->'@ns:com:here:xyz')::JSONB "
              + "|| format('{\"uuid\": \"%s_deleted\"}', jsondata->'properties'->'@ns:com:here:xyz'->>'uuid')::JSONB) "
              + "|| format('{\"version\": %s}', #{version})::JSONB "
              + "|| format('{\"updatedAt\": %s}', (extract(epoch from now()) * 1000)::BIGINT)::JSONB "
              + "|| '{\"deleted\": true}'::JSONB) "
              + "WHERE ${{idColumn}} = #{id} ${{uuidCheck}}")
              .withNamedParameter("version", version)
              .withQueryFragment("idColumn", buildIdFragment(event));

      return setTableVariables(
          query.withQueryFragment("uuidCheck", buildUuidCheckFragment(event)),
          dbHandler,
          event);
  }

  private static String buildUuidCheckFragment(ModifyFeaturesEvent event) {
    return event.getEnableUUID() ? " AND (#{puuid}::TEXT IS NULL OR jsondata->'properties'->'@ns:com:here:xyz'->>'uuid' = #{puuid})" : "";
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

	protected static SQLQuery buildAddSubscriptionQuery(String space, String schemaName, String tableName) {
        String theSql =
          "insert into xyz_config.space_meta  ( id, schem, h_id, meta ) values(?,?,?,'{\"subscriptions\":true}' )"
         +" on conflict (id,schem) do "
         +"  update set meta = xyz_config.space_meta.meta || excluded.meta ";

        return new SQLQuery(theSql, space,schemaName,tableName);
	}

	protected static SQLQuery buildSetReplicaIdentIfNeeded() {
        String theSql =
          "do "
         +"$body$ "
         +"declare "
         +" mrec record; "
         +"begin "
         +" for mrec in "
         +"  select l.schem,l.h_id --, l.meta, r.relreplident, r.oid  "
         +"  from xyz_config.space_meta l left join pg_class r on ( r.oid = to_regclass(schem || '.' || '\"' || h_id || '\"') ) "
         +"  where 1 = 1 "
         +"    and l.meta->'subscriptions' = to_jsonb( true ) "
         +"        and r.relreplident is not null "
         +"      and r.relreplident != 'f' "
         +"  loop "
         +"     execute format('alter table %I.%I replica identity full', mrec.schem, mrec.h_id); "
         +"  end loop; "
         +"end; "
         +"$body$  "
         +"language plpgsql ";

        return new SQLQuery(theSql);
	}

    protected static String getReplicaIdentity(final String schema, final String table)
    { return String.format("select relreplident from pg_class where oid = to_regclass( '\"%s\".\"%s\"' )",schema,table); }

    protected static String setReplicaIdentity(final String schema, final String table)
    { return String.format("alter table \"%s\".\"%s\" replica identity full",schema,table); }

	public static SQLQuery buildRemoveSubscriptionQuery(String space, String schemaName) {
        String theSql =
          "update xyz_config.space_meta "
         +" set meta = meta - 'subscriptions' "
         +"where ( id, schem ) = ( ?, ? ) ";

        return new SQLQuery(theSql, space,schemaName );
	}
}
