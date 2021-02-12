/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

import com.here.xyz.events.CountFeaturesEvent;
import com.here.xyz.events.Event;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.GetHistoryStatisticsEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.IterateHistoryEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.QueryEvent;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.events.TagList;
import com.here.xyz.events.TagsQuery;
import com.here.xyz.models.geojson.WebMercatorTile;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.coordinates.WKTHelper;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.models.hub.Space;
import com.here.xyz.psql.config.PSQLConfig;
import com.here.xyz.psql.factory.H3SQL;
import com.here.xyz.psql.factory.QuadbinSQL;
import com.here.xyz.psql.factory.TweaksSQL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

public class SQLQueryBuilder {
    private static final long GEOMETRY_DECIMAL_DIGITS = 8;
    private static final String IDX_STATUS_TABLE = "xyz_config.xyz_idxs_status";

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

    public static SQLQuery buildGetFeaturesByIdQuery(GetFeaturesByIdEvent event, PSQLConfig config, DataSource dataSource)
        throws SQLException {

        SQLQuery query = new SQLQuery("SELECT");
        query.append(SQLQuery.selectJson(event.getSelection(),dataSource));
        query.append(", replace(ST_AsGeojson(" + getForceMode(event.isForce2D()) + "(geo),"+GEOMETRY_DECIMAL_DIGITS+"),'nan','0') FROM ${schema}.${table} WHERE jsondata->>'id' = ANY(?)",
                SQLQuery.createSQLArray(event.getIds().toArray(new String[0]), "text",dataSource));
       return query;
    }

    public static SQLQuery buildGetFeaturesByGeometryQuery(GetFeaturesByGeometryEvent event, DataSource dataSource)
        throws SQLException{
        final int radius = event.getRadius();
        final Geometry geometry = event.getGeometry();

        final SQLQuery searchQuery = generateSearchQuery(event, dataSource);

        final SQLQuery geoQuery = radius != 0 ? new SQLQuery("ST_Intersects(geo, ST_Buffer(ST_GeomFromText('"
                + WKTHelper.geometryToWKB(geometry) + "')::geography, ? )::geometry)", radius) : new SQLQuery("ST_Intersects(geo, ST_GeomFromText('"
                + WKTHelper.geometryToWKB(geometry) + "',4326))");

        return generateCombinedQuery(event, geoQuery, searchQuery, dataSource);
    }

    public static SQLQuery buildGetFeaturesByBBoxQuery(final GetFeaturesByBBoxEvent event, boolean isBigQuery, DataSource dataSource)
        throws SQLException{
        final BBox bbox = event.getBbox();

        final SQLQuery searchQuery = generateSearchQuery(event,dataSource);
        final SQLQuery geoQuery = new SQLQuery("ST_Intersects(geo, ST_MakeEnvelope(?, ?, ?, ?, 4326))",
                bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat());

        boolean bConvertGeo2GeoJson = ( mvtFromDbRequested(event) == 0 );

        return generateCombinedQuery(event, geoQuery, searchQuery,dataSource, bConvertGeo2GeoJson );
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
            query = new SQLQuery("SELECT CASE WHEN reltuples < 10000");
            query.append("THEN (SELECT count(*) FROM ${schema}.${table})");
            query.append("ELSE reltuples END AS count");
            query.append("FROM pg_class WHERE oid =?::regclass", schemaTable);
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
                .format("ST_Intersection(geo,ST_MakeEnvelope(%.14f,%.14f,%.14f,%.14f,4326) )", bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat())),
                fid = (!event.getClip() ? "h3" : String.format("h3 || %f || %f", bbox.minLon(), bbox.minLat())),
                filterEmptyGeo = (!event.getClip() ? "" : String.format(" and not st_isempty( %s ) ", clippedGeo));

        final SQLQuery searchQuery = generateSearchQuery(event, dataSource);

        String aggField = (statisticalPropertyProvided ? "jsonb_set('{}'::jsonb, ? , agg::jsonb)::json" : "agg");

        boolean bConvertGeo2Geojson = ( mvtFromDbRequested(event) == 0 );

        final SQLQuery query = new SQLQuery(String.format(H3SQL.h3sqlBegin, h3res,
                !h3cflip ? "st_centroid(geo)" : "geo",
                String.format( (bConvertGeo2Geojson ? "st_asgeojson( %1$s, 7 )::json" : "(%1$s)" ), (h3cflip ? "st_centroid(geo)" : clippedGeo) ),
                statisticalPropertyProvided ? ", min, max, sum, avg, median" : "",
                zLevel,
                !h3cflip ? "centroid" : "hexagon",
                aggField,
                fid,
                expBboxSql));

        if (statisticalPropertyProvided) {
            ArrayList<String> jpath = new ArrayList<>();
            jpath.add(statisticalProperty);
            query.addParameter(SQLQuery.createSQLArray(jpath.toArray(new String[]{}), "text", dataSource));
        }

        int pxSize = H3SQL.adjPixelSize( h3res, defaultResForLevel );

        String h3sqlMid = H3SQL.h3sqlMid( clusteringParams.get(H3SQL.HEXBIN_SINGLECOORD) == Boolean.TRUE );

        int samplingStrength = samplingStrengthFromText((String) clusteringParams.getOrDefault(H3SQL.HEXBIN_SAMPLING, "off"),false);
        String samplingCondition =  ( samplingStrength <= 0 ? "1 = 1" : TweaksSQL.strengthSql( samplingStrength, true) );

        if (!statisticalPropertyProvided) {
            query.append(new SQLQuery(String.format(h3sqlMid, h3res, "(0.0)::numeric", zLevel, pxSize,expBboxSql,samplingCondition)));
        } else {
            ArrayList<String> jpath = new ArrayList<>();
            jpath.add("properties");
            jpath.addAll(Arrays.asList(statisticalProperty.split("\\.")));

            query.append(new SQLQuery(String.format(h3sqlMid, h3res, "(jsondata#>> ?)::numeric", zLevel, pxSize,expBboxSql,samplingCondition)));
            query.addParameter(SQLQuery.createSQLArray(jpath.toArray(new String[]{}), "text", dataSource));
        }

        if (searchQuery != null) {
            query.append(" and ");
            query.append(searchQuery);
        }

        query.append(String.format(H3SQL.h3sqlEnd, filterEmptyGeo));
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
        boolean bConvertGeo2Geojson = ( mvtFromDbRequested(event) == 0 );

        return QuadbinSQL.generateQuadbinClusteringSQL(config.getDatabaseSettings().getSchema(), config.readTableFromEvent(event), relResolution, countMode, propQuerySQL, tile, noBuffer, bConvertGeo2Geojson);
    }

    /***************************************** CLUSTERING END **************************************************/

    /***************************************** TWEAKS **************************************************/

    public static boolean mvtFromHubRequested( GetFeaturesByBBoxEvent event )
    {
     return( (event instanceof GetFeaturesByTileEvent) && ( event.getBinaryType() != null ) && "hubmvt".equals(event.getBinaryType()) );
    }

    public static int mvtFromDbRequested( GetFeaturesByBBoxEvent event )
    { if( (event instanceof GetFeaturesByTileEvent) && ( event.getBinaryType() != null ))
       switch ( event.getBinaryType() )
       { case "MVT" : return 1;
         case "MVT_FLATTENED" : return 2;
         default : break;
       }
      return 0;
    }

    private static String map2MvtGeom( GetFeaturesByBBoxEvent event, BBox bbox, String tweaksGeoSql )
    {
     boolean bExtend512 = (   "viz".equals(event.getOptimizationMode())
                           || (event.getTweakParams() != null && event.getTweakParams().size() > 0 )); // -> 512 only if tweaks or viz been specified explicit
     int extend = ( bExtend512 ? 512 : 4096 ), extendPerMargin = extend / WebMercatorTile.TileSizeInPixel, extendWithMargin = extend, level = -1, tileX = -1, tileY = -1, margin = 0;

     if( event instanceof GetFeaturesByTileEvent )
     { GetFeaturesByTileEvent tevnt = (GetFeaturesByTileEvent) event;
       level = tevnt.getLevel();
       tileX = tevnt.getX();
       tileY = tevnt.getY();
       margin = tevnt.getMargin();
       extendWithMargin = extend + (margin * extendPerMargin);
     }
     else
     { final WebMercatorTile tile = getTileFromBbox(bbox);
       level = tile.level;
       tileX = tile.x;
       tileY = tile.y;
     }

     double wgs3857width = 20037508.342789244d,
            xwidth = 2 * wgs3857width,
            ywidth = 2 * wgs3857width,
            gridsize = (1L << level),
            stretchFactor = 1.0 + ( margin / ((double) WebMercatorTile.TileSizeInPixel)); // xyz-hub uses margin for tilesize of 256 pixel.

     String
      box2d   = String.format( String.format("ST_MakeEnvelope(%%.%1$df,%%.%1$df,%%.%1$df,%%.%1$df, 4326)", 14 /*GEOMETRY_DECIMAL_DIGITS*/), bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat() ),
        // 1. build mvt
      mvtgeom = String.format("st_asmvtgeom(st_force2d(st_transform(%1$s,3857)), st_transform(%2$s,3857),%3$d,0,true)", tweaksGeoSql, box2d, extendWithMargin);
        // 2. project the mvt to tile
      mvtgeom = String.format("st_setsrid(st_translate(st_scale(st_translate(%1$s, %2$d , %2$d, 0.0), st_makepoint(%3$f,%4$f,1.0) ), %5$f , %6$f, 0.0 ), 3857)",
                               mvtgeom,
                               -extendWithMargin/2, // => shift to stretch from tilecenter
                               stretchFactor*(xwidth / (gridsize*extend)), stretchFactor * (ywidth / (gridsize*extend)) * -1, // stretch tile to proj. size
                               (tileX - gridsize/2 + 0.5) * (xwidth / gridsize), (tileY - gridsize/2 + 0.5) * (ywidth / gridsize) * -1 // shift to proj. position
                             );
        // 3 project tile to wgs84 and map invalid geom to null
      mvtgeom = String.format("(select case st_isvalid(g) when true then g else null end from st_transform(%1$s,4326) g)", mvtgeom );
        // 4. assure intersect with origin bbox in case of mapping errors
      mvtgeom  = String.format("ST_Intersection(%1$s,st_setsrid(%2$s,4326))", mvtgeom, box2d);
        // 5. map non-null but empty polygons to null - e.g. avoid -> "geometry": { "type": "Polygon", "coordinates": [] }
      mvtgeom = String.format("(select case st_isempty(g) when false then g else null end from %1$s g)", mvtgeom );

      // if geom = point | multipoint then no mvt <-> geo conversion should be done
      mvtgeom = String.format("case strpos(ST_GeometryType( geo ), 'Point') > 0 when true then geo else %1$s end", mvtgeom );

      return mvtgeom;
    }

    private static String clipProjGeom(BBox bbox, String tweaksGeoSql )
    {
     String fmt =  String.format(  " case st_within( %%1$s, ST_MakeEnvelope(%%2$.%1$df,%%3$.%1$df,%%4$.%1$df,%%5$.%1$df, 4326) ) "
                                 + "  when true then %%1$s "
                                 + "  else ST_Intersection(%%1$s,ST_MakeEnvelope(%%2$.%1$df,%%3$.%1$df,%%4$.%1$df,%%5$.%1$df, 4326))"
                                 + " end " , 14 /*GEOMETRY_DECIMAL_DIGITS*/ );
     return String.format( fmt, tweaksGeoSql, bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat());
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

    public static SQLQuery buildSamplingTweaksQuery(GetFeaturesByBBoxEvent event, BBox bbox, Map tweakParams, DataSource dataSource) throws SQLException
    {
     int strength = 0;
     boolean bDistribution = true,
             bConvertGeo2Geojson = ( mvtFromDbRequested(event) == 0 );

     if( tweakParams != null )
     {
      if( tweakParams.get(TweaksSQL.SAMPLING_STRENGTH) instanceof Integer )
       strength = (int) tweakParams.get(TweaksSQL.SAMPLING_STRENGTH);
      else
       strength = samplingStrengthFromText( (String) tweakParams.getOrDefault(TweaksSQL.SAMPLING_STRENGTH,"default"), true );

       switch(((String) tweakParams.getOrDefault(TweaksSQL.SAMPLING_ALGORITHM, TweaksSQL.SAMPLING_ALGORITHM_DST)).toLowerCase() )
       {
         case TweaksSQL.SAMPLING_ALGORITHM_SZE : bDistribution = false; break;
         case TweaksSQL.SAMPLING_ALGORITHM_DST :
         default: bDistribution = true; break;
       }
     }

     boolean bEnsureMode = TweaksSQL.ENSURE.equals( event.getTweakType().toLowerCase() );

     final String sCondition = ( bEnsureMode && strength == 0 ? "1 = 1" : TweaksSQL.strengthSql(strength,bDistribution)  ),
                  twqry = String.format(String.format("ST_Intersects(geo, ST_MakeEnvelope(%%.%1$df,%%.%1$df,%%.%1$df,%%.%1$df, 4326) ) and %%s", 14 /*GEOMETRY_DECIMAL_DIGITS*/), bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat(), sCondition );

     final SQLQuery searchQuery = generateSearchQuery(event,dataSource),
                    tweakQuery = new SQLQuery(twqry);

     if( !bEnsureMode )
      return generateCombinedQuery(event, tweakQuery, searchQuery , dataSource, bConvertGeo2Geojson );

     /* TweaksSQL.ENSURE */
     boolean bTestTweaksGeoIfNull = false;
     String tweaksGeoSql = clipProjGeom(bbox,"geo");
     tweaksGeoSql = map2MvtGeom( event, bbox, tweaksGeoSql );
     //convert to geojson
     tweaksGeoSql = ( bConvertGeo2Geojson ? String.format("replace(ST_AsGeojson(" + getForceMode(event.isForce2D()) + "( %s ),%d),'nan','0')",tweaksGeoSql,GEOMETRY_DECIMAL_DIGITS)
                                          : String.format( getForceMode(event.isForce2D()) + "( %s )",tweaksGeoSql ) );

     return generateCombinedQueryTweaks(event, tweakQuery, searchQuery , tweaksGeoSql, bTestTweaksGeoIfNull, dataSource);
	}

    public static SQLQuery buildSimplificationTweaksQuery(GetFeaturesByBBoxEvent event, BBox bbox, Map tweakParams, DataSource dataSource) throws SQLException
    {
     int strength = 0,
         iMerge = 0;
     String tweaksGeoSql = "geo";
     boolean bStrength = true, bTestTweaksGeoIfNull = true, bConvertGeo2Geojson = ( mvtFromDbRequested(event) == 0 );


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

       if (event.getClip()) // do clip before simplification -- preventing extrem large polygon for further working steps
        tweaksGeoSql = clipProjGeom(bbox,tweaksGeoSql );

       //SIMPLIFICATION_ALGORITHM
       int hint = 0;

       switch( ((String) tweakParams.getOrDefault(TweaksSQL.SIMPLIFICATION_ALGORITHM,"default")).toLowerCase() )
       {
         case TweaksSQL.SIMPLIFICATION_ALGORITHM_A03 : hint++;
         case TweaksSQL.SIMPLIFICATION_ALGORITHM_A02 :
         {
          double tolerance = 0.0;
          if(  strength > 0  && strength <= 25 )      tolerance = 0.0001 + ((0.001-0.0001)/25.0) * (strength -  1);
          else if(  strength > 25 && strength <= 50 ) tolerance = 0.001  + ((0.01 -0.001) /25.0) * (strength - 26);
          else if(  strength > 50 && strength <= 75 ) tolerance = 0.01   + ((0.1  -0.01)  /25.0) * (strength - 51);
          else /* [76 - 100 ] */                      tolerance = 0.1    + ((1.0  -0.1)   /25.0) * (strength - 76);
          tweaksGeoSql = String.format("%s(%s, %f)",( hint == 0 ? "ftm_SimplifyPreserveTopology" : "ftm_Simplify"), tweaksGeoSql, tolerance );
         }
         break;

         case TweaksSQL.SIMPLIFICATION_ALGORITHM_A01 :
         {
          double tolerance = 0.0;
          if(!bStrength) tolerance = Math.abs( bbox.maxLon() - bbox.minLon() ) / 4096;
          else if(  strength > 0  && strength <= 25 ) tolerance = 0.0001 + ((0.001-0.0001)/25.0) * (strength -  1);
          else if(  strength > 25 && strength <= 50 ) tolerance = 0.001  + ((0.01 -0.001) /25.0) * (strength - 26);
          else if(  strength > 50 && strength <= 75 ) tolerance = 0.01   + ((0.1  -0.01)  /25.0) * (strength - 51);
          else /* [76 - 100 ] */                      tolerance = 0.1    + ((1.0  -0.1)   /25.0) * (strength - 76);
          tweaksGeoSql = String.format("ST_SnapToGrid(%s, %f)",tweaksGeoSql, tolerance );
         }
         break;

         case TweaksSQL.SIMPLIFICATION_ALGORITHM_A05 : // gridbytilelevel - convert to/from mvt
         {
          tweaksGeoSql = map2MvtGeom( event, bbox, tweaksGeoSql );
          bTestTweaksGeoIfNull = false;
         }
         break;

         case TweaksSQL.SIMPLIFICATION_ALGORITHM_A06 : iMerge++;
         case TweaksSQL.SIMPLIFICATION_ALGORITHM_A04 : iMerge++; break;

         default: break;
       }

       //convert to geojson
       tweaksGeoSql = ( bConvertGeo2Geojson ? String.format("replace(ST_AsGeojson(" + getForceMode(event.isForce2D()) + "( %s ),%d),'nan','0')",tweaksGeoSql,GEOMETRY_DECIMAL_DIGITS)
                                            : String.format( getForceMode(event.isForce2D()) + "( %s )",tweaksGeoSql) );
     }

       final String bboxqry = String.format( String.format("ST_Intersects(geo, ST_MakeEnvelope(%%.%1$df,%%.%1$df,%%.%1$df,%%.%1$df, 4326) )", 14 /*GEOMETRY_DECIMAL_DIGITS*/), bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat() );

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
        tweaksGeoSql = ( bConvertGeo2Geojson ? String.format("replace(ST_AsGeojson(" + getForceMode(event.isForce2D()) + "( %s ),%d),'nan','0')",tweaksGeoSql,GEOMETRY_DECIMAL_DIGITS)
                                             : String.format(getForceMode(event.isForce2D()) + "( %s )",tweaksGeoSql) );

       if( bConvertGeo2Geojson )
        tweaksGeoSql = String.format("(%s)::jsonb", tweaksGeoSql);

        SQLQuery query =
         ( iMerge == 1 ? new SQLQuery( String.format( TweaksSQL.mergeBeginSql, tweaksGeoSql, minGeoHashLenToMerge, bboxqry ) )
                       : new SQLQuery( String.format( TweaksSQL.linemergeBeginSql, /*(event.getClip() ? clipProjGeom(bbox,"geo") : "geo")*/ "geo" , bboxqry ) ));  // use clipped geom as input (?)

       if (searchQuery != null)
       { query.append(" and ");
         query.append(searchQuery);
       }

       if( iMerge == 1 )
        query.append( TweaksSQL.mergeEndSql(bConvertGeo2Geojson) );
       else
       { query.append( String.format( TweaksSQL.linemergeEndSql1, minGeoHashLenForLineMerge ) );
         query.append(SQLQuery.selectJson(event.getSelection(),dataSource));
         query.append( String.format( TweaksSQL.linemergeEndSql2, tweaksGeoSql ) );
       }

       query.append("LIMIT ?", event.getLimit());

       return query;
	}




    public static SQLQuery buildEstimateSamplingStrengthQuery( GetFeaturesByBBoxEvent event, BBox bbox )
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
       if( (dy == 0) && (dx == 0) ) listOfBBoxes.add(bbox);  // centerbox, this is alredy extended by margin
       else if( ((tileY + dy) > 0) && ((tileY + dy) < nrTilesXY) )
        listOfBBoxes.add( WebMercatorTile.forWeb(level, ((nrTilesXY +(tileX + dx)) % nrTilesXY) , (tileY + dy)).getExtendedBBox(margin) );

     int flag = 0;
     StringBuilder sb = new StringBuilder();
     for (BBox b : listOfBBoxes)
      sb.append(String.format("%s%s",( flag++ > 0 ? "," : ""),String.format( TweaksSQL.requestedTileBoundsSql , b.minLon(), b.minLat(), b.maxLon(), b.maxLat() )));

     return new SQLQuery( String.format( TweaksSQL.estimateCountByBboxesSql, sb.toString() ) );
    }

    public static SQLQuery buildMvtEncapsuledQuery( String spaceId, SQLQuery dataQry, WebMercatorTile mvtTile, int mvtMargin, boolean bFlattend )
    { int extend = 4096, buffer = (extend / WebMercatorTile.TileSizeInPixel) * mvtMargin;
      BBox b = mvtTile.getBBox(false); // pg ST_AsMVTGeom expects tiles bbox without buffer.
      SQLQuery r = new SQLQuery( String.format( TweaksSQL.mvtBeginSql,
                                   String.format( TweaksSQL.requestedTileBoundsSql , b.minLon(), b.minLat(), b.maxLon(), b.maxLat() ),
                                   (!bFlattend) ? TweaksSQL.mvtPropertiesSql : TweaksSQL.mvtPropertiesFlattenSql,
                                   extend,
                                   buffer )
                               );
     r.append(dataQry);
     r.append( String.format( TweaksSQL.mvtEndSql, spaceId ));
     return r;
    }


    /***************************************** TWEAKS END **************************************************/

    public static SQLQuery buildFeaturesQuery(final SearchForFeaturesEvent event, final boolean isIterate, final boolean hasHandle,
                                                 final boolean hasSearch, final long start, DataSource dataSource)
            throws Exception {

        final SQLQuery query = new SQLQuery("SELECT");
        query.append(SQLQuery.selectJson(event.getSelection(), dataSource));
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

    public static SQLQuery buildDeleteFeaturesByTagQuery(boolean includeOldStates, SQLQuery searchQuery){

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

    public static SQLQuery buildLoadFeaturesQuery(final Map<String, String> idMap, boolean enabledHistory, DataSource dataSource)
            throws SQLException{

        final ArrayList<String> ids = new ArrayList<>(idMap.size());
        ids.addAll(idMap.keySet());

        final ArrayList<String> values = new ArrayList<>(idMap.size());
        values.addAll(idMap.values());

        if(!enabledHistory) {
            return new SQLQuery("SELECT jsondata, replace(ST_AsGeojson(ST_Force3D(geo),"+GEOMETRY_DECIMAL_DIGITS+"),'nan','0') FROM ${schema}.${table} WHERE jsondata->>'id' = ANY(?)",
                    SQLQuery.createSQLArray(ids.toArray(new String[0]), "text", dataSource));
        }else{
            final SQLQuery query;
            query = new SQLQuery("SELECT jsondata, replace(ST_AsGeojson(ST_Force3D(geo),"+GEOMETRY_DECIMAL_DIGITS+"),'nan','0') FROM ${schema}.${table} WHERE jsondata->>'id' = ANY(?) ");
            query.append("UNION ");
            query.append("SELECT jsondata, replace(ST_AsGeojson(ST_Force3D(geo),"+GEOMETRY_DECIMAL_DIGITS+"),'nan','0') FROM ${schema}.${hsttable} WHERE uuid = ANY(?) ");
            query.addParameter( SQLQuery.createSQLArray(ids.toArray(new String[0]), "text", dataSource));
            query.addParameter( SQLQuery.createSQLArray(values.toArray(new String[0]), "text", dataSource));
            return query;
        }
    }

    public static SQLQuery buildHistoryQuery(IterateHistoryEvent event) {
        SQLQuery query = new SQLQuery("SELECT operation, version, vid, (SELECT row_to_json(_) from (select f.type, f.id, f.geometry, f.properties) as _) As feature " +
                "FROM ( " +
                "   SELECT 'Feature' As type," +
                "   ( CASE " +
                "      WHEN (COALESCE((jsondata->'properties'->'@ns:com:here:xyz'->>'deleted')::boolean, false) IS true) THEN 'DELETED'" +
                "      WHEN (jsondata->'properties'->'@ns:com:here:xyz'->'puuid' IS NULL" +
                "          AND" +
                "          (COALESCE((jsondata->'properties'->'@ns:com:here:xyz'->>'deleted')::boolean, false) IS NOT true)" +
                "      ) THEN 'INSERTED'" +
                "      ELSE 'UPDATED' " +
                "   END) as operation, " +
                "   jsondata->'properties'->'@ns:com:here:xyz'->>'version' as version," +
                "   ST_AsGeoJSON(geo)::json As geometry," +
                "   jsondata->>'id' as id," +
                "   vid,"+
                "   jsondata->'properties' as properties" +
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
        query.append(") as f");

        return query;
    }

    public static SQLQuery buildSquashHistoryQuery(IterateHistoryEvent event){
        SQLQuery query = new SQLQuery("SELECT operation, version, (SELECT row_to_json(_) from (select f.type, f.id, f.geometry, f.properties) as _) As feature, id " +
                "FROM ( " +
                "   SELECT distinct ON (jsondata->>'id') jsondata->>'id'," +
                "   'Feature' As type," +
                "   ( CASE " +
                "      WHEN (COALESCE((jsondata->'properties'->'@ns:com:here:xyz'->>'deleted')::boolean, false) IS true) THEN 'DELETED'" +
                "      WHEN (jsondata->'properties'->'@ns:com:here:xyz'->'puuid' IS NULL" +
                "          AND" +
                "          (COALESCE((jsondata->'properties'->'@ns:com:here:xyz'->>'deleted')::boolean, false) IS NOT true)" +
                "      ) THEN 'INSERTED'" +
                "      ELSE 'UPDATED' " +
                "   END) as operation, " +
                "   jsondata->'properties'->'@ns:com:here:xyz'->>'version' as version," +
                "   ST_AsGeoJSON(geo)::json As geometry," +
                "   jsondata->>'id' as id," +
                "   jsondata->'properties' as properties" +
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

        query.append(") as f");

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

    public static SQLQuery buildSearchablePropertiesUpsertQuery(Space spaceDefinition, ModifySpaceEvent.Operation operation,
                                                                String schema, String table) throws SQLException {
        Map<String, Boolean> searchableProperties = spaceDefinition.getSearchableProperties();
        Boolean enableAutoIndexing = spaceDefinition.isEnableAutoSearchableProperties();

        String searchablePropertiesJson = "";
        final SQLQuery query = new SQLQuery("");

        if (searchableProperties != null) {
            for (String property : searchableProperties.keySet()) {
                searchablePropertiesJson += "\"" + property + "\":" + searchableProperties.get(property) + ",";
            }
            /* remove last comma */
            searchablePropertiesJson = searchablePropertiesJson.substring(0, searchablePropertiesJson.length() - 1);
        }

        /* update xyz_idx_status table with searchableProperties information */
        query.append("INSERT INTO  "+IDX_STATUS_TABLE+" as x_s (spaceid, schem, idx_creation_finished, idx_manual "
                        +(enableAutoIndexing != null ? ",auto_indexing) ": ") ")
                        + "		VALUES ('" + table + "', '" + schema + "', false, '{" + searchablePropertiesJson
                        + "}'::jsonb"+(enableAutoIndexing != null ? ","+enableAutoIndexing: " ")+") "
                        + "ON CONFLICT (spaceid) DO "
                        + "		UPDATE SET schem='" + schema + "', "
                        + "    			idx_manual = '{" + searchablePropertiesJson + "}'::jsonb, "
                        + "				idx_creation_finished = false"
                        + (enableAutoIndexing != null ? " ,auto_indexing = " + enableAutoIndexing : "")
                        + "		WHERE x_s.spaceid = '" + table + "'");
        return query;
    }

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

                    // The ID is indexed as text
                    if (propertyQuery.getKey().equals("id")) {
                        keyDisjunctionQueries.add(new SQLQuery("jsondata->>'id'" + SQLQuery.getOperation(propertyQuery.getOperation()) + "?::text", v));
                    }
                    // The rest are indexed as jsonb
                    else {
                        SQLQuery q = SQLQuery.createKey(propertyQuery.getKey());

                        q.append(new SQLQuery(SQLQuery.getOperation(propertyQuery.getOperation()) + SQLQuery.getValue(v,propertyQuery.getOperation()), v));
                        keyDisjunctionQueries.add(q);
                    }
                });
                conjunctionQueries.add(SQLQuery.join(keyDisjunctionQueries, "OR", true));
            });
            disjunctionQueries.add(SQLQuery.join(conjunctionQueries, "AND", false));
        });
        return SQLQuery.join(disjunctionQueries, "OR", false);
    }

    private static SQLQuery generateTagsQuery(TagsQuery tags, DataSource dataSource)
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
            query = new SQLQuery(" (jsondata->'properties'->'@ns:com:here:xyz'->'tags' ??| ?)", SQLQuery.createSQLArray(orList, "text",dataSource));
        } else {
            query = new SQLQuery("(" + andQuery + ")");
            for (TagList tag : tags) {
                query.addParameter(SQLQuery.createSQLArray(tag.toArray(new String[0]), "text",dataSource));
            }
        }

        return query;
    }

    private static SQLQuery generateCombinedQueryTweaks(SearchForFeaturesEvent event, SQLQuery indexedQuery, SQLQuery secondaryQuery, String tweaksgeo, boolean bTestTweaksGeoIfNull, DataSource dataSource) throws SQLException
    {
     final SQLQuery query = new SQLQuery();

     query.append("select * from ( SELECT");

     query.append(SQLQuery.selectJson(event.getSelection(),dataSource));

     query.append(String.format(",%s as geo",tweaksgeo));

     query.append("FROM ${schema}.${table} WHERE");
     query.append(indexedQuery);

     if( secondaryQuery != null )
     { query.append(" and ");
       query.append(secondaryQuery);
     }

     query.append(String.format(" ) tw where %s ", bTestTweaksGeoIfNull ? "geo is not null" : "1 = 1" ) );

     query.append("LIMIT ?", event.getLimit());
     return query;
    }

    private static SQLQuery generateCombinedQuery(SearchForFeaturesEvent event, SQLQuery indexedQuery, SQLQuery secondaryQuery, DataSource dataSource, boolean bConvertGeo2Geojson ) throws SQLException
    {
     final SQLQuery query = new SQLQuery();

     query.append("SELECT");

     query.append(SQLQuery.selectJson(event.getSelection(),dataSource));

     if (event instanceof GetFeaturesByBBoxEvent) {
         query.append(",");
         query.append(geometrySelectorForEvent((GetFeaturesByBBoxEvent) event, bConvertGeo2Geojson));
     }
     else
      query.append( bConvertGeo2Geojson ? ( ",replace(ST_AsGeojson(" + getForceMode(event.isForce2D()) + "(geo),"+GEOMETRY_DECIMAL_DIGITS+"),'nan','0') as geo" )
                                        :  getForceMode(event.isForce2D()) + "(geo) as geo" );

     query.append("FROM ${schema}.${table} WHERE");
     query.append(indexedQuery);

     if( secondaryQuery != null )
     { query.append(" and ");
       query.append(secondaryQuery);
     }

     query.append("LIMIT ?", event.getLimit());
     return query;
    }

    private static SQLQuery generateCombinedQuery(SearchForFeaturesEvent event, SQLQuery indexedQuery, SQLQuery secondaryQuery, DataSource dataSource ) throws SQLException
    { return generateCombinedQuery( event, indexedQuery, secondaryQuery, dataSource, true ); }

    /**
     * Returns the query, which will contains the geometry object.
     */

    private static SQLQuery geometrySelectorForEvent(final GetFeaturesByBBoxEvent event, boolean bGeoJson) {

        if (!event.getClip()) {
          String  geoSqlAttrib = ( bGeoJson ? String.format("replace(ST_AsGeojson(%s(geo),%d),'nan','0') as geo", getForceMode(event.isForce2D()), GEOMETRY_DECIMAL_DIGITS )
                                            : String.format("%s(geo) as geo",getForceMode(event.isForce2D())));

          return new SQLQuery( geoSqlAttrib );
        }

        final BBox bbox = event.getBbox();

        String geoCol = "geo",
               geoSqlAttrib = ( bGeoJson ? String.format("replace(ST_AsGeoJson(ST_Intersection(ST_MakeValid(%s),ST_MakeEnvelope(?,?,?,?,4326)),%d),'nan','0') as geo", geoCol, GEOMETRY_DECIMAL_DIGITS )
                                         : String.format("ST_Intersection( ST_MakeValid(%s),ST_MakeEnvelope(?,?,?,?,4326) ) as geo", geoCol ) );

            return new SQLQuery( geoSqlAttrib ,bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat());
    }

    private static SQLQuery geometrySelectorForEvent(final GetFeaturesByBBoxEvent event) { return geometrySelectorForEvent(event,true); }


    protected static SQLQuery generateSearchQuery(final QueryEvent event, final DataSource dataSource)
            throws SQLException {
        final SQLQuery propertiesQuery = generatePropertiesQuery(event.getPropertiesQuery());
        final SQLQuery tagsQuery = generateTagsQuery(event.getTags(),dataSource);

        return SQLQuery.join("AND", propertiesQuery, tagsQuery);
    }

    protected static SQLQuery generateLoadOldFeaturesQuery(final String[] idsToFetch, final DataSource dataSource)
            throws SQLException {
        return new SQLQuery("SELECT jsondata, replace(ST_AsGeojson(ST_Force3D(geo),"+GEOMETRY_DECIMAL_DIGITS+"),'nan','0') FROM ${schema}.${table} WHERE jsondata->>'id' = ANY(?)",
            SQLQuery.createSQLArray(idsToFetch, "text", dataSource));
    }

    protected static SQLQuery generateLoadExistingIdsQuery(final String[] idsToFetch, final DataSource dataSource)
        throws SQLException {
      return new SQLQuery("SELECT jsondata->>'id' id FROM ${schema}.${table} WHERE jsondata->>'id' = ANY(?)",
          SQLQuery.createSQLArray(idsToFetch, "text", dataSource));
    }

    protected static SQLQuery generateIDXStatusQuery(final String space){
        return new SQLQuery("SELECT idx_available FROM "+IDX_STATUS_TABLE+" WHERE spaceid=?", space);
    }

    protected static String insertStmtSQL(final String schema, final String table){
        String insertStmtSQL ="INSERT INTO ${schema}.${table} (jsondata, geo) VALUES(?::jsonb, ST_Force3D(ST_GeomFromWKB(?,4326)))";
        return SQLQuery.replaceVars(insertStmtSQL, schema, table);
    }

    protected static String insertWithoutGeometryStmtSQL(final String schema, final String table){
        String insertWithoutGeometryStmtSQL = "INSERT INTO ${schema}.${table} (jsondata, geo) VALUES(?::jsonb, NULL)";

        return SQLQuery.replaceVars(insertWithoutGeometryStmtSQL, schema, table);
    }

    protected static String updateStmtSQL(final String schema, final String table, final boolean handleUUID){
        String updateStmtSQL = "UPDATE ${schema}.${table} SET jsondata = ?::jsonb, geo=ST_Force3D(ST_GeomFromWKB(?,4326)) WHERE jsondata->>'id' = ?";
        if(handleUUID) {
            updateStmtSQL += " AND jsondata->'properties'->'@ns:com:here:xyz'->>'uuid' = ?";
        }
        return SQLQuery.replaceVars(updateStmtSQL, schema, table);
    }

    protected static String updateWithoutGeometryStmtSQL(final String schema, final String table, final boolean handleUUID){
        String updateWithoutGeometryStmtSQL = "UPDATE ${schema}.${table} SET  jsondata = ?::jsonb, geo=NULL WHERE jsondata->>'id' = ?";
        if(handleUUID) {
            updateWithoutGeometryStmtSQL += " AND jsondata->'properties'->'@ns:com:here:xyz'->>'uuid' = ?";
        }
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

    protected static String deleteOldHistoryEntries(final String schema, final String table, long version){
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
                "        AND (jsondata->'properties'->'@ns:com:here:xyz'->'version' <= '"+version+"'::jsonb " +
                "        AND jsondata->'properties'->'@ns:com:here:xyz'->'version' > '0'::jsonb)" +
                ")" +
                "   ORDER  BY vid" +
                "   FOR    UPDATE" +
                "   ) del " +
                "WHERE  t.vid = del.vid;";

        return SQLQuery.replaceVars(deleteOldHistoryEntriesSQL, schema, table);
    }

    protected static String flagOutdatedHistoryEntries(final String schema, final String table, long version){
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
                "AND jsondata->'properties'->'@ns:com:here:xyz'->'version' <= '"+version+"'::jsonb " +
                "AND jsondata->'properties'->'@ns:com:here:xyz'->'version' > '0'::jsonb;";

        return SQLQuery.replaceVars(flagOutdatedHistoryEntries, schema, table);
    }

    protected static String deleteHistoryEntriesWithDeleteFlag(final String schema, final String table){
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

    protected static String deleteHistoryTriggerSQL(final String schema, final String table){
        String deleteHistoryTriggerSQL = "DROP TRIGGER IF EXISTS \"TR_"+table.replaceAll("-","_")+"_HISTORY_WRITER\" ON  ${schema}.${table};";

        return SQLQuery.replaceVars(deleteHistoryTriggerSQL, schema, table);
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

    private static String getForceMode(boolean isForce2D) {
      return isForce2D ? "ST_Force2D" : "ST_Force3D";
    }
}
