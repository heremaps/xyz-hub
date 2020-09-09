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
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.QueryEvent;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.events.TagList;
import com.here.xyz.events.TagsQuery;
import com.here.xyz.models.geojson.WebMercatorTile;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.coordinates.WKTHelper;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.psql.factory.H3SQL;
import com.here.xyz.psql.factory.QuadbinSQL;
import com.here.xyz.psql.factory.TweaksSQL;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.sql.DataSource;

public class SQLQueryBuilder {
    private static final long GEOMETRY_DECIMAL_DIGITS = 8;
    private static final String SQL_STATISTIC_FUNCTION = "xyz_statistic_space";
    private static final String IDX_STATUS_TABLE = "xyz_config.xyz_idxs_status";

    public static SQLQuery buildGetStatisticsQuery(GetStatisticsEvent event, PSQLConfig config) throws Exception {
        final String schema = config.schema();
        final String table = config.table(event);

        return new SQLQuery("SELECT * from " + schema + "."+SQL_STATISTIC_FUNCTION+"('" + schema + "','" + table + "')");
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

    public static SQLQuery buildGetFeaturesByBBoxQuery(final GetFeaturesByBBoxEvent event, boolean isBigQuery,
                                                          DataSource dataSource)
        throws SQLException{
        final BBox bbox = event.getBbox();

        final SQLQuery searchQuery = generateSearchQuery(event,dataSource);
        final SQLQuery geoQuery = new SQLQuery("ST_Intersects(geo, ST_MakeEnvelope(?, ?, ?, ?, 4326))",
                bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat());

        return generateCombinedQuery(event, geoQuery, searchQuery,dataSource);
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

    private static int evalH3Resolution( Map<String, Object> clusteringParams, int maxResForLevel )
    {
     int h3res = maxResForLevel;

     if( clusteringParams == null ) return h3res;
/** deprecated */
     if( clusteringParams.get(H3SQL.HEXBIN_RESOLUTION) != null )
      h3res = Math.min((Integer) clusteringParams.get(H3SQL.HEXBIN_RESOLUTION), maxResForLevel);
/***/
     if( clusteringParams.get(H3SQL.HEXBIN_RESOLUTION_ABSOLUTE) != null )
      h3res = Math.min((Integer) clusteringParams.get(H3SQL.HEXBIN_RESOLUTION_ABSOLUTE), maxResForLevel);

     if( clusteringParams.get(H3SQL.HEXBIN_RESOLUTION_RELATIVE) != null )
      h3res += Math.max(0, Math.min( 4, (Integer) clusteringParams.get(H3SQL.HEXBIN_RESOLUTION_RELATIVE)));

     return Math.min( h3res, 13 );
    }

    public static SQLQuery buildHexbinClusteringQuery(
            GetFeaturesByBBoxEvent event, BBox bbox,
            Map<String, Object> clusteringParams, DataSource dataSource) throws Exception {

        int zLevel = (event instanceof GetFeaturesByTileEvent ? ((GetFeaturesByTileEvent) event).getLevel() : H3SQL.bbox2zoom(bbox)),
            maxResForLevel = H3SQL.zoom2resolution(zLevel),
            h3res = evalH3Resolution( clusteringParams, maxResForLevel );

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

        final SQLQuery query = new SQLQuery(String.format(H3SQL.h3sqlBegin, h3res,
                !h3cflip ? "st_centroid(geo)" : "geo",
                h3cflip ? "st_centroid(geo)" : clippedGeo,
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

        int pxSize = H3SQL.adjPixelSize( h3res, maxResForLevel );
         
        if (!statisticalPropertyProvided) {
            query.append(new SQLQuery(String.format(H3SQL.h3sqlMid, h3res, "(0.0)::numeric", zLevel, pxSize,expBboxSql)));
        } else {
            ArrayList<String> jpath = new ArrayList<>();
            jpath.add("properties");
            jpath.addAll(Arrays.asList(statisticalProperty.split("\\.")));

            query.append(new SQLQuery(String.format(H3SQL.h3sqlMid, h3res, "(jsondata#>> ?)::numeric", zLevel, pxSize,expBboxSql)));
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
        return QuadbinSQL.generateQuadbinClusteringSQL(config.schema(), config.table(event), relResolution, countMode, propQuerySQL, tile, noBuffer);
    }

    /***************************************** CLUSTERING END **************************************************/

    /***************************************** TWEAKS **************************************************/

    public static SQLQuery buildSamplingTweaksQuery(GetFeaturesByBBoxEvent event, BBox bbox, Map tweakParams, DataSource dataSource) throws SQLException
    {
     int strength = 0;
     boolean bDistribution = true;

     if( tweakParams != null )
     {
      if( tweakParams.get(TweaksSQL.SAMPLING_STRENGTH) instanceof Integer )
       strength = (int) tweakParams.get(TweaksSQL.SAMPLING_STRENGTH);
      else
       switch(((String) tweakParams.getOrDefault(TweaksSQL.SAMPLING_STRENGTH,"default")).toLowerCase() )
       { case "low"     : strength =  10;  break;
         case "lowmed"  : strength =  30;  break;
         case "med"     : strength =  50;  break;
         case "medhigh" : strength =  75;  break;
         case "high"    : strength = 100; break;
         default: strength  = 50; break;
       }

       switch(((String) tweakParams.getOrDefault(TweaksSQL.SAMPLING_ALGORITHM, TweaksSQL.SAMPLING_ALGORITHM_DST)).toLowerCase() )
       {
         case TweaksSQL.SAMPLING_ALGORITHM_SZE : bDistribution = false; break;
         case TweaksSQL.SAMPLING_ALGORITHM_DST :
         default: bDistribution = true; break;
       }
     }


     final String twqry = String.format(String.format("ST_Intersects(geo, ST_MakeEnvelope(%%.%1$df,%%.%1$df,%%.%1$df,%%.%1$df, 4326) ) and %%s", 14 /*GEOMETRY_DECIMAL_DIGITS*/), bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat(), TweaksSQL.strengthSql(strength,bDistribution) );

     final SQLQuery searchQuery = generateSearchQuery(event,dataSource);
     final SQLQuery tweakQuery = new SQLQuery(twqry);

     return generateCombinedQuery(event, tweakQuery, searchQuery , dataSource);
	}

    public static SQLQuery buildSimplificationTweaksQuery(GetFeaturesByBBoxEvent event, BBox bbox, Map tweakParams, DataSource dataSource) throws SQLException
    {
     int strength = 0;
     String tweaksGeoSql = "geo";
     boolean bMerge = false, bStrength = true, bTestTweaksGeoIfNull = true;

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

       // do clip before simplifications
       if (event.getClip())
       { String fmt =  String.format(  " case st_within( %%1$s, ST_MakeEnvelope(%%2$.%1$df,%%3$.%1$df,%%4$.%1$df,%%5$.%1$df, 4326) ) "
                                     + "  when true then %%1$s "
                                     + "  else ST_Intersection(%%1$s,ST_MakeEnvelope(%%2$.%1$df,%%3$.%1$df,%%4$.%1$df,%%5$.%1$df, 4326))"
                                     + " end " , 14 /*GEOMETRY_DECIMAL_DIGITS*/ );
         tweaksGeoSql = String.format( fmt, tweaksGeoSql, bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat());
       }

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

         case TweaksSQL.SIMPLIFICATION_ALGORITHM_A05 : // gridbylevel - convert to/from mvt
         { int extend = 512, extendPerMargin = extend / WebMercatorTile.TileSizeInPixel, extendWithMargin = extend, level = -1, tileX = -1, tileY = -1, margin = 0;
           
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

           final String 
            box2d   = String.format( String.format("ST_MakeEnvelope(%%.%1$df,%%.%1$df,%%.%1$df,%%.%1$df, 4326)", 14 /*GEOMETRY_DECIMAL_DIGITS*/), bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat() ),
            mvtgeom = String.format("st_translate(st_scale(st_translate(st_asmvtgeom(st_force2d(st_transform(%1$s,3857)), st_transform(%2$s,3857),%3$d), %4$d , %4$d, 0.0), st_makepoint(%5$f,%6$f,1.0) ), %7$f , %8$f, 0.0 )",
                                       tweaksGeoSql, box2d, extendWithMargin, 
                                       -extendWithMargin/2, // => shift to stretch from tilecenter
                                       stretchFactor*(xwidth / (gridsize*extend)), stretchFactor * (ywidth / (gridsize*extend)) * -1, // stretch tile to proj. size
                                        (tileX - gridsize/2 + 0.5) * (xwidth / gridsize), (tileY - gridsize/2 + 0.5) * (ywidth / gridsize) * -1 // shift to proj. position
                                       );
            
            tweaksGeoSql = String.format("st_transform(ST_SetSRID( %1$s, 3857), 4326)", mvtgeom);
            bTestTweaksGeoIfNull = false;
         } 
         break;
         
         case TweaksSQL.SIMPLIFICATION_ALGORITHM_A04 : bMerge = true; break;

         default: break;
       }

       //convert to geojson
       tweaksGeoSql = String.format("replace(ST_AsGeojson(" + getForceMode(event.isForce2D()) + "( %s ),%d),'nan','0')",tweaksGeoSql,GEOMETRY_DECIMAL_DIGITS);
     }

       final String bboxqry = String.format( String.format("ST_Intersects(geo, ST_MakeEnvelope(%%.%1$df,%%.%1$df,%%.%1$df,%%.%1$df, 4326) )", 14 /*GEOMETRY_DECIMAL_DIGITS*/), bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat() );

       final SQLQuery searchQuery = generateSearchQuery(event,dataSource);

       if( !bMerge )
        return generateCombinedQuery(event, new SQLQuery(bboxqry), searchQuery , tweaksGeoSql, bTestTweaksGeoIfNull, dataSource);

       // Merge Algorithm - only using low, med, high

       int minGeoHashLenToMerge = 0;

       if( strength <= 20 ) minGeoHashLenToMerge = 7;
       else if ( strength <= 60 ) minGeoHashLenToMerge = 6;

       if( "geo".equals(tweaksGeoSql) )
        tweaksGeoSql = String.format("replace(ST_AsGeojson(" + getForceMode(event.isForce2D()) + "( %s ),%d),'nan','0')",tweaksGeoSql,GEOMETRY_DECIMAL_DIGITS);

       SQLQuery query = new SQLQuery( String.format( TweaksSQL.mergeBeginSql, tweaksGeoSql, minGeoHashLenToMerge, bboxqry ) );

       if (searchQuery != null)
       { query.append(" and ");
         query.append(searchQuery);
       }

       query.append(TweaksSQL.mergeEndSql);
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
      sb.append(String.format("%s%s",( flag++ > 0 ? "," : ""),String.format( TweaksSQL.estimateBuildBboxSql , b.minLon(), b.minLat(), b.maxLon(), b.maxLat() )));

     return new SQLQuery( String.format( TweaksSQL.estimateCountByBboxesSql, sb.toString() ) );
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

    public static SQLQuery buildSearchablePropertiesUpsertQuery(Map<String, Boolean> searchableProperties, ModifySpaceEvent.Operation operation,
                                                                   String schema, String table) throws SQLException {
        String searchablePropertiesJson = "";
        final SQLQuery query = new SQLQuery("");

        if (searchableProperties != null) {
            for (String property : searchableProperties.keySet()) {
                searchablePropertiesJson += "\"" + property + "\":" + searchableProperties.get(property) + ",";
            }
            /* remove last comma */
            searchablePropertiesJson = searchablePropertiesJson.substring(0, searchablePropertiesJson.length() - 1);
        }

        /* update xyz_idx_status table with searchabelProperties information */
        query.append("INSERT INTO  "+IDX_STATUS_TABLE+" as x_s (spaceid,schem,idx_creation_finished,idx_manual) "
                        + "		VALUES ('" + table + "', '" + schema + "', false, '{" + searchablePropertiesJson
                        + "}'::jsonb) "
                        + "ON CONFLICT (spaceid) DO "
                        + "		UPDATE SET schem='" + schema + "', "
                        + "    			idx_manual = '{" + searchablePropertiesJson + "}'::jsonb, "
                        + "				idx_creation_finished = false "
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

    private static SQLQuery generateCombinedQuery(SearchForFeaturesEvent event, SQLQuery indexedQuery, SQLQuery secondaryQuery, String tweaksgeo, boolean bTestTweaksGeoIfNull, DataSource dataSource)
            throws SQLException {
        final SQLQuery query = new SQLQuery();


        if( tweaksgeo != null )
         query.append("select * from ( SELECT");
        else
         query.append("SELECT");

        query.append(SQLQuery.selectJson(event.getSelection(),dataSource));

        if( tweaksgeo != null )
            query.append(String.format(",%s as twgeo",tweaksgeo));
        else if (event instanceof GetFeaturesByBBoxEvent) {
            query.append(",");
            query.append(geometrySelectorForEvent((GetFeaturesByBBoxEvent) event));
        }
        else
         query.append(",replace(ST_AsGeojson(" + getForceMode(event.isForce2D()) + "(geo),"+GEOMETRY_DECIMAL_DIGITS+"),'nan','0')");

        query.append("FROM ${schema}.${table} WHERE");
        query.append(indexedQuery);

        if( secondaryQuery != null )
        { query.append(" and ");
          query.append(secondaryQuery);
        }

        if( tweaksgeo != null )
         query.append(String.format(" ) tw where %s ", bTestTweaksGeoIfNull ? "twgeo is not null" : "1 = 1" ) );

        query.append("LIMIT ?", event.getLimit());
        return query;
    }

    private static SQLQuery generateCombinedQuery(SearchForFeaturesEvent event, SQLQuery indexedQuery, SQLQuery secondaryQuery, DataSource dataSource) throws SQLException
    { return generateCombinedQuery(event,indexedQuery,secondaryQuery,null, false,dataSource); }


    /**
     * Returns the query, which will contains the geometry object.
     */
    private static SQLQuery geometrySelectorForEvent(final GetFeaturesByBBoxEvent event) {

        if (!event.getClip()) {
                return new SQLQuery("replace(ST_AsGeojson(" + getForceMode(event.isForce2D()) + "(geo),"+GEOMETRY_DECIMAL_DIGITS+"),'nan','0')");
        }

        final BBox bbox = event.getBbox();
            return new SQLQuery("replace(ST_AsGeoJson(ST_Intersection(ST_MakeValid(geo),ST_MakeEnvelope(?,?,?,?,4326)),"+GEOMETRY_DECIMAL_DIGITS+"),'nan','0')",
                    bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat());
    }

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

    protected static String deleteIdArrayStmtSQL(final String schema, final String table, final boolean handleUUID){
        String deleteIdArrayStmtSQL = "DELETE FROM ${schema}.${table} WHERE jsondata->>'id' = ANY(?) ";
        if(handleUUID) {
            deleteIdArrayStmtSQL += " AND jsondata->'properties'->'@ns:com:here:xyz'->>'uuid' = ANY(?)";
        }
        return SQLQuery.replaceVars(deleteIdArrayStmtSQL, schema, table);
    }

    protected static String deleteHistoryTriggerSQL(final String schema, final String table){
        String deleteHistoryTriggerSQL = "DROP TRIGGER IF EXISTS TR_"+table.replaceAll("-","_")+"_HISTORY_WRITER ON  ${schema}.${table};";

        return SQLQuery.replaceVars(deleteHistoryTriggerSQL, schema, table);
    }

    protected static String addHistoryTriggerSQL(final String schema, final String table, final Integer maxVersionCount, final boolean compactHistory){
        String triggerFunction = compactHistory ? "xyz_trigger_historywriter" : "xyz_trigger_historywriter_full";

        String historyTriggerSQL = "CREATE TRIGGER TR_"+table.replaceAll("-","_")+"_HISTORY_WRITER " +
                "BEFORE "+(compactHistory ? "" : "INSERT OR ")+" UPDATE OR DELETE ON ${schema}.${table} " +
                " FOR EACH ROW " +
                "EXECUTE PROCEDURE "+triggerFunction;
        historyTriggerSQL+=
                    maxVersionCount == null ? "()" : "('"+maxVersionCount+"')";

        return SQLQuery.replaceVars(historyTriggerSQL, schema, table);
    }

    private static String getForceMode(boolean isForce2D) {
      return isForce2D ? "ST_Force2D" : "ST_Force3D";
    }
}
