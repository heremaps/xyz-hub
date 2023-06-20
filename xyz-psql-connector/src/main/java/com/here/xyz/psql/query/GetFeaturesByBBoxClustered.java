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

import static com.here.xyz.events.GetFeaturesByTileEvent.ResponseType.GEO_JSON;
import static com.here.xyz.responses.XyzError.ILLEGAL_ARGUMENT;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.models.geojson.WebMercatorTile;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.factory.H3SQL;
import com.here.xyz.psql.factory.QuadbinSQL;
import com.here.xyz.psql.factory.TweaksSQL;
import com.here.xyz.psql.tools.DhString;
import com.here.xyz.responses.XyzResponse;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

public class GetFeaturesByBBoxClustered<E extends GetFeaturesByBBoxEvent, R extends XyzResponse> extends GetFeaturesByBBox<E, R> {

  public static final String COUNTMODE_REAL      = "real";  // Real live counts via count(*)
  public static final String COUNTMODE_ESTIMATED = "estimated"; // Estimated counts, determined with _postgis_selectivity() or EXPLAIN Plan analyze
  public static final String COUNTMODE_MIXED     = "mixed"; // Combination of real and estimated.
  public static final String COUNTMODE_BOOL      = "bool"; // no counts but test [0|1] if data exists int tile.
  private boolean isMvtRequested;

  public GetFeaturesByBBoxClustered(E event, DatabaseHandler dbHandler)
      throws SQLException, ErrorResponseException {
    super(event, dbHandler);
  }

  @Override
  protected SQLQuery buildQuery(E event) throws SQLException, ErrorResponseException {
    isMvtRequested = isMvtRequested(event);
    SQLQuery query;
    switch(event.getClusteringType().toLowerCase()) {
      case H3SQL.HEXBIN: {
        setUseReadReplica(true); // => set to 'false' for use of h3cache ( insert into h3cache ) 
        query = buildHexbinClusteringQuery(event);

        if (isMvtRequested)
          return GetFeaturesByBBox.buildMvtEncapsuledQuery((GetFeaturesByTileEvent) event, query);
        return query;
      }
      case QuadbinSQL.QUAD: {
        setUseReadReplica(true);
        query = buildQuadbinClusteringQuery(event, dbHandler);

        if (isMvtRequested)
          return GetFeaturesByBBox.buildMvtEncapsuledQuery(dbHandler.getConfig().readTableFromEvent(event), (GetFeaturesByTileEvent) event, query);
        return query;
      }
      default: {
        throw new ErrorResponseException(ILLEGAL_ARGUMENT, "Invalid clustering type provided. Allowed values are: "
            + H3SQL.HEXBIN + ", " + QuadbinSQL.QUAD);
      }
    }
  }

  @Override
  public R handle(ResultSet rs) throws SQLException {
    return isMvtRequested ? (R) dbHandler.defaultBinaryResultSetHandler(rs) : super.handle(rs);
  }

  /**
   * Check if request parameters are valid. In case of invalidity throw an Exception
   */
  private static void checkQuadbinInput(String countMode, int relResolution, GetFeaturesByBBoxEvent event, DatabaseHandler dbHandler) throws ErrorResponseException
  {
    if( countMode != null )
     switch( countMode.toLowerCase() )
     { case COUNTMODE_REAL : case COUNTMODE_ESTIMATED: case COUNTMODE_MIXED: case COUNTMODE_BOOL : break;
       default:
        throw new ErrorResponseException(ILLEGAL_ARGUMENT,
             "Invalid request parameters. Unknown clustering.countmode="+countMode+". Available are: ["+ COUNTMODE_REAL
            +","+ COUNTMODE_ESTIMATED +","+ COUNTMODE_MIXED +","+ COUNTMODE_BOOL +"]!");
     }

    if(relResolution > 5)
      throw new ErrorResponseException(ILLEGAL_ARGUMENT,
          "Invalid request parameters. clustering.relativeResolution="+relResolution+" to high. 5 is maximum!");

    if(event.getPropertiesQuery() != null && event.getPropertiesQuery().get(0).size() != 1)
      throw new ErrorResponseException(ILLEGAL_ARGUMENT,
          "Invalid request parameters. Only one Property is allowed");

    checkCanSearchFor(event, dbHandler);
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
            GetFeaturesByBBoxEvent event) {
    BBox bbox = event.getBbox();
    Map<String, Object> clusteringParams = event.getClusteringParams();

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

        final SQLQuery searchQuery = generateSearchQueryBWC(event);

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

  public static SQLQuery buildQuadbinClusteringQuery(GetFeaturesByBBoxEvent event, DatabaseHandler dbHandler)
      throws ErrorResponseException {

    final Map<String, Object> clusteringParams = event.getClusteringParams();
    int relResolution = ( clusteringParams.get(QuadbinSQL.QUADBIN_RESOLUTION) != null ? (int) clusteringParams.get(QuadbinSQL.QUADBIN_RESOLUTION) :
        ( clusteringParams.get(QuadbinSQL.QUADBIN_RESOLUTION_RELATIVE) != null ? (int) clusteringParams.get(QuadbinSQL.QUADBIN_RESOLUTION_RELATIVE) : 0)),
        absResolution = clusteringParams.get(QuadbinSQL.QUADBIN_RESOLUTION_ABSOLUTE) != null ? (int) clusteringParams.get(QuadbinSQL.QUADBIN_RESOLUTION_ABSOLUTE) : 0;
    final String countMode = (String) clusteringParams.get(QuadbinSQL.QUADBIN_COUNTMODE);
    final boolean noBuffer = (boolean) clusteringParams.getOrDefault(QuadbinSQL.QUADBIN_NOBOFFER,false);


    checkQuadbinInput(countMode, relResolution, event, dbHandler);
    BBox bbox = event.getBbox();
        boolean isTileRequest = (event instanceof GetFeaturesByTileEvent) && ((GetFeaturesByTileEvent) event).getMargin() == 0,
                clippedOnBbox = (!isTileRequest && event.getClip());

        final WebMercatorTile tile = getTileFromBbox(bbox);

        if( (absResolution - tile.level) >= 0 )  // case of valid absResolution convert it to a relative resolution and add both resolutions
         relResolution = Math.min( relResolution + (absResolution - tile.level), 5);

        SQLQuery propQuery;
        String propQuerySQL = null;

        if (event.getPropertiesQuery() != null) {
            propQuery = generatePropertiesQueryBWC(event);

            if (propQuery != null) {
                propQuerySQL = propQuery.text();
                for (Object param : propQuery.parameters()) {
                    propQuerySQL = propQuerySQL.replaceFirst("\\?", "'" + param + "'");
                }
            }
        }
        return QuadbinSQL.generateQuadbinClusteringSQL(dbHandler.getConfig().getDatabaseSettings().getSchema(), dbHandler.getConfig().readTableFromEvent(event), relResolution, countMode, propQuerySQL, tile, bbox, isTileRequest, clippedOnBbox, noBuffer, getResponseType(event) == GEO_JSON);
    }
}
