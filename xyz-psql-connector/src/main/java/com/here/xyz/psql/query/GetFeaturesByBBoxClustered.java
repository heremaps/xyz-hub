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
import static com.here.xyz.psql.DatabaseHandler.HEAD_TABLE_SUFFIX;
import static com.here.xyz.responses.XyzError.ILLEGAL_ARGUMENT;

import com.google.common.collect.Streams;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.models.geojson.WebMercatorTile;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.psql.PSQLXyzConnector;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.factory.TweaksSQL;
import com.here.xyz.psql.tools.DhString;
import com.here.xyz.responses.XyzResponse;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

public class GetFeaturesByBBoxClustered<E extends GetFeaturesByBBoxEvent, R extends XyzResponse> extends GetFeaturesByBBox<E, R> {

  public static final String COUNTMODE_REAL      = "real";  // Real live counts via count(*)
  public static final String COUNTMODE_ESTIMATED = "estimated"; // Estimated counts, determined with _postgis_selectivity() or EXPLAIN Plan analyze
  public static final String COUNTMODE_MIXED     = "mixed"; // Combination of real and estimated.
  public static final String COUNTMODE_BOOL      = "bool"; // no counts but test [0|1] if data exists int tile.
  private boolean isMvtRequested;

  public GetFeaturesByBBoxClustered(E event)
      throws SQLException, ErrorResponseException {
    super(event);
  }

  @Override
  protected SQLQuery buildQuery(E event) throws SQLException, ErrorResponseException {
    isMvtRequested = isMvtRequested(event);
    SQLQuery query;
    switch(event.getClusteringType().toLowerCase()) {
      case HEXBIN: {
        setUseReadReplica(true); // => set to 'false' for use of h3cache ( insert into h3cache )
        query = buildHexbinClusteringQuery(event);

        if (isMvtRequested)
          return buildMvtEncapsuledQuery((GetFeaturesByTileEvent) event, query);
        return query;
      }
      case QUAD: {
        setUseReadReplica(true);
        query = buildQuadbinClusteringQuery(event, PSQLXyzConnector.getInstance());

        if (isMvtRequested)
          return buildMvtEncapsuledQuery(XyzEventBasedQueryRunner.readTableFromEvent(event), (GetFeaturesByTileEvent) event, query);
        return query;
      }
      default: {
        throw new ErrorResponseException(ILLEGAL_ARGUMENT, "Invalid clustering type provided. Allowed values are: "
            + HEXBIN + ", " + QUAD);
      }
    }
  }

  @Override
  public R handle(ResultSet rs) throws SQLException {
    return isMvtRequested ? (R) GetFeaturesByBBox.defaultBinaryResultSetHandler(rs) : super.handle(rs);
  }

  /**
   * Check if request parameters are valid. In case of invalidity throw an Exception
   */
  private static void checkQuadbinInput(String countMode, int relResolution, GetFeaturesByBBoxEvent event, PSQLXyzConnector dbHandler) throws ErrorResponseException
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
     if( clusteringParams.get(HEXBIN_RESOLUTION) != null )
      h3res = Math.min((Integer) clusteringParams.get(HEXBIN_RESOLUTION), defaultResForLevel + overzoomingRes);
/***/
     if( clusteringParams.get(HEXBIN_RESOLUTION_ABSOLUTE) != null )
      h3res = Math.min((Integer) clusteringParams.get(HEXBIN_RESOLUTION_ABSOLUTE), defaultResForLevel + overzoomingRes);

     if( clusteringParams.get(HEXBIN_RESOLUTION_RELATIVE) != null )
      h3res += Math.max(-2, Math.min( 2, (Integer) clusteringParams.get(HEXBIN_RESOLUTION_RELATIVE)));

     return Math.min( Math.min( h3res, defaultResForLevel + overzoomingRes ) , 13 ); // cut to maximum res
    }

  private SQLQuery buildHexbinClusteringQuery(GetFeaturesByBBoxEvent event) {
    BBox bbox = event.getBbox();
    Map<String, Object> clusteringParams = event.getClusteringParams();

        int zLevel = (event instanceof GetFeaturesByTileEvent ? ((GetFeaturesByTileEvent) event).getLevel() : bbox2zoom(bbox)),
            defaultResForLevel = zoom2resolution(zLevel),
            h3Resolution = evalH3Resolution( clusteringParams, defaultResForLevel );

        if( zLevel == 1)  // prevent ERROR:  Antipodal (180 degrees long) edge detected!
         if( bbox.minLon() == 0.0 )
          bbox.setEast( bbox.maxLon() - 0.0001 );
         else
          bbox.setWest( bbox.minLon() + 0.0001);

        String statisticalProperty = (String) clusteringParams.get(HEXBIN_PROPERTY);
        boolean statisticalPropertyProvided = (statisticalProperty != null && statisticalProperty.length() > 0),
                h3cflip = clusteringParams.get(HEXBIN_POINTMODE) == Boolean.TRUE;
               /** todo: replace format %.14f with parameterized version e.g. re-use #buildGeoFilterFromBbox()*/
        final String expBboxSql = DhString
                .format("st_envelope( st_buffer( ST_MakeEnvelope(%.14f,%.14f,%.14f,%.14f, 4326)::geography, ( 2.5 * edgeLengthM( %d )) )::geometry )",
                        bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat(), h3Resolution);

        /*clippedGeo - passed bbox is extended by "margin" on service level */
        String clippedGeo = (!event.getClip() ? "geo" : DhString
                .format("ST_Intersection(st_makevalid(geo),ST_MakeEnvelope(%.14f,%.14f,%.14f,%.14f,4326) )", bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat())),
                fid = (!event.getClip() ? "h3" : DhString.format("h3 || %f || %f", bbox.minLon(), bbox.minLat()));


      return buildH3Query(event, h3Resolution, h3cflip, clippedGeo, statisticalPropertyProvided, zLevel, fid,
        expBboxSql, defaultResForLevel);
    }

  private SQLQuery buildH3Query(GetFeaturesByBBoxEvent event, int h3Resolution, boolean h3cflip, String clippedGeo,
      boolean statisticalPropertyProvided, int zLevel, String fid, String expBboxSql, int defaultResForLevel) {

    String h3Sql =
        "  select "
            + "  ( "
            + "   select row_to_json(ftr) from "
            + "   (  "
            + "    select  "
            + "     'Feature'::text as type, "
            + "     left(md5(#{fid}),15) as id, "
            + "     ( select row_to_json( prop ) "
            + "       from "
            + "       ( select 'H3'::text as kind, "
            + "                 h3 as kind_detail, "
            + "                 #{h3Resolution}::integer as resolution, "
            + "                 #{zLevel}::integer as level, "
            + "                 ${{aggregationField}} as aggregation, "
            + "                 h3IsPentagon( ('x' || h3 )::bit(64)::bigint ) as pentagon, "
            + "                 st_asgeojson( ${{geoResult}}, 7 )::json#>'{coordinates}' as ${{geoResultFieldName}} "
            + "       ) prop "
            + "     ) as properties "
            + "    ) ftr "
            + "  )::jsonb as jsondata, "
            + "  ${{geo}} as geo "
            + "  from "
            + "  ( "
            + "   with h3cluster as "
            + "   ( select oo.h3, "
            + "           ( select row_to_json( t1 ) from (select qty${{statistics}}) t1 ) as agg, "
            + "                    st_containsproperly(${{geoFilter}}, oo.geo) as omni,"
            + "                    oo.geo "
            + "              FROM ${{h3InnerQuery}} "
            + "            ) in_data "
            + "          ) q2 "
            + "          group by px, py "
            + "        ) a_data "
            + "       ) c "
            + "      ) cc "
            + "      group by h3 "
            + "     ) oo "
            + "     where 1 = 1 "
            + "   ) "
            + "   select * from h3cluster "
            + "   where 1 = 1 "
            + "     and omni = true "
            + "     ${{filterEmptyGeo}} "
            + "  ) outer_v LIMIT #{limit}";

    final SQLQuery query = new SQLQuery(h3Sql)
        .withNamedParameter("h3Resolution", h3Resolution)
        .withQueryFragment("geoResult", !h3cflip ? "st_centroid(geo)" : "geo")
        .withQueryFragment("geo", new SQLQuery(getResponseType(event) == GEO_JSON ? "st_asgeojson(${{innerGeo}}, 7)::json" : "(${{innerGeo}})")
            .withQueryFragment("innerGeo", h3cflip ? "st_centroid(geo)" : clippedGeo))
        .withQueryFragment("statistics", statisticalPropertyProvided ? ", min, max, sum, avg, median" : "")
        .withNamedParameter("zLevel", zLevel)
        .withQueryFragment("geoResultFieldName", !h3cflip ? "centroid" : "hexagon")
        .withNamedParameter("fid", fid)
        .withQueryFragment("geoFilter", expBboxSql)
        .withQueryFragment("filterEmptyGeo", event.getClip() ? DhString.format(" and not st_isempty( %s ) ", clippedGeo) : "")
        .withNamedParameter("limit", event.getLimit());

    Map<String, Object> clusteringParams = event.getClusteringParams();
    String statisticalProperty = (String) clusteringParams.get(HEXBIN_PROPERTY);

    if (statisticalPropertyProvided)
      query.setQueryFragment("aggregationField", new SQLQuery("jsonb_set('{}'::jsonb, #{path}, agg::jsonb)::json")
          .withNamedParameter("path", new String[]{statisticalProperty}));
    else
      query.setQueryFragment("aggregationField", "agg");

    boolean pointmode = clusteringParams.get(HEXBIN_SINGLECOORD) == Boolean.TRUE;
    int pxSize = adjPixelSize( h3Resolution, defaultResForLevel );
    int samplingStrength = samplingStrengthFromText((String) clusteringParams.getOrDefault(HEXBIN_SAMPLING, "off"),false);
    String samplingCondition =  samplingStrength <= 0 ? "1 = 1" : TweaksSQL.strengthSql( samplingStrength, true);

    SQLQuery h3InnerQuery = new SQLQuery(h3sqlMid_0 + " ${{pointModeOrStandardFlavor}} " + h3sqlMid_2 + " ${{searchQuery}}")
        .withVariable(SCHEMA, getSchema())
        .withVariable("headTable", getDefaultTable((E) event) + HEAD_TABLE_SUFFIX)
        .withQueryFragment("pointModeOrStandardFlavor", !pointmode ? h3sqlMid_1a : h3sqlMid_1b)
        .withNamedParameter("h3Resolution", h3Resolution)
        .withQueryFragment("statisticalValue", statisticalPropertyProvided
            ? new SQLQuery("(jsondata#>> #{path})::numeric").withNamedParameter("path",
              Streams.concat(Stream.of("properties"), Arrays.stream(statisticalProperty.split("\\."))).toArray(String[]::new))
            : new SQLQuery("(0.0)::numeric"))
        .withNamedParameter("zLevel", zLevel)
        .withNamedParameter("pxSize", pxSize)
        .withQueryFragment("geoFilter", expBboxSql)
        .withQueryFragment("samplingCondition", samplingCondition);

    final SQLQuery searchQuery = generateSearchQuery(event);
    if (searchQuery != null)
      h3InnerQuery.setQueryFragment("searchQuery", new SQLQuery("AND ${{innerSearchQuery}}")
          .withQueryFragment("innerSearchQuery", searchQuery));
    else
      h3InnerQuery.setQueryFragment("searchQuery", "");

    query
        .withQueryFragment("h3InnerQuery", h3InnerQuery);

    return query;
  }

  public SQLQuery buildQuadbinClusteringQuery(GetFeaturesByBBoxEvent event, PSQLXyzConnector dbHandler)
      throws ErrorResponseException {

    final Map<String, Object> clusteringParams = event.getClusteringParams();
    int relResolution = ( clusteringParams.get(QUADBIN_RESOLUTION) != null ? (int) clusteringParams.get(QUADBIN_RESOLUTION) :
        ( clusteringParams.get(QUADBIN_RESOLUTION_RELATIVE) != null ? (int) clusteringParams.get(QUADBIN_RESOLUTION_RELATIVE) : 0)),
        absResolution = clusteringParams.get(QUADBIN_RESOLUTION_ABSOLUTE) != null ? (int) clusteringParams.get(QUADBIN_RESOLUTION_ABSOLUTE) : 0;
    final String countMode = (String) clusteringParams.get(QUADBIN_COUNTMODE);
    final boolean noBuffer = (boolean) clusteringParams.getOrDefault(QUADBIN_NOBUFFER,false);


    checkQuadbinInput(countMode, relResolution, event, dbHandler);
    BBox bbox = event.getBbox();
        boolean isTileRequest = (event instanceof GetFeaturesByTileEvent) && ((GetFeaturesByTileEvent) event).getMargin() == 0,
                clippedOnBbox = (!isTileRequest && event.getClip());

        final WebMercatorTile tile = getTileFromBbox(bbox);

        if( (absResolution - tile.level) >= 0 )  // case of valid absResolution convert it to a relative resolution and add both resolutions
         relResolution = Math.min( relResolution + (absResolution - tile.level), 5);

        return generateQuadbinClusteringSQL(event, relResolution, countMode, tile, bbox, isTileRequest, clippedOnBbox, noBuffer, getResponseType(event) == GEO_JSON);
    }

    //------------------------- Quadbin related -------------------------

  /**
   * MIXED-mode only supports tables with lower than LIMIT_MIXED_MODE records
   */
  private static final Integer LIMIT_COUNTMODE_MIXED = 6000000;
  private static final String QUADBIN_NOBUFFER = "noBuffer";
  private static final String QUADBIN_COUNTMODE = "countmode";
  private static final String HEXBIN_RESOLUTION_RELATIVE = "relativeResolution";
  private static final String QUADBIN_RESOLUTION_RELATIVE = HEXBIN_RESOLUTION_RELATIVE;
  private static final String HEXBIN_RESOLUTION_ABSOLUTE = "absoluteResolution";
  private static final String QUADBIN_RESOLUTION_ABSOLUTE = HEXBIN_RESOLUTION_ABSOLUTE;
  private static final String HEXBIN_RESOLUTION = "resolution";
  private static final String QUADBIN_RESOLUTION = HEXBIN_RESOLUTION;
  private static final String QUAD = "quadbin";

  /**
   * @deprecated Please do not use this method for any new purposes.
   * NOTE: This method works only if the query has no other parameter types than STRING.
   * @param query
   * @return
   */
  @Deprecated
  private static String substituteCompletely(SQLQuery query) {
    String sql = query.substitute().text();
    for (Object param : query.parameters())
      sql = sql.replaceFirst("\\?", "'" + param + "'");
    return sql;
  }


  /**
   * Creates the SQLQuery for Quadbin requests.
   */
  private SQLQuery generateQuadbinClusteringSQL(GetFeaturesByBBoxEvent event, int resolution, String quadMode, WebMercatorTile tile,
      BBox bbox, boolean isTileRequest, boolean clippedOnBbox, boolean noBuffer, boolean convertGeo2Geojson) {
    int effectiveLevel = tile.level + resolution;

    double bufferSizeInDeg = tile.getBBox(false).widthInDegree(true) / (Math.pow(2, resolution) *  1024.0);
    String realCountCondition = "",
        boolCountCondition = "FALSE",
        _pureEstimation = "select sum( ti.tbl_est_cnt * xyz_postgis_selectivity( ti.tbloid, 'geo',qkbbox) )::bigint from tblinfo ti",
        pureEstimation = "",
        resultQkGeo = (!noBuffer ? DhString.format("ST_Buffer(qkbbox, -%f)",bufferSizeInDeg) : "qkbbox"),
        geoPrj  = ( convertGeo2Geojson ? "ST_AsGeojson( qkgeo , 8 )::jsonb" : "qkgeo" ),
        bboxSql = DhString.format( DhString.format("ST_MakeEnvelope(%%.%1$df,%%.%1$df,%%.%1$df,%%.%1$df, 4326)", 14 /*GEOMETRY_DECIMAL_DIGITS*/), bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat() ),
        coveringQksSql = ( !isTileRequest ? DhString.format("select xyz_qk_lrc2qk(rowy,colx,level) as qk from xyz_qk_envelope2lrc( %s, %d)", bboxSql, effectiveLevel)
            :" SELECT unnest(xyz_qk_child_calculation('"+(tile.asQuadkey() == null ? 0 :tile.asQuadkey())+"',"+resolution+",null)) as qk" );
    if( clippedOnBbox )
      resultQkGeo = DhString.format("ST_Intersection(%s,%s)", resultQkGeo, bboxSql);

    if(quadMode == null)
      quadMode = COUNTMODE_MIXED;

    switch (quadMode) {
      case COUNTMODE_BOOL:
        boolCountCondition = "TRUE";
      case COUNTMODE_REAL:
        realCountCondition = "TRUE";
        pureEstimation = _pureEstimation;
        break;
      case COUNTMODE_ESTIMATED:
      case COUNTMODE_MIXED:

        realCountCondition = quadMode.equalsIgnoreCase(COUNTMODE_MIXED) ? "cond_est_cnt < 100 AND est_cnt < "+ LIMIT_COUNTMODE_MIXED : "FALSE";

        if (event.getPropertiesQuery() != null) {
          pureEstimation =
              "  SELECT xyz_count_estimation(concat(" +
                  "      'select 1 from ${schema}.${headTable}"+
                  "       WHERE ST_Intersects(geo, xyz_qk_qk2bbox(''',qk,''')) "+
                  " AND "+
                  substituteCompletely(generatePropertiesQuery(event)).replaceAll("'","''")+
                  "'))";
        }
        else
          pureEstimation = _pureEstimation;
        break;
    }

    return new SQLQuery(
        /*cte begin*/
        "with  "+
            "tblinfo as "+
            "( select "+
            "   coalesce(c2.oid, c1.oid) as tbloid,"+
            "   coalesce(c2.relname, c1.relname) as tblname, "+
            "   coalesce(c2.reltuples, c1.reltuples)::bigint as tbl_est_cnt "+
            "   from pg_class c1 "+
            "   left join pg_inherits pm on (c1.oid = pm.inhparent) "+
            "   left join pg_class c2 on (c2.oid = pm.inhrelid) "+
            "   where c1.oid = ('${schema}.${headTable}')::regclass "+
            "), "+
            "tbl_stats as ( select sum(tbl_est_cnt) as est_cnt from tblinfo ), "+
            "quadkeys  as ( "+ coveringQksSql + " ), "+
            "quaddata  as ( select qk, xyz_qk_qk2bbox( qk ) as qkbbox, ( select array[r.level,r.colx,r.rowy] from xyz_qk_qk2lrc(qk) r ) as qkxyz from quadkeys ), "+
            "qk_stats  as ( select ("+boolCountCondition+") as bool_condition, ("+realCountCondition+") as real_condition, * from (select *, ("+pureEstimation+") as cond_est_cnt, floor(est_cnt/pow(4,qkxyz[1]))::bigint as equi_cnt from tbl_stats, quaddata ) a ) "+
            /*cte end*/
            "SELECT jsondata, "+ geoPrj +" as geo from ("+
            "SELECT  "+
            "    ( select row_to_json( ftr ) from "+
            "	  ( select 'Feature'::text as type, ('x' || left(md5(qk), 15))::bit(60)::bigint::text as id, "+
            "	    ( select row_to_json( prop ) from "+
            "		  ( select cnt_bbox_est as count, qk, qkxyz as zxy, row(qkxyz[3],qkxyz[2],qkxyz[1])::text as xyz, NOT(real_condition) as estimated, est_cnt::bigint as total_count, equi_cnt as equipartition_count "+
            "		  ) prop "+
            "		) as properties "+
            "	  ) ftr "+
            "    )::jsonb as jsondata,   "+
            "    (CASE WHEN cnt_bbox_est != 0"+
            "        THEN"+
            "            " + resultQkGeo +
            "        ELSE"+
            "            NULL::geometry"+
            "        END "+
            "    ) as qkgeo"+
            "    FROM "+
            "    (SELECT real_condition,est_cnt,equi_cnt,qk,qkbbox,qkxyz,"+
            "        ("+
            "        CASE"+
            "         WHEN bool_condition THEN "+
            "          CASE WHEN EXISTS (select 1 from ${schema}.${headTable} where ST_Intersects(geo, qkbbox) AND ${{propertiesQuery}})"+
            "           THEN 1::bigint "+
            "           ELSE 0::bigint "+
            "          END"+
            "         WHEN real_condition THEN "+
            "           (select count(1) from ${schema}.${headTable} where ST_Intersects(geo, qkbbox) AND ${{propertiesQuery}})"+
            "         ELSE "+
            "           cond_est_cnt "+
            "        END )::bigint as cnt_bbox_est"+
            "    FROM qk_stats "+
            "    ) c"+
            ") x WHERE qkgeo IS NOT null ")
        .withVariable(SCHEMA, getSchema())
        .withVariable("headTable", getDefaultTable((E) event) + HEAD_TABLE_SUFFIX)
        .withQueryFragment("propertiesQuery", event.getPropertiesQuery() != null ? generatePropertiesQuery(event) : new SQLQuery("TRUE"));
  }

  //------------------------- Hexbin / H3 related -------------------------

  private static final String HEXBIN_SAMPLING = "sampling";
  private static final String HEXBIN_SINGLECOORD = "singlecoord";
  private static final String HEXBIN_POINTMODE = "pointmode";
  private static final String HEXBIN_PROPERTY = "property";
  private static final String HEXBIN = "hexbin";
  private static int[] MaxResForZoom = {2, 2, 2, 2, 3, 4, 4, 5, 6, 6, 7, 8, 9, 9, 10, 11, 11, 12, 13, 13, 13, 13, 13};
  private static String h3sqlMid_2 =
      "              where 1 = 1 and st_intersects(geo , ${{geoFilter}}) and ${{samplingCondition}}";
  private static String h3sqlMid_1b = // convert to points flavour
      "              select ${{statisticalValue}} as cval, st_geometryn(st_points( ST_Intersection(v.geo, ${{geoFilter}}) ) ,1) as refpt"
          + "              from ${schema}.${headTable} v ";
  private static String h3sqlMid_1a = // standard flavour
      "              select ${{statisticalValue}} as cval, coalesce( l.geoh3, v.geo ) as refpt"
          + "              from ${schema}.${headTable} v "
          + "               left join lateral "
          + "                ( select st_force3d(st_setsrid( h3ToGeoDeg( coveringDeg( case ST_Within(geo, ${{geoFilter}}) "
          + "                                                                          when true then ST_MakeValid(geo) "
          + "                                                                          else ST_Intersection( ST_MakeValid(geo), ${{geoFilter}}) "
          + "                                                                         end, #{h3Resolution}::INTEGER)), st_srid(geo))) "
          + "                  where st_geometrytype(v.geo) != 'ST_Point'"
          + "                ) l(geoh3) "
          + "                on ( true ) ";
  private static String h3sqlMid_0 =
      "     ( "
          + "      select to_hex(cc.h3) as h3,"
          + "              count(1) as qty,"
          + "              min(cc.unnest) as min,"
          + "              max(cc.unnest) as max,"
          + "              sum(cc.unnest) as sum,"
          + "              round( avg(cc.unnest ),5) as avg,  "
          + "              percentile_cont(0.5) within group (order by cc.unnest) as median,"
          + "              h3togeoboundarydeg(cc.h3)::geometry(Polygon, 4326) AS geo"
          + "      from "
          + "      ( select c.h3, unnest(c.cvals) "
          + "      from "
          + "      ( "
          + "        select a_data.cvals, "
          + "        coveringDeg(a_data.refpt, #{h3Resolution}::INTEGER) as h3 "
          + "        from "
          + "        ( "
          + "          select "
          + "             array_agg(cval) as cvals, "
          + "             (case "
          + "               when q2.lon < -180 then 0 "
          + "               when q2.lon > 180  then power(2, (#{zLevel}::INTEGER)) * #{pxSize}::INTEGER "
          + "               else floor((q2.lon + 180.0) / 360.0 * power(2, (#{zLevel}::INTEGER)) * #{pxSize}::INTEGER) "
          + "              end "
          + "             )::bigint as px, "
          + "             (case "
          + "               when q2.lat <= -85 then power(2, (#{zLevel}::INTEGER)) * #{pxSize}::INTEGER "
          + "               when q2.lat >= 85  then 0 "
          + "               else floor((1.0 - ln(tan(radians(q2.lat)) + (1.0 / cos(radians(q2.lat)))) / pi()) / 2 * power(2, (#{zLevel}::INTEGER)) * #{pxSize}::INTEGER) "
          + "              end "
          + "              )::bigint as py, "
          + "              ST_Centroid( ST_ConvexHull( st_collect( refpt ) ) ) as refpt "
          + "          from "
          + "          ( "
          + "            select cval, st_x(in_data.refpt) as lon, st_y(in_data.refpt) as lat, refpt "
          + "            from "
          + "            ( ";

  private static int zoom2resolution(int zoom) {
    return (MaxResForZoom[zoom]);
  }

  private static int bbox2zoom(BBox bbox) {
    return ((int) Math.round((5.88610403145016 - Math.log(bbox.maxLon() - bbox.minLon())) / 0.693147180559945));
  }

  private static int adjPixelSize(int h3res, int maxResForLevel)
  {
    int pxSize = 64,   // 64 in general, but clashes when h3 res is to fine grained (e.g. resulting in holes in hex areas).
        pdiff = (h3res - maxResForLevel);

    if(  pdiff == 2 ) pxSize = 160;
    else if (  pdiff >= 3 ) pxSize = 256;

    return( pxSize );
  }
}
