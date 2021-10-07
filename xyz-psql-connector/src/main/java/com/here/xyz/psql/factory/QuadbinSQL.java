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

package com.here.xyz.psql.factory;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.models.geojson.WebMercatorTile;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.psql.Capabilities;
import com.here.xyz.psql.PSQLXyzConnector;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.responses.XyzError;

public class QuadbinSQL {

    public static final String QUAD = "quadbin";
    public static final String QUADBIN_RESOLUTION = H3SQL.HEXBIN_RESOLUTION;
    public static final String QUADBIN_RESOLUTION_ABSOLUTE = H3SQL.HEXBIN_RESOLUTION_ABSOLUTE;
    public static final String QUADBIN_RESOLUTION_RELATIVE = H3SQL.HEXBIN_RESOLUTION_RELATIVE;
    public static final String QUADBIN_COUNTMODE = "countmode";
    public static final String QUADBIN_NOBOFFER = "noBuffer";


    /**
     * Real live counts via count(*)
     */
    private static final String COUNTMODE_REAL = "real";
    /**
     * Estimated counts, determined with _postgis_selectivity() or EXPLAIN Plan analyze
     */
    private static final String COUNTMODE_ESTIMATED = "estimated";
    /**
     * Combination of real and estimated.
     */
    private static final String COUNTMODE_MIXED = "mixed";
    /**
     * MIXED-mode only supports tables with lower than LIMIT_MIXED_MODE records
     */
    private static final Integer LIMIT_COUNTMODE_MIXED = 6000000;

    /**
     * Check if request parameters are valid. In case of invalidity throw an Exception
     */
    public static void checkQuadbinInput(String countMode, int relResolution, GetFeaturesByBBoxEvent event, String spaceId, String streamId, PSQLXyzConnector connector) throws ErrorResponseException
    {
        if(countMode != null && (!countMode.equalsIgnoreCase(QuadbinSQL.COUNTMODE_REAL) && !countMode.equalsIgnoreCase(QuadbinSQL.COUNTMODE_ESTIMATED) && !countMode.equalsIgnoreCase(QuadbinSQL.COUNTMODE_MIXED)) )
            throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT,
                    "Invalid request parameters. Unknown clustering.countmode="+countMode+". Available are: ["+ QuadbinSQL.COUNTMODE_REAL +","+ QuadbinSQL.COUNTMODE_ESTIMATED +","+ QuadbinSQL.COUNTMODE_MIXED +"]!");

        if(relResolution > 5)
            throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT,
                    "Invalid request parameters. clustering.relativeResolution="+relResolution+" to high. 5 is maximum!");

        if(event.getPropertiesQuery() != null && event.getPropertiesQuery().get(0).size() != 1)
            throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT,
                    "Invalid request parameters. Only one Property is allowed");

        if (!Capabilities.canSearchFor(spaceId, event.getPropertiesQuery(), connector)) {
            throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT,
                    "Invalid request parameters. Search for the provided properties is not supported for this resource.");
        }
    }

    /**
     * Creates the SQLQuery for Quadbin requests.
     */
    public static SQLQuery generateQuadbinClusteringSQL(String schema, String space, int resolution, String quadMode, String propQuery, WebMercatorTile tile, BBox bbox, boolean isTileRequest, boolean clippedOnBbox, boolean noBuffer, boolean convertGeo2Geojson ) {
        SQLQuery query = new SQLQuery("");

        int effectiveLevel = tile.level + resolution;

        double bufferSizeInDeg = tile.getBBox(false).widthInDegree(true) / (Math.pow(2, resolution) *  1024.0);
        String realCountCondition = "",
               _pureEstimation = "xyz_postgis_selectivity( '"+schema+".\""+space+"\"'::regclass, 'geo',qkbbox) ",
               pureEstimation = "",
               estCalc = "cond_est_cnt",
               resultQkGeo = (!noBuffer ? String.format("ST_Buffer(qkbbox, -%f)",bufferSizeInDeg) : "qkbbox"),
               geoPrj  = ( convertGeo2Geojson ? "ST_AsGeojson( qkgeo , 8 )::jsonb" : "qkgeo" ),
               bboxSql = String.format( String.format("ST_MakeEnvelope(%%.%1$df,%%.%1$df,%%.%1$df,%%.%1$df, 4326)", 14 /*GEOMETRY_DECIMAL_DIGITS*/), bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat() ),
               coveringQksSql = ( !isTileRequest ? String.format("select xyz_qk_lrc2qk(rowy,colx,level) as qk from xyz_qk_envelope2lrc( %s, %d)", bboxSql, effectiveLevel)
                                                 :" SELECT unnest(xyz_qk_child_calculation('"+(tile.asQuadkey() == null ? 0 :tile.asQuadkey())+"',"+resolution+",null)) as qk" );
        if( clippedOnBbox )
         resultQkGeo = String.format("ST_Intersection(%s,%s)",resultQkGeo,bboxSql);


        if(quadMode == null)
            quadMode = QuadbinSQL.COUNTMODE_MIXED;

        switch (quadMode) {
            case QuadbinSQL.COUNTMODE_REAL:
                realCountCondition = "TRUE";
                pureEstimation = _pureEstimation;
                break;
            case QuadbinSQL.COUNTMODE_ESTIMATED:
            case QuadbinSQL.COUNTMODE_MIXED:

                if(quadMode.equalsIgnoreCase(QuadbinSQL.COUNTMODE_MIXED)) {
                    if (propQuery != null) {
                        realCountCondition = "cond_est_cnt < 100 AND est_cnt < "+ LIMIT_COUNTMODE_MIXED;
                    } else {
                        realCountCondition = "cond_est_cnt < (1000 / (est_cnt+1)) AND est_cnt < "+ LIMIT_COUNTMODE_MIXED;
                    }
                }else
                    realCountCondition = "FALSE";

                if(propQuery != null){
                    pureEstimation =
                                    "  SELECT xyz_count_estimation(concat(" +
                                    "      'select 1 from "+ schema+".\""+space+"\""+
                                    "       WHERE ST_Intersects(geo, xyz_qk_qk2bbox(''',qk,''')) "+
                                    " AND "+
                                    propQuery.replaceAll("'","''")+
                                    "'))";
                    estCalc = "cond_est_cnt"; // "(CASE WHEN cond_est_cnt <= 1 THEN 0 ELSE cond_est_cnt END)";
                }else{
                    pureEstimation = _pureEstimation;
                    estCalc += " * est_cnt ";
                }
                break;
        }

        query.append(
                "WITH stats AS("+
                        "    select sum( coalesce( c2.reltuples, c1.reltuples ) )::bigint as est_cnt "+
                        "    from pg_class c1 left join pg_inherits pm on ( c1.oid = pm.inhparent ) left join pg_class c2 on ( c2.oid = pm.inhrelid ) "+
                        "    where c1.oid = '"+schema+".\""+space+"\"'::regclass"+
                        ")"+
                        "SELECT jsondata, "+ geoPrj +" as geo from ("+
                        "SELECT  (SELECT concat('{\"id\": \"', ('x' || left(md5(qk),15) )::bit(60)::bigint ,'\", \"type\": \"Feature\""+
                        "       ,\"properties\": {\"count\": ',cnt_bbox_est,',\"qk\":\"',qk,'\""+
                        "       ,\"xyz\":\"',qkxyz,'\" ,\"estimated\":',to_jsonb(NOT("+realCountCondition+")),',\"total_count\":',est_cnt::bigint,',\"equipartition_count\":',"+
                        "          (floor((est_cnt/POW(2,"+(tile.level+1)+")/POW(4,"+resolution+")))),'}}')::jsonb) as jsondata,"+
                        "    (CASE WHEN cnt_bbox_est != 0"+
                        "        THEN"+
                        "            " + resultQkGeo +
                        "        ELSE"+
                        "            NULL::geometry"+
                        "        END "+
                        "    ) as qkgeo"+
                        "    FROM "+
                        "    (SELECT cond_est_cnt,est_cnt,qk,qkbbox,qkxyz,"+
                        "        ("+
                        "        CASE WHEN "+realCountCondition+" THEN "+
                        "            (select count(1) from "+ schema+".\""+space+"\""+
                        "                WHERE ST_Intersects(geo, qkbbox)");
        if(propQuery != null) {
            query.append(" AND ");
            query.append(propQuery);
        }
        query.append(")"+
                "         ELSE "+
                "          "+estCalc+""+
                "        END)::bigint as cnt_bbox_est"+
                "    FROM stats,"+
                "        (SELECT *,"+
                "              ("+pureEstimation+") as cond_est_cnt "+
                "            from("+
                "            SELECT qk, xyz_qk_qk2bbox( qk ) as qkbbox, xyz_qk_qk2lrc(qk) as qkxyz from ("+ coveringQksSql + ") a"+
                "        ) b"+
                "    )c"+
                ")x ) d WHERE qkgeo IS NOT null ");
        return query;
    }
}
