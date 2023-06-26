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
package com.here.naksha.lib.psql.sql;

import com.here.naksha.lib.core.models.geojson.WebMercatorTile;
import com.here.naksha.lib.core.models.geojson.coordinates.BBox;
import com.here.naksha.lib.core.models.payload.events.clustering.ClusteringQuadBin.CountMode;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class QuadbinSQL {
  /** Real live counts via count(*) */
  public static final String COUNTMODE_REAL = "real";
  /** Estimated counts, determined with _postgis_selectivity() or EXPLAIN Plan analyze */
  public static final String COUNTMODE_ESTIMATED = "estimated";
  /** Combination of real and estimated. */
  public static final String COUNTMODE_MIXED = "mixed";

  public static final String QUAD = "quadbin";
  public static final String QUADBIN_RESOLUTION = H3SQL.HEXBIN_RESOLUTION;
  public static final String QUADBIN_RESOLUTION_ABSOLUTE = H3SQL.HEXBIN_RESOLUTION_ABSOLUTE;
  public static final String QUADBIN_RESOLUTION_RELATIVE = H3SQL.HEXBIN_RESOLUTION_RELATIVE;
  public static final String QUADBIN_COUNTMODE = "countmode";
  public static final String QUADBIN_NOBOFFER = "noBuffer";

  /** MIXED-mode only supports tables with lower than LIMIT_MIXED_MODE records */
  public static final Integer LIMIT_COUNTMODE_MIXED = 6000000;

  /** Creates the SQLQuery for Quadbin requests. */
  public static SQLQuery generateQuadbinClusteringSQL(
      int resolution,
      @Nullable CountMode countMode,
      @Nullable String propQuery,
      @NotNull WebMercatorTile tile,
      @NotNull BBox bbox,
      boolean isTileRequest,
      boolean clippedOnBbox,
      boolean noBuffer,
      boolean convertGeo2Geojson) {
    SQLQuery query = new SQLQuery("");

    int effectiveLevel = tile.level + resolution;

    double bufferSizeInDeg = tile.getBBox(false).widthInDegree(true) / (Math.pow(2, resolution) * 1024.0);
    String realCountCondition = "",
        _pureEstimation =
            "select sum( ti.tbl_est_cnt * xyz_postgis_selectivity( ti.tbloid, 'geo',qkbbox)"
                + " )::bigint from tblinfo ti",
        pureEstimation = "",
        resultQkGeo = (!noBuffer ? DhString.format("ST_Buffer(qkbbox, -%f)", bufferSizeInDeg) : "qkbbox"),
        geoPrj = (convertGeo2Geojson ? "ST_AsGeojson( qkgeo , 8 )::jsonb" : "qkgeo"),
        bboxSql =
            DhString.format(
                DhString.format(
                    "ST_MakeEnvelope(%%.%1$df,%%.%1$df,%%.%1$df,%%.%1$df, 4326)",
                    14 /*GEOMETRY_DECIMAL_DIGITS*/),
                bbox.minLon(),
                bbox.minLat(),
                bbox.maxLon(),
                bbox.maxLat()),
        coveringQksSql =
            (!isTileRequest
                ? DhString.format(
                    "select xyz_qk_lrc2qk(rowy,colx,level) as qk from xyz_qk_envelope2lrc( %s, %d)",
                    bboxSql, effectiveLevel)
                : " SELECT unnest(xyz_qk_child_calculation('"
                    + (tile.asQuadkey() == null ? 0 : tile.asQuadkey())
                    + "',"
                    + resolution
                    + ",null)) as qk");
    if (clippedOnBbox) resultQkGeo = DhString.format("ST_Intersection(%s,%s)", resultQkGeo, bboxSql);

    if (countMode == null) countMode = CountMode.MIXED;

    switch (countMode) {
      case REAL:
        realCountCondition = "TRUE";
        pureEstimation = _pureEstimation;
        break;
      case ESTIMATED:
      case MIXED:
        realCountCondition = countMode == CountMode.MIXED
            ? "cond_est_cnt < 100 AND est_cnt < " + LIMIT_COUNTMODE_MIXED
            : "FALSE";

        if (propQuery != null) {
          pureEstimation = "  SELECT xyz_count_estimation(concat("
              + "      'select 1 from ${schema}.${table}"
              + "       WHERE ST_Intersects(geo, xyz_qk_qk2bbox(''',qk,''')) "
              + " AND "
              + propQuery.replaceAll("'", "''")
              + "'))";
        } else {
          pureEstimation = _pureEstimation;
        }
        break;
    }

    if (propQuery == null) propQuery = "(1 = 1)";

    query.append(
        /*cte begin*/
        "with  "
            + "tblinfo as "
            + "( select "
            + "   coalesce(c2.oid, c1.oid) as tbloid,"
            + "   coalesce(c2.relname, c1.relname) as tblname, "
            + "   coalesce(c2.reltuples, c1.reltuples)::bigint as tbl_est_cnt "
            + "   from pg_class c1 "
            + "   left join pg_inherits pm on (c1.oid = pm.inhparent) "
            + "   left join pg_class c2 on (c2.oid = pm.inhrelid) "
            + "   where c1.oid = ('${schema}.${table}')::regclass "
            + "), "
            + "tbl_stats as ( select sum(tbl_est_cnt) as est_cnt from tblinfo ), "
            + "quadkeys as ( "
            + coveringQksSql
            + " ), quaddata as ( select qk, xyz_qk_qk2bbox( qk ) as qkbbox, ( select"
            + " array[r.level,r.colx,r.rowy] from xyz_qk_qk2lrc(qk) r ) as qkxyz from quadkeys ),"
            + " qk_stats  as ( select ("
            + realCountCondition
            + ") as real_condition, * from (select *, ("
            + pureEstimation
            + ") as cond_est_cnt, floor(est_cnt/pow(4,qkxyz[1]))::bigint as equi_cnt from"
            + " tbl_stats, quaddata ) a ) "
            +
            /*cte end*/
            "SELECT jsondata, "
            + geoPrj
            + " as geo from (SELECT      ( select row_to_json( ftr ) from 	  ( select"
            + " 'Feature'::text as type, ('x' || left(md5(qk), 15))::bit(60)::bigint::text as id, 	"
            + "    ( select row_to_json( prop ) from 		  ( select cnt_bbox_est as count, qk, qkxyz"
            + " as zxy, row(qkxyz[3],qkxyz[2],qkxyz[1])::text as xyz, NOT(real_condition) as"
            + " estimated, est_cnt::bigint as total_count, equi_cnt as equipartition_count 		  )"
            + " prop 		) as properties 	  ) ftr     )::jsonb as jsondata,       (CASE WHEN"
            + " cnt_bbox_est != 0        THEN            "
            + resultQkGeo
            + "        ELSE            NULL::geometry        END     ) as qkgeo    FROM     (SELECT"
            + " real_condition,est_cnt,equi_cnt,qk,qkbbox,qkxyz,        (        CASE WHEN"
            + " real_condition THEN             (select count(1) from ${schema}.${table} where"
            + " ST_Intersects(geo, qkbbox) and "
            + propQuery
            + ")"
            + "        ELSE "
            + "          cond_est_cnt "
            + "        END)::bigint as cnt_bbox_est"
            + "    FROM qk_stats "
            + "    ) c"
            + ") x WHERE qkgeo IS NOT null ");

    return query;
  }
}
