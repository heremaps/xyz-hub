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

package com.here.naksha.lib.psql.sql;

import com.here.naksha.lib.core.models.geojson.coordinates.BBox;

public class H3SQL {
  public static final String HEXBIN = "hexbin";
  public static final String HEXBIN_RESOLUTION = "resolution";
  public static final String HEXBIN_RESOLUTION_ABSOLUTE = "absoluteResolution";
  public static final String HEXBIN_RESOLUTION_RELATIVE = "relativeResolution";
  public static final String HEXBIN_PROPERTY = "property";
  public static final String HEXBIN_POINTMODE = "pointmode";
  public static final String HEXBIN_SINGLECOORD = "singlecoord";
  public static final String HEXBIN_SAMPLING = "sampling";

  public static String
      h3sqlBegin =
          "  select "
              + "  ( "
              + "   select row_to_json(ftr) from "
              + "   (  "
              + "    select  "
              + "     'Feature'::text as type, "
              + "     left(md5( %8$s ),15) as id, "
              + "     ( select row_to_json( prop ) "
              + "       from "
              + "       ( select 'H3'::text as kind, "
              + "                 h3 as kind_detail, "
              + "                 %1$d as resolution, "
              + "                 %5$d as level, "
              + "                 %7$s as aggregation, "
              + "                 h3IsPentagon( ('x' || h3 )::bit(64)::bigint ) as pentagon, "
              + "                 st_asgeojson( %2$s, 7 )::json#>'{coordinates}' as %6$s "
              + "       ) prop "
              + "     ) as properties "
              + "    ) ftr "
              + "  )::jsonb as jsondata, "
              + "  %3$s as geo "
              + "  from "
              + "  ( "
              + "   with h3cluster as "
              + "   ( select oo.h3, "
              + "           ( select row_to_json( t1 ) from ( select qty %4$s ) t1 ) as agg, "
              + "                    st_containsproperly( %9$s, oo.geo ) as omni,"
              + "                    oo.geo "
              + "     from ",
      h3sqlMid_0 =
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
              + "        coveringDeg(a_data.refpt,  %1$d ) as h3 "
              + "        from "
              + "        ( "
              + "          select "
              + "             array_agg(cval) as cvals, "
              + "             (case "
              + "               when q2.lon < -180 then 0 "
              + "               when q2.lon > 180  then power(2, ( %3$d )) * %4$d "
              + "               else floor((q2.lon + 180.0) / 360.0 * power(2, ( %3$d )) * %4$d) "
              + "              end "
              + "             )::bigint as px, "
              + "             (case "
              + "               when q2.lat <= -85 then power(2, ( %3$d )) * %4$d "
              + "               when q2.lat >= 85  then 0 "
              + "               else floor((1.0 - ln(tan(radians(q2.lat)) + (1.0 / cos(radians(q2.lat)))) / pi()) / 2 * power(2, ( %3$d )) * %4$d) "
              + "              end "
              + "              )::bigint as py, "
              + "              ST_Centroid( ST_ConvexHull( st_collect( refpt ) ) ) as refpt "
              + "          from "
              + "          ( "
              + "            select cval, st_x(in_data.refpt) as lon, st_y(in_data.refpt) as lat, refpt "
              + "            from "
              + "            ( ",
      h3sqlMid_1a = // standard flavour
      "              select %2$s as cval, coalesce( l.geoh3, v.geo ) as refpt"
              + "              from ${schema}.${table} v "
              + "               left join lateral "
              + "                ( select st_force3d(st_setsrid( h3ToGeoDeg( coveringDeg( case ST_Within(geo, %5$s ) "
              + "                                                                          when true then ST_MakeValid(geo) "
              + "                                                                          else ST_Intersection( ST_MakeValid(geo), %5$s ) "
              + "                                                                         end, %1$d)), st_srid(geo))) "
              + "                  where st_geometrytype(v.geo) != 'ST_Point'"
              + "                ) l(geoh3) "
              + "                on ( true ) ",
      h3sqlMid_1b = // convert to points flavour
      "              select %2$s as cval, st_geometryn(st_points( ST_Intersection( v.geo, %5$s ) ) ,1) as refpt"
              + "              from ${schema}.${table} v ",
      h3sqlMid_2 = "              where 1 = 1 and st_intersects( geo , %5$s ) and %6$s",
      h3sqlEnd =
          "            ) in_data "
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
              + "     %1$s "
              + "  ) outer_v ";

  public static String h3sqlMid(boolean pointmode) {
    return H3SQL.h3sqlMid_0 + (!pointmode ? H3SQL.h3sqlMid_1a : H3SQL.h3sqlMid_1b) + H3SQL.h3sqlMid_2;
  }

  public static int[] MaxResForZoom = {2, 2, 2, 2, 3, 4, 4, 5, 6, 6, 7, 8, 9, 9, 10, 11, 11, 12, 13, 13, 13, 13, 13};

  public static int zoom2resolution(int zoom) {
    return (MaxResForZoom[zoom]);
  }

  public static int bbox2zoom(BBox bbox) {
    return ((int) Math.round((5.88610403145016 - Math.log(bbox.maxLon() - bbox.minLon())) / 0.693147180559945));
  }

  public static int adjPixelSize(int h3res, int maxResForLevel) {
    int pxSize = 64, // 64 in general, but clashes when h3 res is to fine grained (e.g. resulting in
        // holes in hex areas).
        pdiff = (h3res - maxResForLevel);

    if (pdiff == 2) pxSize = 160;
    else if (pdiff >= 3) pxSize = 256;

    return (pxSize);
  }
}
