/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

import com.here.xyz.models.geojson.coordinates.BBox;

public class H3SQL
{
//Clustering constants
  public static final String HEXBIN = "hexbin";
  public static final String HEXBIN_RESOLUTION = "resolution";
  public static final String HEXBIN_PROPERTY = "property";
  public static final String HEXBIN_POINTMODE = "pointmode";

    public static String h3sqlBegin =
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
        + "   ) as jsondata, "
        + "   st_asgeojson( %3$s, 7 )::json as geojson "
        + "  from "
        + "  ( "
        + "   with h3cluster as "
        + "   ( select oo.h3, "
        + "           ( select row_to_json( t1 ) from ( select qty %4$s ) t1 ) as agg, "
        + "           st_containsproperly( ",
//+"                  st_envelope( st_buffer( ST_MakeEnvelope( 45, 21.943045533438177, 67.49999999999997, 40.97989806962013, 4326 )::geography, ( 2.5 * edgeLengthM( 2 )) )::geometry )"
h3sqlMid1 = " "
        + "              , oo.geo ) as omni, "
        + "           oo.geo "
        + "     from "
        + "     ( "
////------- 
        + "      with rtile as  "
        + "      ( select * from  "
        + "        ( "
        + "         select  "
        + "          ST_SetBandNoDataValue( ST_AddBand( ST_MakeEmptyRaster( pxsz, pxsz, st_xmin(tile), st_ymax(tile), ( (st_xmax(tile) - st_xmin(tile))/pxsz::float )::float , ( (st_ymin(tile) - st_ymax(tile))/pxsz::float )::float, 0,0, 4326 ),'1BB' ),0) as raster, "
        + "          level, "
        + "          tile "
        + "         from "
        + "         (  "
        + "           select ",
h3sqlMid2 = " " //+ "st_envelope(st_buffer(ST_MakeEnvelope('-116.19140625', '32.8426736319542982', '-116.015625', '32.9902355596510688', 4326)::geography, (2.5 * edgeLengthM('8')))::geometry) as tile, "
        + "                   %2$d::integer as level, "
        + "                   %3$d::integer as pxsz "
        + "         ) r1 "
        + "        ) r2 "
        + "      ), pxdata as "
        + "      ( "
        + "       select id, array_agg(x1.cval) as cvals, (g).geom as geo, (g).x, (g).y "
        + "       from "
        + "       ( "  
        + "        select case st_geometrytype(geo) = 'ST_Point' when true then -1 else v.i end as id, %1$s as cval, ST_PixelAsPoints( st_setvalue( r.raster, v.geo, 1.0 ),1 ) as g from ${schema}.${table} v, rtile r "
        + "        where 1 = 1 "
        + "          and st_intersects( v.geo, r.tile ) ",
h3sqlEnd = " "
        + "       ) x1 "
        + "       group by id, geo, x, y "
        + "      ) "
        + "      select to_hex(cc.h3) as h3,"
        + "              count(1) as qty,"
        + "              min(cc.unnest) as min,"
        + "              max(cc.unnest) as max,"
        + "              sum(cc.unnest) as sum,"
        + "              round( avg(cc.unnest ),5) as avg,  "
        + "              percentile_cont(0.5) within group (order by cc.unnest) as median,"
        + "              h3togeoboundarydeg(cc.h3)::geometry(Polygon, 4326) AS geo"
        + "      from "
        + "      ( "
        + "       select c.h3, unnest(c.cvals )  "
        + "       from "
        + "       ( "
        + "         select id,cvals, geoToH3Deg(geo, %1$d) as h3 from pxdata where id = -1"
        + "        union all "
        + "         select distinct on (id, h3) id,cvals, geoToH3Deg(geo, %1$d) as h3 from pxdata where id != -1"
        + "       ) c"
        + "      ) cc"
        + "       group by h3   "
        + "     ) oo "
        + "     where 1 = 1 "
        + "   ) "
        + "   select * from h3cluster "
        + "   where 1 = 1 "
        + "     and omni = true "
        + "     %2$s "
        + "  ) outer_v ";

  public static int[] MaxResForZoom = {2, 2, 2, 2, 3, 4, 4, 5, 6, 6, 7, 8, 9, 9, 10, 11, 11, 12, 13, 14, 14, 15, 15};

  public static int zoom2resolution(int zoom) {
    return (MaxResForZoom[zoom]);
  }

  public static int bbox2zoom(BBox bbox) {
    return ((int) Math.round((5.88610403145016 - Math.log(bbox.maxLon() - bbox.minLon())) / 0.693147180559945));
  }

  public static int pxSize = 64;

}
