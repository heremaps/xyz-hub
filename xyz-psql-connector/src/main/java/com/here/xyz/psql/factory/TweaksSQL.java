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

package com.here.xyz.psql.factory;

import com.here.xyz.psql.tools.DhString;

public class TweaksSQL
{
  public static final String SAMPLING = "sampling";
  public static final String SAMPLING_STRENGTH = "strength";
  public static final String SAMPLING_ALGORITHM = "algorithm";
  public static final String SAMPLING_ALGORITHM_DST = "distribution";
  public static final String SAMPLING_ALGORITHM_DST2 = "distribution2";
  public static final String SAMPLING_ALGORITHM_SZE = "geometrysize";
  public static final String SIMPLIFICATION = "simplification";
  public static final String SIMPLIFICATION_STRENGTH = SAMPLING_STRENGTH;
  public static final String SIMPLIFICATION_ALGORITHM = "algorithm";
  public static final String SIMPLIFICATION_ALGORITHM_A01 = "grid";
  public static final String SIMPLIFICATION_ALGORITHM_A05 = "gridbytilelevel";
  public static final String SIMPLIFICATION_ALGORITHM_A02 = "simplifiedkeeptopology";
  public static final String SIMPLIFICATION_ALGORITHM_A03 = "simplified";
  public static final String SIMPLIFICATION_ALGORITHM_A04 = "merge";
  public static final String SIMPLIFICATION_ALGORITHM_A06 = "linemerge";
  public static final String ENSURE = "ensure";
  public static final String ENSURE_DEFAULT_SELECTION = "defaultselection";
  public static final String ENSURE_SAMPLINGTHRESHOLD = "samplingthreshold";

  /*
   [  1     |   (1) | 1/3    | ~ md5( '' || i) < '5'   ]
   [  5     |   (5) | 1/4    | ~ md5( '' || i) < '4'   ]
   [ low    |  (10) | 1/8    | ~ md5( '' || i) < '2'   ]
   [ lowmed	|  (30) | 1/32   | ~ md5( '' || i) < '08'  ]
   [ med    |  (50) | 1/128  | ~ md5( '' || i) < '02'  ]
   [ medhigh|  (75) | 1/1024 | ~ md5( '' || i) < '004' ]
   [ high   | (100) | 1/4096 | ~ md5( '' || i) < '001' ]
  */

  private static final String DstFunctIndexExpr = "left(md5(''||i),5)";

  public static String distributionFunctionIndexExpression() { return DstFunctIndexExpr; }

  public static String strengthSql(int strength, boolean bRandom)
  {
   if( !bRandom )
   {
    double bxLen = ( strength <=  5  ? 0.001  :
                     strength <= 10  ? 0.002  :
                     strength <= 30  ? 0.004  :
                     strength <= 50  ? 0.008  :
                     strength <= 75  ? 0.01   : 0.05 );
    return DhString.format("( ST_Perimeter(box2d(geo) ) > %f )", bxLen );
   }

   String s = ( strength <=  1  ? "5"   :
                strength <=  5  ? "4"   :
                strength <= 10  ? "2"   :
                strength <= 30  ? "08"  :
                strength <= 50  ? "02"  :
                strength <= 75  ? "004" : "001" );

   return DhString.format("%s < '%s'",DstFunctIndexExpr,s);
  }

  public static float tableSampleRatio(int strength)
  {
   float r = (  strength <=  1  ? (1 / 3f)    :
                strength <=  5  ? (1 / 4f)    :
                strength <= 10  ? (1 / 8f)    :
                strength <= 30  ? (1 / 32f)   :
                strength <= 50  ? (1 / 128f)  :
                strength <= 75  ? (1 / 1024f) : (1 / 4096f) );
   return r;
  }


  public static int calculateDistributionStrength(int rCount, int chunkSize)
  {
    if( rCount <= (       chunkSize ) ) return  0;
    if( rCount <= (   3 * chunkSize ) ) return  1;
    if( rCount <= (   4 * chunkSize ) ) return  5;
    if( rCount <= (   8 * chunkSize ) ) return 10;
    if( rCount <= (  32 * chunkSize ) ) return 30;
    if( rCount <= ( 128 * chunkSize ) ) return 50;
    if( rCount <= (1024 * chunkSize ) ) return 75;
    return 100;
  }

  public static String mergeBeginSql =
    "select jsondata, geo "
   +"from "
   +"( "
   +" select jsonb_set('{\"type\": \"Feature\"}'::jsonb,'{properties}', jsonb_set( case when (ginfo->>1)::integer = 1 then jsonb_set( '{}'::jsonb,'{ids}', ids ) else '{}'::jsonb end , '{groupInfo}', ginfo )) as jsondata, %1$s as geo"
   +" from "
   +" ( "
   +"  select jsonb_build_array(left(md5( gh || gsz ), 12), row_number() over w, count(1) over w, nrobj ) as ginfo, * "
   +"  from "
   +"  ( "
   +"   select gh, case length(gh) > %2$d when true then 0 else i end as gsz, count(1) as nrobj, jsonb_agg(id) as ids, (st_dump( st_union(oo.geo) )).geom as geo "
   +"   from "
   +"   ( "
   +"    select ST_GeoHash(geo) as gh, i, id , geo "
   +"    from "
   +"    ( "
   +"     select i, id, geo "  // fetch objects
   +"     from ${schema}.${table} "
   +"     where 1 = 1 "
   +"       and %3$s ";  // bboxquery

  private static String _mergeEndSql =
    "    ) o "
   +"   ) oo "
   +"   group by gsz, gh"
   +"  ) ooo window w as (partition by gh, gsz ) "
   +" ) oooo "
   +") ooooo "
   +"where 1 = 1 "
   +"and geo is not null ";


  public static String mergeEndSql(boolean bGeojson)
  {  return _mergeEndSql + "and " + ( bGeojson ? "geo->>'type' != 'GeometryCollection' and jsonb_array_length(geo->'coordinates') > 0 "
                                               : "geometrytype(geo) != 'GEOMETRYCOLLECTION' and not st_isempty(geo) " );  }

  public static String linemergeBeginSql =
    "with "
   +"indata as "
   +"( select i, %1$s as geo from ${schema}.${table} "
   +"  where 1 = 1 "
   +"    and %2$s ";  // bboxquery

  public static String linemergeEndSql1 =
    "), "
   +"cx2ids as "
   +"( select left( gid, %1$d ) as region, ids "
   +"  from "
   +"  ( select gid, array_agg( i ) as ids "
   +"    from "
   +"    ( select i, unnest( array[ ST_GeoHash( st_startpoint(geo),9 ) , ST_GeoHash( st_endpoint(geo), 9 ) ] ) as gid from indata where ( geometrytype(geo) = 'LINESTRING' ) ) o "
   +"    group by gid "
   +"  ) o	"
   +"  where 1 = 1 "
   +"    and cardinality(ids) = 2 "
   +"), "
   +"cxlist as "
   +"( select count(1) over ( PARTITION BY region ) as rcount, array[(row_number() over ( PARTITION BY region ))::integer] as rids, region, ids from cx2ids ), "
   +"mergedids as "
   +"( with recursive mrgdids( step, region, rcount, rids, ids ) as "
   +"  ( "
   +"	  select 1, region, rcount, rids, ids from cxlist "
   +"	 union all "
   +"		select distinct on (region, rids[1] ) * "
   +"		from "
   +"		( select l.step+1 as step, l.region, l.rcount, array( select unnest( l.rids || r.rids ) order by 1 )  as rids, l.ids || r.ids as ids "
   +"		  from mrgdids l join cxlist r on ( l.region = r.region and not (l.rids @> r.rids) and  (l.ids && r.ids ) ) "
   +"		  where 1 = 1 "
   +"		) i1 "
   +"	) "
   +"  select l.region, l.rcount, l.step, l.rids, array( select distinct unnest( l.ids ) ) as ids "
   +"  from mrgdids l left join mrgdids r on ( l.region = r.region and l.step < r.step and l.rids <@ r.rids ) "
   +"  where 1 = 1 "
   +"    and r.region is null "
   +"), "
   +"ccxuniqid as "
   +"( select distinct unnest(ids) as id from cx2ids ), "
   +"iddata as "
   +"(  select step, ids from mergedids "
   +"  union "
   +"   select 0 as step, array[i] as ids from indata where not i in (select id from ccxuniqid ) "
   +"), "
   +"finaldata as "
   +"(	select "
   +"   case when step = 0 "
   +"    then ( select "; /* prj_jsondata */
  public static String linemergeEndSql2 =
                         " from ${schema}.${table} where i = ids[1] ) "
   +"    else ( select jsonb_set( jsonb_set('{\"type\":\"Feature\",\"properties\":{}}'::jsonb,'{id}', to_jsonb(max(id))),'{properties,ids}', jsonb_agg(id)) from ${schema}.${table} where i in ( select unnest( ids ) ) ) "
   +"   end as jsondata, "
   +"   case when step = 0 "
   +"    then ( select geo from ${schema}.${table} where i = ids[1] ) "
   +"    else ( select ST_LineMerge( st_collect( geo ) ) from ${schema}.${table} where i in ( select unnest( ids )) ) "
   +"   end as geo "
   +"  from iddata "
   +") "
   +"select jsondata, %1$s as geo from finaldata ";


  private static String
   estWithPgClass_B =
     "   select sum( coalesce( c2.reltuples, c1.reltuples ) )::bigint as reltuples, "
    +"   string_agg(  coalesce( c2.reltuples, c1.reltuples ) || '~' || coalesce(c2.relname, c1.relname),',' ) as rtup "
    +"   from indata i, pg_class c1 left join pg_inherits pm on ( c1.oid = pm.inhparent ) left join pg_class c2 on ( c2.oid = pm.inhrelid ) "
    +"   where c1.oid = format('%s.%s',i.schema,i.space)::regclass",

   estWithoutPgClass_B =
     "   select sum( c0.reltuples )::bigint as reltuples, "
    +"   '%1$s'::text as rtup "
    +"   from indata i, ( select split_part(r1,'~',2)::name as tblname, split_part(r1,'~',1)::real as reltuples from ( select regexp_split_to_table( '%1$s',',' ) as r1 ) r2 ) c0",

   estimateCountByBboxesSql_B =  //flavour2: calc _postgis_selectivity using sum of reltupels
    " with indata as "
    +" ( select '${schema}' as schema, '${table}' as space, array[ %1$s ] as tiles, 'geo' as colname ), "
    +" reldata as ( %2$s ),"
    +" iindata as "
    +" ( select i.schema, i.space, i.colname, t.tile, "
    +"          true as bstats, "
    +"          r.reltuples, r.rtup "
    +"   from indata i, reldata r, unnest( i.tiles ) t(tile) "
    +" ), "
    +" iiidata as "
    +" ( select ii.rtup, case when ii.bstats then ii.reltuples * xyz_postgis_selectivity(format('%%s.%%s', ii.schema, ii.space)::regclass, ii.colname, ii.tile) else 0.0 end estim "
    +"   from iindata ii "
    +" ) "
    +" select jsonb_set(jsonb_set( '{\"type\":\"Feature\"}', '{rcount}', to_jsonb( max(estim)::integer ) ),'{rtuples}', to_jsonb(max(rtup)) ) as rcount, null from iiidata ",

   estWithPgClass_A =
     "   select i.schema, i.space, i.colname, "
    +"          t.tile, t.tid, "
    +"          true as bstats, "
    +"          coalesce(c2.relname, c1.relname) as tblname, "
    +"          coalesce( c2.reltuples, c1.reltuples ) reltuples "
    +"   from indata i, unnest( i.tiles) with ordinality t(tile,tid), pg_class c1 left join pg_inherits pm on ( c1.oid = pm.inhparent ) left join pg_class c2 on ( c2.oid = pm.inhrelid ) "
    +"   where c1.oid = format('%s.%s',i.schema,i.space)::regclass",

   estWithoutPgClass_A =
     "   select i.schema, i.space, i.colname, "
    +"          t.tile, t.tid, "
    +"          true as bstats, "
    +"          c0.tblname, "
    +"          c0.reltuples "
    +"   from indata i, unnest( i.tiles) with ordinality t(tile,tid), ( select split_part(r1,'~',2)::name as tblname, split_part(r1,'~',1)::real as reltuples from ( select regexp_split_to_table( '%1$s',',' ) as r1 ) r2 ) c0",

   estimateCountByBboxesSql_A = //flavour1: calc _postgis_selectivity with partitions and sum up
     " with indata as "
    +" ( select '${schema}' as schema, '${table_head}' as space, array[ %1$s ] as tiles, 'geo' as colname ), "
    +" iindata as ( %2$s ),"
    +" iiidata as "
    +" ( select ii.tid, string_agg(  ii.reltuples::bigint || '~' || ii.tblname,',' ) as rtup, sum( case when ii.bstats then ii.reltuples * xyz_postgis_selectivity(format('%%s.%%I', ii.schema, ii.tblname)::regclass, ii.colname, ii.tile) else 0.0 end ) estim "
    +"   from iindata ii "
    +"   group by tid "
    +" ) "
    +" select jsonb_set( jsonb_set( '{\"type\":\"Feature\"}', '{rcount}', to_jsonb( max(estim)::integer)), '{rtuples}', to_jsonb(max(rtup))) as rcount, null from iiidata ";

  public static String
   requestedTileBoundsSql = DhString.format("ST_MakeEnvelope(%%.%1$df,%%.%1$df,%%.%1$df,%%.%1$df, 4326)", 14 /*GEOMETRY_DECIMAL_DIGITS*/),

   estWithPgClass = estWithPgClass_A,
   estWithoutPgClass = estWithoutPgClass_A,
   estimateCountByBboxesSql = estimateCountByBboxesSql_A;


  public static String
   mvtPropertiesSql        = "( select jsonb_object_agg(key, case when jsonb_typeof(value) in ('object', 'array') then to_jsonb(value::text) else value end) from jsonb_each(jsonb_set((jsondata)->'properties','{id}', to_jsonb(id))))",
   mvtPropertiesFlattenSql = "( select jsonb_object_agg('properties.' || jkey,jval) from prj_flatten( jsonb_set((jsondata)->'properties','{id}', to_jsonb(id)) ))",

   hrtBeginSql =
    "with tile as ( select %1$s as bounds, %3$d::integer as extend, %4$d::integer as buffer, true as clip_geom ), "
   +"mvtdata as "
   +"( "
   +" select %2$s as mproperties, ST_AsMVTGeom(st_force2d( geo ), t.bounds, t.extend, t.buffer, t.clip_geom ) as mgeo "
   +" from "
   +" ( ",

   mvtBeginSql =
    "with tile as ( select st_transform(%1$s,3857) as bounds, %3$d::integer as extend, %4$d::integer as buffer, true as clip_geom ), "
   +"mvtdata as "
   +"( "
   +" select %2$s as mproperties, ST_AsMVTGeom(st_force2d( st_transform(geo,3857) ), t.bounds, t.extend, t.buffer, t.clip_geom ) as mgeo "
   +" from "
   +" ( ",
    /** inner sql comes here, like "select jsondata, geo from table " , it is expected that the attributs are named "jsondata" and "geo" */
   mvtEndSql =
    " ) data , tile t "
   +") "
   +"select ST_AsMVT( mvtdata , '%1$s' ) as bin from mvtdata where mgeo is not null";


}



