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

public class TweaksSQL
{
  public static final String SAMPLING = "sampling";
  public static final String SAMPLING_STRENGTH = "strength";
  public static final String SAMPLING_ALGORITHM = "algorithm";
  public static final String SAMPLING_ALGORITHM_DST = "distribution";
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
  public static String strengthSql(int strength, boolean bRandom)
  { 
   if( !bRandom ) 
   {
    double bxLen = ( strength <=  5  ? 0.001  : 
                     strength <= 10  ? 0.002  : 
                     strength <= 30  ? 0.004  : 
                     strength <= 50  ? 0.008  :
                     strength <= 75  ? 0.01   : 0.05 );
    return String.format("( ST_Perimeter(box2d(geo) ) > %f )", bxLen );              
   }
    
   String s = ( strength <=  1  ? "5"   : 
                strength <=  5  ? "4"   : 
                strength <= 10  ? "2"   : 
                strength <= 30  ? "08"  :
                strength <= 50  ? "02"  :
                strength <= 75  ? "004" : "001" );
     
   return String.format("%s < '%s'",DstFunctIndexExpr,s);
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
   +" select jsonb_set('{\"type\": \"Feature\"}'::jsonb,'{properties}', jsonb_set( jsonb_set( '{}'::jsonb, '{gid}', to_jsonb(gid) ),'{gidObjs}', to_jsonb(w) )  ) as jsondata, %1$s as geo"
   +" from "
   +" ( "
   +"  select left( md5(gh), 12 ) as gid, case length(gh) > %2$d when true then 0 else i end as gsz, count(1) as w, (st_dump( st_union(oo.geo) )).geom as geo "
   +"  from "
   +"  ( "
   +"   select ST_GeoHash(geo) as gh, i, jsondata , geo "
   +"   from "
   +"   ( "
   +"    select i, jsondata, geo "  // fetch objects
   +"    from ${schema}.${table} "
   +"    where 1 = 1 "
   +"      and %3$s ";  // bboxquery
 
  private static String _mergeEndSql = 
    "   ) o "
   +"  ) oo "
   +"  group by gh, gsz"
   +" ) ooo "
   +") oooo "
   +"where 1 = 1 "
   +"and geo is not null ";
   

  public static String mergeEndSql(boolean bGeojson)
  {  return _mergeEndSql + "and " + ( bGeojson ? "geo->>'type' != 'GeometryCollection' " : "geometrytype(geo) != 'GEOMETRYCOLLECTION'" );  }
  
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
   +"    else ( select jsonb_set( jsonb_set('{\"type\":\"Feature\",\"properties\":{}}'::jsonb,'{id}', to_jsonb( max(jsondata->>'id') )),'{properties,ids}', jsonb_agg( jsondata->>'id' )) from ${schema}.${table} where i in ( select unnest( ids ) ) ) "
   +"   end as jsondata, "
   +"   case when step = 0 "
   +"    then ( select geo from ${schema}.${table} where i = ids[1] ) "
   +"    else ( select ST_LineMerge( st_collect( geo ) ) from ${schema}.${table} where i in ( select unnest( ids )) ) "
   +"   end as geo "
   +"  from iddata "
   +") "
   +"select jsondata, %1$s as geo from finaldata ";
  




  public static String 
   requestedTileBoundsSql = String.format("ST_MakeEnvelope(%%.%1$df,%%.%1$df,%%.%1$df,%%.%1$df, 4326)", 14 /*GEOMETRY_DECIMAL_DIGITS*/),
   estimateCountByBboxesSql =
    " with indata as ( select '${schema}' as schema, '${table}' as space, unnest( array[ %1$s ] ) as tile, 'geo' as colname, exists ( select 1 from pg_stats where schemaname = replace('${schema}','\"','') and tablename = replace('${table}','\"','') and attname = 'geo' ) as bstats  ) "
   +" select jsonb_set( '{\"type\":\"Feature\"}', '{rcount}', to_jsonb( max( case when r.bstats then ( reltuples * _postgis_selectivity(format('%%s.%%s', r.schema, r.space)::regclass, r.colname, r.tile) ) else 0.0 end )::integer ) ) as rcount, null "
   +" from pg_class l, indata r "
   +" where oid = format('%%s.%%s',r.schema,r.space )::regclass ";
  
  public static String 
   mvtPropertiesSql        = "( select jsonb_object_agg(key, case when jsonb_typeof(value) in ('object', 'array') then to_jsonb(value::text) else value end) from jsonb_each(jsonb_set((jsondata)->'properties','{id}', to_jsonb(jsondata->>'id'))))", 
   mvtPropertiesFlattenSql = "( select jsonb_object_agg('properties.' || jkey,jval) from prj_flatten( jsonb_set((jsondata)->'properties','{id}', to_jsonb( jsondata->>'id' )) ))",
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



