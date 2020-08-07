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

import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.psql.PSQLXyzConnector;

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
  public static final String ENSURE = "ensure";
  public static final String ENSURE_OPTIONS = "options";
  public static final String ENSURE_OPTIONS_ALLPROP = "allprop";
  
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

  public static int calculateDistributionStrength(int rCount) 
  { int CHNKSZ = 10000;
    if( rCount <= (   3 * CHNKSZ ) ) return  1;
    if( rCount <= (   4 * CHNKSZ ) ) return  5;
    if( rCount <= (   8 * CHNKSZ ) ) return 10;
    if( rCount <= (  32 * CHNKSZ ) ) return 30;
    if( rCount <= ( 128 * CHNKSZ ) ) return 50;
    if( rCount <= (1024 * CHNKSZ ) ) return 75;
    return 100;
  }
 

  public static String mergeBeginSql = 
    "select jsondata, geo "
   +"from "
   +"( "
   +" select jsonb_set('{\"type\": \"Feature\"}'::jsonb,'{properties}', jsonb_set( jsonb_set( '{}'::jsonb, '{gid}', to_jsonb(gid) ),'{gidObjs}', to_jsonb(w) )  ) as jsondata, (%1$s)::jsonb as geo"
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
 
  public static String mergeEndSql = 
    "   ) o "
   +"  ) oo "
   +"  group by gh, gsz"
   +" ) ooo "
   +") oooo "
   +"where 1 = 1 "
   +"and geo is not null "
   +"and geo->>'type' != 'GeometryCollection' ";
  
  public static String 
   estimateBuildBboxSql = String.format("ST_MakeEnvelope(%%.%1$df,%%.%1$df,%%.%1$df,%%.%1$df, 4326)", 14 /*GEOMETRY_DECIMAL_DIGITS*/),
   estimateCountByBboxesSql =
    " with indata as ( select '${schema}' as schema, '${table}' as space, unnest( array[ %1$s ] ) as tile, 'geo' as colname  ) "
   +" select jsonb_set( '{\"type\":\"Feature\"}', '{rcount}', to_jsonb( max( reltuples * _postgis_selectivity( format('%%s.%%s',r.schema,r.space )::regclass, r.colname, r.tile) )::integer ) ) as rcount, null "
   +" from pg_class l, indata r "
   +" where oid = format('%%s.%%s',r.schema,r.space )::regclass ";
  
}



