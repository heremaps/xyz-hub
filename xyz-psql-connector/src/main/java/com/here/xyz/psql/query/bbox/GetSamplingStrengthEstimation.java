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

package com.here.xyz.psql.query.bbox;

import static com.here.xyz.psql.query.GetFeaturesByBBox.getTileFromBbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.models.geojson.WebMercatorTile;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.query.XyzEventBasedQueryRunner;
import com.here.xyz.psql.query.bbox.GetSamplingStrengthEstimation.SamplingStrengthEstimation;
import com.here.xyz.psql.tools.DhString;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class GetSamplingStrengthEstimation<E extends GetFeaturesByBBoxEvent> extends XyzEventBasedQueryRunner<E, SamplingStrengthEstimation> {

  private static String
   requestedTileBoundsSql = DhString.format("ST_MakeEnvelope(%%.%1$df,%%.%1$df,%%.%1$df,%%.%1$df, 4326)", 14 /*GEOMETRY_DECIMAL_DIGITS*/);
  private static TupleTime tTime = new TupleTime();
  private static String
   estWithPgClass_B =
     "   select sum( coalesce( c2.reltuples, c1.reltuples ) )::bigint as reltuples, "
    +"   string_agg(  coalesce( c2.reltuples, c1.reltuples ) || '~' || coalesce(c2.relname, c1.relname),',' ) as rtup "
    +"   from indata i, pg_class c1 left join pg_inherits pm on ( c1.oid = pm.inhparent ) left join pg_class c2 on ( c2.oid = pm.inhrelid ) "
    +"   where c1.oid = format('%s.%s',i.schema,i.space)::regclass";
  private static String estWithoutPgClass_B =
     "   select sum( c0.reltuples )::bigint as reltuples, "
    +"   '%1$s'::text as rtup "
    +"   from indata i, ( select split_part(r1,'~',2)::name as tblname, split_part(r1,'~',1)::real as reltuples from ( select regexp_split_to_table( '%1$s',',' ) as r1 ) r2 ) c0";
  private static String estimateCountByBboxesSql_B =  //flavour2: calc _postgis_selectivity using sum of reltupels
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
    +" select jsonb_set(jsonb_set( '{\"type\":\"Feature\"}', '{rcount}', to_jsonb( max(estim)::integer ) ),'{rtuples}', to_jsonb(max(rtup)) ) as rcount, null from iiidata ";
  private static String estWithPgClass_A =
     "   select i.schema, i.space, i.colname, "
    +"          t.tile, t.tid, "
    +"          true as bstats, "
    +"          coalesce(c2.relname, c1.relname) as tblname, "
    +"          coalesce( c2.reltuples, c1.reltuples ) reltuples "
    +"   from indata i, unnest( i.tiles) with ordinality t(tile,tid), pg_class c1 left join pg_inherits pm on ( c1.oid = pm.inhparent ) left join pg_class c2 on ( c2.oid = pm.inhrelid ) "
    +"   where c1.oid = format('%s.%s',i.schema,i.space)::regclass";
  private static String estWithPgClass = estWithPgClass_A;
  private static String estWithoutPgClass_A =
     "   select i.schema, i.space, i.colname, "
    +"          t.tile, t.tid, "
    +"          true as bstats, "
    +"          c0.tblname, "
    +"          c0.reltuples "
    +"   from indata i, unnest( i.tiles) with ordinality t(tile,tid), ( select split_part(r1,'~',2)::name as tblname, split_part(r1,'~',1)::real as reltuples from ( select regexp_split_to_table( '%1$s',',' ) as r1 ) r2 ) c0";
  private static String estWithoutPgClass = estWithoutPgClass_A;
  private static String estimateCountByBboxesSql_A = //flavour1: calc _postgis_selectivity with partitions and sum up
     " with indata as "
    +" ( select '${schema}' as schema, '${table_head}' as space, array[ %1$s ] as tiles, 'geo' as colname ), "
    +" iindata as ( %2$s ),"
    +" iiidata as "
    +" ( select ii.tid, string_agg(  ii.reltuples::bigint || '~' || ii.tblname,',' ) as rtup, sum( case when ii.bstats then ii.reltuples * xyz_postgis_selectivity(format('%%s.%%I', ii.schema, ii.tblname)::regclass, ii.colname, ii.tile) else 0.0 end ) estim "
    +"   from iindata ii "
    +"   group by tid "
    +" ) "
    +" select jsonb_set( jsonb_set( '{\"type\":\"Feature\"}', '{rcount}', to_jsonb( max(estim)::integer)), '{rtuples}', to_jsonb(max(rtup))) as rcount, null from iiidata ";
  private static String estimateCountByBboxesSql = estimateCountByBboxesSql_A;
  private String rTuples;
  private String spaceId;

  private static class TupleTime {
    private static Map<String, String> rTuplesMap = new ConcurrentHashMap<>();
    private long expireTs = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(15);
    private boolean expired() {
      return System.currentTimeMillis() > expireTs;
    }
  }

  public GetSamplingStrengthEstimation(E event)
      throws SQLException, ErrorResponseException {
    super(event);
  }
  @Override
  protected SQLQuery buildQuery(E event) throws SQLException, ErrorResponseException {
    if (tTime.expired())
      tTime = new TupleTime();
    rTuples = TupleTime.rTuplesMap.get(event.getSpace()); //TODO: Check semantics of that tuples cache
    spaceId = event.getSpace();

    return buildEstimateSamplingStrengthQuery(event, event.getBbox(), rTuples);
  }

  @Override
  public SamplingStrengthEstimation handle(ResultSet rs) throws SQLException {
    FeatureCollection collection = dbHandler.legacyDefaultFeatureResultSetHandler(rs);
    Feature estimateFtr = null;
    try {
      estimateFtr = collection.getFeatures().get(0);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    if (rTuples == null) {
      rTuples = estimateFtr.get("rtuples");
      TupleTime.rTuplesMap.put(spaceId, rTuples);
    }

    SamplingStrengthEstimation result = new SamplingStrengthEstimation();
    result.rCount = estimateFtr.get("rcount");
    result.rTuples = rTuples;

    return result;
  }

  public static class SamplingStrengthEstimation {
    public String rTuples;
    public int rCount;
  }

  public static SQLQuery buildEstimateSamplingStrengthQuery(GetFeaturesByBBoxEvent event, BBox bbox, String relTuples)
  {
    int level, tileX, tileY, margin = 0;

    if( event instanceof GetFeaturesByTileEvent)
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
        if( (dy == 0) && (dx == 0) ) listOfBBoxes.add(bbox);  // centerbox, this is already extended by margin
        else if( ((tileY + dy) > 0) && ((tileY + dy) < nrTilesXY) )
          listOfBBoxes.add( WebMercatorTile.forWeb(level, ((nrTilesXY +(tileX + dx)) % nrTilesXY) , (tileY + dy)).getExtendedBBox(margin) );

    int flag = 0;
    StringBuilder sb = new StringBuilder();
    for (BBox b : listOfBBoxes)
      sb.append(DhString.format("%s%s",( flag++ > 0 ? "," : ""),DhString.format( requestedTileBoundsSql , b.minLon(), b.minLat(), b.maxLon(), b.maxLat() )));

    String estimateSubSql = ( relTuples == null ? estWithPgClass : DhString.format( estWithoutPgClass, relTuples ) );

    return new SQLQuery( DhString.format( estimateCountByBboxesSql, sb.toString(), estimateSubSql ) );
  }
}
