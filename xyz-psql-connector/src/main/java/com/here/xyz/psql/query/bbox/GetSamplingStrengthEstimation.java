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
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SCHEMA;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.models.geojson.WebMercatorTile;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.psql.query.XyzEventBasedQueryRunner;
import com.here.xyz.psql.query.bbox.GetSamplingStrengthEstimation.SamplingStrengthEstimation;
import com.here.xyz.psql.tools.DhString;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper;
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
  private static String estWithPgClass = "   select i.schema, i.space, i.colname, "
     + "          t.tile, t.tid, "
     + "          true as bstats, "
     + "          coalesce(c2.relname, c1.relname) as tblname, "
     + "          coalesce( c2.reltuples, c1.reltuples ) reltuples "
     + "   from indata i, unnest( i.tiles) with ordinality t(tile,tid), pg_class c1 left join pg_inherits pm on ( c1.oid = pm.inhparent ) left join pg_class c2 on ( c2.oid = pm.inhrelid ) "
     + "   where c1.oid = format('%s.%s',i.schema,i.space)::regclass";
  private static String estWithoutPgClass = "   select i.schema, i.space, i.colname, "
     + "          t.tile, t.tid, "
     + "          true as bstats, "
     + "          c0.tblname, "
     + "          c0.reltuples "
     + "   from indata i, unnest( i.tiles) with ordinality t(tile,tid), ( select split_part(r1,'~',2)::name as tblname, split_part(r1,'~',1)::real as reltuples from ( select regexp_split_to_table( '%1$s',',' ) as r1 ) r2 ) c0";
  private static String estimateCountByBboxesSql = " with indata as "
     + " ( select '${schema}' as schema, '${headTable}' as space, array[ %1$s ] as tiles, 'geo' as colname ), "
     + " iindata as ( %2$s ),"
     + " iiidata as "
     + " ( select ii.tid, string_agg(  ii.reltuples::bigint || '~' || ii.tblname,',' ) as rtup, sum( case when ii.bstats then ii.reltuples * xyz_postgis_selectivity(format('%%s.%%I', ii.schema, ii.tblname)::regclass, ii.colname, ii.tile) else 0.0 end ) estim "
     + "   from iindata ii "
     + "   group by tid "
     + " ) "
     + " SELECT max(estim)::INTEGER AS rcount, max(rtup) AS rtuples FROM iiidata ";
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
    if (rs.next()) {
      SamplingStrengthEstimation result = new SamplingStrengthEstimation();
      result.rCount = rs.getInt("rcount");
      if (rTuples == null) {
        rTuples = rs.getString("rtuples");
        TupleTime.rTuplesMap.put(spaceId, rTuples);
      }
      result.rTuples = rTuples;
      return result;
    }
    throw new RuntimeException("No values were returned for SamplingStrengthEstimation");
  }

  public static class SamplingStrengthEstimation {
    public String rTuples;
    public int rCount;
  }

  public SQLQuery buildEstimateSamplingStrengthQuery(GetFeaturesByBBoxEvent event, BBox bbox, String relTuples) {
    int level, tileX, tileY, margin = 0;

    if (event instanceof GetFeaturesByTileEvent tileEvent) {
      level = tileEvent.getLevel();
      tileX = tileEvent.getX();
      tileY = tileEvent.getY();
      margin = tileEvent.getMargin();
    }
    else {
      final WebMercatorTile tile = getTileFromBbox(bbox);
      level = tile.level;
      tileX = tile.x;
      tileY = tile.y;
    }

    ArrayList<BBox> listOfBBoxes = new ArrayList<>();
    int nrTilesXY = 1 << level;

    for (int dy = -1; dy < 2; dy++)
      for (int dx = -1; dx < 2; dx++)
        if (dy == 0 && dx == 0)
          listOfBBoxes.add(bbox); //centerbox, this is already extended by margin
        else if (tileY + dy > 0 && tileY + dy < nrTilesXY)
          listOfBBoxes.add(WebMercatorTile.forWeb(level, (nrTilesXY + tileX + dx) % nrTilesXY, tileY + dy).getExtendedBBox(margin));

    int flag = 0;
    StringBuilder sb = new StringBuilder();
    for (BBox b : listOfBBoxes)
      sb.append((flag++ > 0 ? "," : "") + DhString.format(requestedTileBoundsSql, b.minLon(), b.minLat(), b.maxLon(), b.maxLat()));

    String estimateSubSql = relTuples == null ? estWithPgClass : DhString.format(estWithoutPgClass, relTuples);

    return new SQLQuery(DhString.format(estimateCountByBboxesSql, sb.toString(), estimateSubSql))
        .withVariable(SCHEMA, getSchema())
        .withVariable("headTable", getDefaultTable((E) event));
  }
}
