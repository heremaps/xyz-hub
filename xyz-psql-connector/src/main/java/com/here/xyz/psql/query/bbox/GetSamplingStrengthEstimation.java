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
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.factory.TweaksSQL;
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

  private static TupleTime tTime = new TupleTime();
  private String rTuples;
  private String spaceId;

  private static class TupleTime {
    private static Map<String, String> rTuplesMap = new ConcurrentHashMap<>();
    private long expireTs = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(15);
    private boolean expired() {
      return System.currentTimeMillis() > expireTs;
    }
  }

  public GetSamplingStrengthEstimation(E event, DatabaseHandler dbHandler)
      throws SQLException, ErrorResponseException {
    super(event, dbHandler);
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
    FeatureCollection collection = dbHandler.defaultFeatureResultSetHandler(rs);
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
      sb.append(DhString.format("%s%s",( flag++ > 0 ? "," : ""),DhString.format( TweaksSQL.requestedTileBoundsSql , b.minLon(), b.minLat(), b.maxLon(), b.maxLat() )));

    String estimateSubSql = ( relTuples == null ? TweaksSQL.estWithPgClass : DhString.format( TweaksSQL.estWithoutPgClass, relTuples ) );

    return new SQLQuery( DhString.format( TweaksSQL.estimateCountByBboxesSql, sb.toString(), estimateSubSql ) );
  }
}
