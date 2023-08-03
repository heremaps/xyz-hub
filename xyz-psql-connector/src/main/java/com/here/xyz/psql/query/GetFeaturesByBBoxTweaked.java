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

package com.here.xyz.psql.query;

import static com.here.xyz.responses.XyzError.ILLEGAL_ARGUMENT;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.factory.TweaksSQL;
import com.here.xyz.psql.query.bbox.GetSamplingStrengthEstimation;
import com.here.xyz.psql.query.bbox.GetSamplingStrengthEstimation.SamplingStrengthEstimation;
import com.here.xyz.responses.XyzResponse;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class GetFeaturesByBBoxTweaked<E extends GetFeaturesByBBoxEvent, R extends XyzResponse> extends GetFeaturesByBBox<E, R> {

  private boolean isMvtRequested;
  private boolean resultIsPartial;

  public GetFeaturesByBBoxTweaked(E event)
      throws SQLException, ErrorResponseException {
    super(event);
    setUseReadReplica(true);
  }

  @Override
  protected SQLQuery buildQuery(E event) throws SQLException, ErrorResponseException {
    isMvtRequested = isMvtRequested(event);
    Map<String, Object> tweakParams = getTweakParams(event);
    SQLQuery query;
    switch (event.getTweakType().toLowerCase()) {
      case TweaksSQL.ENSURE: {
        tweakParams = getTweakParamsForEnsureMode(event, tweakParams);
      }
      case TweaksSQL.SAMPLING: {
        if (!"off".equals(event.getVizSampling().toLowerCase())) {
          query = buildSamplingTweaksQuery(event, tweakParams);

          if (isMvtRequested)
            return buildMvtEncapsuledQuery((GetFeaturesByTileEvent) event, query);
          else if ((int) tweakParams.get("strength") > 0)
            resultIsPartial = true; //Either ensure mode or explicit tweaks:sampling request where strength in [1..100]
          return query;
        }
        else
          //Fall thru tweaks=simplification e.g. mode=viz and vizSampling=off
          tweakParams.put("algorithm", "gridbytilelevel");
      }

      case TweaksSQL.SIMPLIFICATION: {
        query = buildSimplificationTweaksQuery(event, tweakParams);

        if (isMvtRequested)
          return buildMvtEncapsuledQuery((GetFeaturesByTileEvent) event, query);
        return query;
      }
      default:
        throw new ErrorResponseException(ILLEGAL_ARGUMENT, "Invalid tweak type provided. Allowed values are: "
            + TweaksSQL.ENSURE + ", " + TweaksSQL.SAMPLING + ", " + TweaksSQL.SIMPLIFICATION);
    }
  }

  @Override
  public R handle(ResultSet rs) throws SQLException {
    if (isMvtRequested)
      return (R) GetFeaturesByBBox.defaultBinaryResultSetHandler(rs);
    else {
      FeatureCollection collection = dbHandler.defaultFeatureResultSetHandlerSkipIfGeomIsNull(rs);
      if (resultIsPartial)
        collection.setPartial(true);
      return (R) collection;
    }
  }

  static private boolean isVeryLargeSpace( String rTuples )
  { long tresholdVeryLargeSpace = 350000000L; // 350M Objects

    if( rTuples == null ) return false;

    String[] a = rTuples.split("~");
    if( a == null || a[0] == null ) return false;

    try{ return tresholdVeryLargeSpace <= Long.parseLong(a[0]); } catch( NumberFormatException e ){}

    return false;
  }

  public static Map<String, Object> getTweakParams(GetFeaturesByBBoxEvent event) {
    Map<String, Object> tweakParams;

    if (event.getTweakType() != null)
      tweakParams = event.getTweakParams();
    else if (!"viz".equals(event.getOptimizationMode())) {
      //FIXME: This case can never happen due condition which was met before ("viz".equals(event.getOptimizationMode()))
      event.setTweakType(TweaksSQL.SIMPLIFICATION);
      tweakParams = new HashMap<>(Collections.singletonMap("algorithm", "gridbytilelevel"));
    }
    else {
      event.setTweakType(TweaksSQL.ENSURE);
      tweakParams = getTweakParamsForVizMode(event);
    }

    tweakParams.put("strength", 1);
    tweakParams.put("sortByHashedValue", false);
    return tweakParams;
  }

  private static Map<String, Object> getTweakParamsForVizMode(GetFeaturesByBBoxEvent event) {
    Map<String, Object> m = new HashMap<>();

    switch( event.getVizSampling().toLowerCase() ) {
      case "high":
        m.put(TweaksSQL.ENSURE_SAMPLINGTHRESHOLD, 15);
        break;
      case "low":
        m.put(TweaksSQL.ENSURE_SAMPLINGTHRESHOLD, 70);
        break;
      case "off":
        m.put(TweaksSQL.ENSURE_SAMPLINGTHRESHOLD, 100);
        break;
      case "med":
      default:
        m.put(TweaksSQL.ENSURE_SAMPLINGTHRESHOLD, 30);
        break;
    }
    return m;
  }

  public static HashMap<String, Object> getTweakParamsForEnsureMode(GetFeaturesByBBoxEvent event,
      Map<String, Object> tweakParams) throws SQLException, ErrorResponseException {
    SamplingStrengthEstimation estimationResult = new GetSamplingStrengthEstimation<>(event).run();

    HashMap<String, Object> hmap = new HashMap<>();

    hmap.put("sortByHashedValue", isVeryLargeSpace(estimationResult.rTuples));

    boolean bDefaultSelectionHandling = (tweakParams.get(TweaksSQL.ENSURE_DEFAULT_SELECTION) == Boolean.TRUE );

    boolean bSelectionStar = false;
    if (event.getSelection() != null && "*".equals( event.getSelection().get(0))) {
      event.setSelection(null);
      bSelectionStar = true; // differentiation needed, due to different semantic of "event.getSelection() == null" tweaks vs. nonTweaks
    }

    if (event.getSelection() == null && !bDefaultSelectionHandling && !bSelectionStar)
      event.setSelection(Arrays.asList("id", "type"));

    hmap.put("algorithm", "distribution");
    hmap.put("strength", TweaksSQL.calculateDistributionStrength(estimationResult.rCount, Math.max(Math.min((int) tweakParams.getOrDefault(TweaksSQL.ENSURE_SAMPLINGTHRESHOLD, 10), 100), 10) * 1000));
    return hmap;
  }
}
