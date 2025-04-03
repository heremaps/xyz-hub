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

package com.here.xyz.hub.connectors.test;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.connectors.StorageConnector;
import com.here.xyz.events.GetChangesetStatisticsEvent;
import com.here.xyz.events.DeleteChangesetsEvent;
import com.here.xyz.events.Event;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.GetHistoryStatisticsEvent;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.GetStorageStatisticsEvent;
import com.here.xyz.events.IterateChangesetsEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.IterateHistoryEvent;
import com.here.xyz.events.LoadFeaturesEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.ModifySubscriptionEvent;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.StatisticsResponse.Value;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class InMemoryStorage extends StorageConnector {

  private static Map<String, Feature> storage = new ConcurrentHashMap<>();

  @Override
  protected XyzResponse processModifySpaceEvent(ModifySpaceEvent event) throws Exception {
    if (event.getSpace() != null)
      return new SuccessResponse();
    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(event.getSpace()), event.getSpace() + " message.");
  }

  @Override
  protected XyzResponse processModifySubscriptionEvent(ModifySubscriptionEvent event) throws Exception {
    throw new UnsupportedOperationException(event.getClass().getSimpleName() + " not implemented.");
  }

  @Override
  protected XyzResponse processGetStatistics(GetStatisticsEvent event) throws Exception {
    throw new UnsupportedOperationException(event.getClass().getSimpleName() + " not implemented.");
  }

  @Override
  protected XyzResponse processGetHistoryStatisticsEvent(GetHistoryStatisticsEvent event) throws Exception {
    throw new UnsupportedOperationException(event.getClass().getSimpleName() + " not implemented.");
  }

  @Override
  protected XyzResponse processGetFeaturesByIdEvent(GetFeaturesByIdEvent event) throws Exception {
    return new FeatureCollection()
        .withFeatures(event.getIds().stream().map(id -> storage.get(id)).filter(f -> f != null).collect(Collectors.toList()));
  }

  @Override
  protected XyzResponse processGetFeaturesByGeometryEvent(GetFeaturesByGeometryEvent event) throws Exception {
    throw new UnsupportedOperationException(event.getClass().getSimpleName() + " not implemented.");
  }

  @Override
  protected XyzResponse processGetFeaturesByBBoxEvent(GetFeaturesByBBoxEvent event) throws Exception {
    throw new UnsupportedOperationException(event.getClass().getSimpleName() + " not implemented.");
  }

  @Override
  protected XyzResponse processGetFeaturesByTileEvent(GetFeaturesByTileEvent event) throws Exception {
    return new FeatureCollection()
        .withFeatures(new ArrayList<>(storage.values()));
  }

  @Override
  protected XyzResponse processIterateFeaturesEvent(IterateFeaturesEvent event) throws Exception {
    return new FeatureCollection()
        .withFeatures(new ArrayList<>(storage.values()));
  }

  @Override
  protected XyzResponse processSearchForFeaturesEvent(SearchForFeaturesEvent event) throws Exception {
    throw new UnsupportedOperationException(event.getClass().getSimpleName() + " not implemented.");
  }

  @Override
  protected XyzResponse processLoadFeaturesEvent(LoadFeaturesEvent event) throws Exception {
    return new FeatureCollection()
        .withFeatures(Collections.emptyList());
  }

  @Override
  protected XyzResponse processModifyFeaturesEvent(ModifyFeaturesEvent event) throws Exception {
    event.getInsertFeatures().forEach(f -> storage.put(f.getId(), f));
    return new FeatureCollection()
        .withFeatures(event.getInsertFeatures())
        .withInserted(event.getInsertFeatures().stream().map(f -> f.getId()).collect(Collectors.toList()));
  }

  @Override
  protected XyzResponse processIterateHistoryEvent(IterateHistoryEvent event) throws Exception {
    throw new UnsupportedOperationException(event.getClass().getSimpleName() + " not implemented.");
  }

  @Override
  protected XyzResponse processGetStorageStatisticsEvent(GetStorageStatisticsEvent event) throws Exception {
    return new StatisticsResponse()
        .withCount(new Value<>((long) storage.size()).withEstimated(false));
  }

  @Override
  protected XyzResponse processDeleteChangesetsEvent(DeleteChangesetsEvent event) throws Exception {
    throw new UnsupportedOperationException(event.getClass().getSimpleName() + " not implemented.");
  }

  @Override
  protected XyzResponse processIterateChangesetsEvent(IterateChangesetsEvent event) throws Exception {
    throw new UnsupportedOperationException(event.getClass().getSimpleName() + " not implemented.");
  }

  @Override
  protected XyzResponse processGetChangesetsStatisticsEvent(GetChangesetStatisticsEvent event) throws Exception {
    throw new UnsupportedOperationException(event.getClass().getSimpleName() + " not implemented.");
  }

  @Override
  protected void initialize(Event event) throws Exception {}
}
