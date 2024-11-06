/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.connectors.StorageConnector;
import com.here.xyz.events.DeleteChangesetsEvent;
import com.here.xyz.events.Event;
import com.here.xyz.events.GetChangesetStatisticsEvent;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.GetStorageStatisticsEvent;
import com.here.xyz.events.IterateChangesetsEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.LoadFeaturesEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.ModifySubscriptionEvent;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.events.WriteFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.responses.ChangesetsStatisticsResponse;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.StorageStatistics;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.changesets.ChangesetCollection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class InMemoryStorage extends StorageConnector {

  private static Map<String, Feature> storage = new ConcurrentHashMap<>();

  @Override
  protected SuccessResponse processModifySpaceEvent(ModifySpaceEvent event) throws Exception {
    if (event.getSpace() != null)
      return new SuccessResponse();
    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(event.getSpace()), event.getSpace() + " message.");
  }

  @Override
  protected SuccessResponse processModifySubscriptionEvent(ModifySubscriptionEvent event) throws Exception {
    throw new UnsupportedOperationException(event.getClass().getSimpleName() + " not implemented.");
  }

  @Override
  protected StatisticsResponse processGetStatistics(GetStatisticsEvent event) throws Exception {
    throw new UnsupportedOperationException(event.getClass().getSimpleName() + " not implemented.");
  }

  @Override
  protected FeatureCollection processGetFeaturesByIdEvent(GetFeaturesByIdEvent event) throws Exception {
    return new FeatureCollection()
        .withFeatures(event.getIds().stream().map(id -> storage.get(id)).filter(f -> f != null).collect(Collectors.toList()));
  }

  @Override
  protected FeatureCollection processGetFeaturesByGeometryEvent(GetFeaturesByGeometryEvent event) throws Exception {
    throw new UnsupportedOperationException(event.getClass().getSimpleName() + " not implemented.");
  }

  @Override
  protected FeatureCollection processGetFeaturesByBBoxEvent(GetFeaturesByBBoxEvent event) throws Exception {
    throw new UnsupportedOperationException(event.getClass().getSimpleName() + " not implemented.");
  }

  @Override
  protected FeatureCollection processGetFeaturesByTileEvent(GetFeaturesByTileEvent event) throws Exception {
    return new FeatureCollection()
        .withFeatures(new ArrayList<>(storage.values()));
  }

  @Override
  protected FeatureCollection processIterateFeaturesEvent(IterateFeaturesEvent event) throws Exception {
    return new FeatureCollection()
        .withFeatures(new ArrayList<>(storage.values()));
  }

  @Override
  protected FeatureCollection processSearchForFeaturesEvent(SearchForFeaturesEvent event) throws Exception {
    throw new UnsupportedOperationException(event.getClass().getSimpleName() + " not implemented.");
  }

  @Override
  protected FeatureCollection processLoadFeaturesEvent(LoadFeaturesEvent event) throws Exception {
    return new FeatureCollection()
        .withFeatures(Collections.emptyList());
  }

  @Override
  protected FeatureCollection processModifyFeaturesEvent(ModifyFeaturesEvent event) throws Exception {
    event.getInsertFeatures().forEach(f -> storage.put(f.getId(), f));
    return new FeatureCollection()
        .withFeatures(event.getInsertFeatures())
        .withInserted(event.getInsertFeatures().stream().map(f -> f.getId()).collect(Collectors.toList()));
  }

  @Override
  protected FeatureCollection processWriteFeaturesEvent(WriteFeaturesEvent event) throws Exception {
    List<Feature> featuresToInsert = event.getModifications().stream().flatMap(modification -> {
      try {
        return modification.getFeatureData().getFeatures().stream();
      }
      catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }).toList();

    featuresToInsert.forEach(feature -> storage.put(feature.getId(), feature));

    return new FeatureCollection()
        .withFeatures(featuresToInsert)
        .withInserted(featuresToInsert.stream().map(feature -> feature.getId()).collect(Collectors.toList()));
  }

  @Override
  protected StorageStatistics processGetStorageStatisticsEvent(GetStorageStatisticsEvent event) throws Exception {
    throw new UnsupportedOperationException(event.getClass().getSimpleName() + " not implemented.");
  }

  @Override
  protected SuccessResponse processDeleteChangesetsEvent(DeleteChangesetsEvent event) throws Exception {
    throw new UnsupportedOperationException(event.getClass().getSimpleName() + " not implemented.");
  }

  @Override
  protected ChangesetCollection processIterateChangesetsEvent(IterateChangesetsEvent event) throws Exception {
    throw new UnsupportedOperationException(event.getClass().getSimpleName() + " not implemented.");
  }

  @Override
  protected ChangesetsStatisticsResponse processGetChangesetsStatisticsEvent(GetChangesetStatisticsEvent event) throws Exception {
    throw new UnsupportedOperationException(event.getClass().getSimpleName() + " not implemented.");
  }

  @Override
  protected void handleProcessingException(Exception exception, Event event) throws Exception {
    throw exception;
  }

  @Override
  protected void initialize(Event event) throws Exception {}
}
