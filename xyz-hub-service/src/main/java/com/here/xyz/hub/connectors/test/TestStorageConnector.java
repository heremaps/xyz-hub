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

package com.here.xyz.hub.connectors.test;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.connectors.StorageConnector;
import com.here.xyz.events.feature.DeleteFeaturesByTagEvent;
import com.here.xyz.events.Event;
import com.here.xyz.events.feature.GetFeaturesByBBoxEvent;
import com.here.xyz.events.feature.GetFeaturesByGeometryEvent;
import com.here.xyz.events.feature.GetFeaturesByIdEvent;
import com.here.xyz.events.feature.GetFeaturesByTileEvent;
import com.here.xyz.events.info.GetHistoryStatisticsEvent;
import com.here.xyz.events.info.GetStatisticsEvent;
import com.here.xyz.events.info.GetStorageStatisticsEvent;
import com.here.xyz.events.feature.IterateFeaturesEvent;
import com.here.xyz.events.feature.history.IterateHistoryEvent;
import com.here.xyz.events.feature.LoadFeaturesEvent;
import com.here.xyz.events.feature.ModifyFeaturesEvent;
import com.here.xyz.events.space.ModifySpaceEvent;
import com.here.xyz.events.admin.ModifySubscriptionEvent;
import com.here.xyz.events.feature.SearchForFeaturesEvent;
import com.here.xyz.hub.Core;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import java.util.Arrays;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * A connector for testing handling of error responses in the service.
 */
public class TestStorageConnector extends StorageConnector {

  //NOTE: this is is a special space ID. For it the connector will return a feature with a random id for each tile request.
  public static final String RANDOM_FEATURE_SPACE = "random_feature_test";
  public static final String HUGE_RESPONSE_SPACE = "huge_response_test_";
  private static Feature sampleKBFeature = new Feature()
      .withId(RandomStringUtils.randomAlphanumeric(16))
      .withGeometry(new Point().withCoordinates(new PointCoordinates(10, 10)))
      .withProperties(new Properties().withXyzNamespace(new XyzNamespace()
          .withCreatedAt(Core.currentTimeMillis())
          .withUpdatedAt(Core.currentTimeMillis())));

  @Override
  protected XyzResponse processModifySpaceEvent(ModifySpaceEvent event) throws Exception {
    if (RANDOM_FEATURE_SPACE.equals(event.getSpace())) {
      return new SuccessResponse();
    }
    if (event.getSpace().contains(HUGE_RESPONSE_SPACE)) {
      return new SuccessResponse();
    }
    if (XyzError.forValue(event.getSpace()) != null) {
      return new SuccessResponse();
    }
    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(event.getSpace()), event.getSpace() + " message.");
  }

  @Override
  protected XyzResponse processModifySubscriptionEvent(ModifySubscriptionEvent event) throws Exception {
    // Needs further implementation
    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(event.getSpace()), event.getSpace() + " message.");
  }

  @Override
  protected XyzResponse processGetStatistics(GetStatisticsEvent event) throws Exception {
    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(event.getSpace()), event.getSpace() + " message.");
  }

  @Override
  protected XyzResponse processGetHistoryStatisticsEvent(GetHistoryStatisticsEvent event) throws Exception {
    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(event.getSpace()), event.getSpace() + " message.");
  }

  @Override
  protected XyzResponse processGetFeaturesByIdEvent(GetFeaturesByIdEvent event) throws Exception {
    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(event.getSpace()), event.getSpace() + " message.");
  }

  @Override
  protected XyzResponse processGetFeaturesByGeometryEvent(GetFeaturesByGeometryEvent event) throws Exception {
    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(event.getSpace()), event.getSpace() + " message.");
  }

  @Override
  protected XyzResponse processGetFeaturesByBBoxEvent(GetFeaturesByBBoxEvent event) throws Exception {
    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(event.getSpace()), event.getSpace() + " message.");
  }

  @Override
  protected XyzResponse processGetFeaturesByTileEvent(GetFeaturesByTileEvent event) throws Exception {
    final String space = event.getSpace();
    if (space.equals(RANDOM_FEATURE_SPACE)) {
      FeatureCollection fc = new FeatureCollection()
          .withFeatures(Arrays.asList(new Feature()
              .withId(RandomStringUtils.randomAlphanumeric(16))
              .withGeometry(new Point().withCoordinates(new PointCoordinates(0, 0)))
              .withProperties(new Properties())));
      return fc;
    } else if (space.contains(HUGE_RESPONSE_SPACE)) {
      int size = Integer.parseInt(space.substring(space.lastIndexOf("_")+1)) * 1024 * 1024;

      int numFeatures = size / sampleKBFeature.toByteArray().length;

      FeatureCollection fc = new FeatureCollection();

      for (int i = 0; i < numFeatures; i++) {
        fc.getFeatures().add(sampleKBFeature.<Feature>copy().withId(RandomStringUtils.randomAlphanumeric(16)));
      }
      return fc;
    }
    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(space), space + " message.");
  }

  @Override
  protected XyzResponse processIterateFeaturesEvent(IterateFeaturesEvent event) throws Exception {
    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(event.getSpace()), event.getSpace() + " message.");
  }

  @Override
  protected XyzResponse processSearchForFeaturesEvent(SearchForFeaturesEvent event) throws Exception {
    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(event.getSpace()), event.getSpace() + " message.");
  }

  @Override
  protected XyzResponse processDeleteFeaturesByTagEvent(DeleteFeaturesByTagEvent event) throws Exception {
    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(event.getSpace()), event.getSpace() + " message.");
  }

  @Override
  protected XyzResponse processLoadFeaturesEvent(LoadFeaturesEvent event) throws Exception {
    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(event.getSpace()), event.getSpace() + " message.");
  }

  @Override
  protected XyzResponse processModifyFeaturesEvent(ModifyFeaturesEvent event) throws Exception {
    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(event.getSpace()), event.getSpace() + " message.");
  }

  @Override
  protected XyzResponse processIterateHistoryEvent(IterateHistoryEvent event) throws Exception {
    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(event.getSpace()), event.getSpace() + " message.");
  }

  @Override
  protected XyzResponse processGetStorageStatisticsEvent(GetStorageStatisticsEvent event) throws Exception {
    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(event.getSpace()), event.getSpace() + " message.");
  }

  @Override
  protected void initialize(Event event) throws Exception {

  }
}
