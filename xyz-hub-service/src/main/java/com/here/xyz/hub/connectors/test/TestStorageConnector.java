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

import static com.here.xyz.responses.XyzError.ILLEGAL_ARGUMENT;

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
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.responses.ChangesetsStatisticsResponse;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.StorageStatistics;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.changesets.ChangesetCollection;
import com.here.xyz.util.service.Core;
import java.util.Arrays;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * A connector for testing handling of error responses in the service.
 */
public class TestStorageConnector extends StorageConnector {
  //NOTE: this is is a special space ID. For it the connector will return a feature with a random id for each tile request.
  public static final String RANDOM_FEATURE_SPACE = "random_feature_test";
  public static final String ILLEGAL_ARGUMENT_SPACE = "illegal_argument_test";
  public static final String HUGE_RESPONSE_SPACE = "huge_response_test_";
  private static Feature sampleKBFeature = new Feature()
      .withId(RandomStringUtils.randomAlphanumeric(16))
      .withGeometry(new Point().withCoordinates(new PointCoordinates(10, 10)))
      .withProperties(new Properties().withXyzNamespace(new XyzNamespace()
          .withCreatedAt(Core.currentTimeMillis())
          .withUpdatedAt(Core.currentTimeMillis())));

  @Override
  protected SuccessResponse processModifySpaceEvent(ModifySpaceEvent event) throws Exception {
    if (RANDOM_FEATURE_SPACE.equals(event.getSpace()) || ILLEGAL_ARGUMENT_SPACE.equals(event.getSpace())
        || event.getSpace().contains(HUGE_RESPONSE_SPACE))
      return new SuccessResponse();

    if (XyzError.forValue(event.getSpace()) != null)
      return new SuccessResponse();

    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(event.getSpace()), event.getSpace() + " message.");
  }

  @Override
  protected SuccessResponse processModifySubscriptionEvent(ModifySubscriptionEvent event) throws Exception {
    //Needs further implementation
    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(event.getSpace()), event.getSpace() + " message.");
  }

  @Override
  protected StatisticsResponse processGetStatistics(GetStatisticsEvent event) throws Exception {
    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(event.getSpace()), event.getSpace() + " message.");
  }

  @Override
  protected FeatureCollection processGetFeaturesByIdEvent(GetFeaturesByIdEvent event) throws Exception {
    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(event.getSpace()), event.getSpace() + " message.");
  }

  @Override
  protected FeatureCollection processGetFeaturesByGeometryEvent(GetFeaturesByGeometryEvent event) throws Exception {
    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(event.getSpace()), event.getSpace() + " message.");
  }

  @Override
  protected FeatureCollection processGetFeaturesByBBoxEvent(GetFeaturesByBBoxEvent event) throws Exception {
    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(event.getSpace()), event.getSpace() + " message.");
  }

  @Override
  protected FeatureCollection processGetFeaturesByTileEvent(GetFeaturesByTileEvent event) throws Exception {
    final String space = event.getSpace();
    if (space.equals(RANDOM_FEATURE_SPACE)) {
      FeatureCollection fc = new FeatureCollection()
          .withFeatures(Arrays.asList(new Feature()
              .withId(RandomStringUtils.randomAlphanumeric(16))
              .withGeometry(new Point().withCoordinates(new PointCoordinates(0, 0)))
              .withProperties(new Properties())));
      return fc;
    }
    else if (space.contains(HUGE_RESPONSE_SPACE)) {
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
  protected FeatureCollection processIterateFeaturesEvent(IterateFeaturesEvent event) throws Exception {
    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(event.getSpace()), event.getSpace() + " message.");
  }

  @Override
  protected FeatureCollection processSearchForFeaturesEvent(SearchForFeaturesEvent event) throws Exception {
    if (ILLEGAL_ARGUMENT_SPACE.equals(event.getSpace()))
      throw new ErrorResponseException(ILLEGAL_ARGUMENT, "Invalid request parameters.");

    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(event.getSpace()), event.getSpace() + " message.");
  }

  @Override
  protected FeatureCollection processLoadFeaturesEvent(LoadFeaturesEvent event) throws Exception {
    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(event.getSpace()), event.getSpace() + " message.");
  }

  @Override
  protected FeatureCollection processModifyFeaturesEvent(ModifyFeaturesEvent event) throws Exception {
    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(event.getSpace()), event.getSpace() + " message.");
  }

  @Override
  protected FeatureCollection processWriteFeaturesEvent(WriteFeaturesEvent event) throws Exception {
    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(event.getSpace()), event.getSpace() + " message.");
  }

  @Override
  protected StorageStatistics processGetStorageStatisticsEvent(GetStorageStatisticsEvent event) throws Exception {
    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(event.getSpace()), event.getSpace() + " message.");
  }

  @Override
  protected SuccessResponse processDeleteChangesetsEvent(DeleteChangesetsEvent event) throws Exception {
    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(event.getSpace()), event.getSpace() + " message.");
  }

  @Override
  protected ChangesetCollection processIterateChangesetsEvent(IterateChangesetsEvent event) throws Exception {
    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(event.getSpace()), event.getSpace() + " message.");
  }

  @Override
  protected ChangesetsStatisticsResponse processGetChangesetsStatisticsEvent(GetChangesetStatisticsEvent event) throws Exception {
    throw new ErrorResponseException(event.getStreamId(), XyzError.forValue(event.getSpace()), event.getSpace() + " message.");
  }

  @Override
  protected void handleProcessingException(Exception exception, Event event) throws Exception {
    throw exception;
  }

  @Override
  protected void initialize(Event event) throws Exception {

  }
}
