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

package com.here.xyz.connectors;

import com.here.xyz.Payload;
import com.here.xyz.events.feature.DeleteFeaturesByTagEvent;
import com.here.xyz.events.Event;
import com.here.xyz.events.feature.GetFeaturesByBBoxEvent;
import com.here.xyz.events.feature.GetFeaturesByGeometryEvent;
import com.here.xyz.events.feature.GetFeaturesByIdEvent;
import com.here.xyz.events.feature.GetFeaturesByTileEvent;
import com.here.xyz.events.info.GetStatisticsEvent;
import com.here.xyz.events.info.HealthCheckEvent;
import com.here.xyz.events.feature.IterateFeaturesEvent;
import com.here.xyz.events.feature.ModifyFeaturesEvent;
import com.here.xyz.events.space.ModifySpaceEvent;
import com.here.xyz.events.admin.ModifySubscriptionEvent;
import com.here.xyz.events.feature.SearchForFeaturesEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.ModifiedEventResponse;
import com.here.xyz.responses.ModifiedResponseResponse;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;

/**
 * This class could be extended by any processor connector implementations.
 */
@SuppressWarnings({"WeakerAccess", "unused", "rawtypes"})
public abstract class ProcessorConnector extends AbstractConnectorHandler {

  public static final String REQUEST = ".request";
  public static final String RESPONSE = ".response";

  @Override
  protected XyzResponse processEvent(Event event) throws Exception {
    if (event instanceof HealthCheckEvent) {
      return processHealthCheckEvent((HealthCheckEvent) event);
    } else {
      throw new ErrorResponseException(streamId, XyzError.NOT_IMPLEMENTED,
          "Unknown notification type '" + event.getClass().getSimpleName() + "'");
    }
  }

  private ModifiedEventResponse wrapEvent(Event event) {
    return new ModifiedEventResponse().withEvent(event);
  }

  private ModifiedResponseResponse wrapResponse(Payload response) {
    return new ModifiedResponseResponse().withResponse(response);
  }

  // Override the method below in your implementation
  @SuppressWarnings("RedundantThrows")
  @Override
  protected void initialize(Event event) throws Exception {
  }

  protected GetFeaturesByIdEvent processGetFeaturesById(GetFeaturesByIdEvent event, NotificationParams notificationParams)
      throws Exception {
    throw new ErrorResponseException(streamId, XyzError.NOT_IMPLEMENTED, "Not implemented by this processor");
  }

  protected FeatureCollection processGetFeaturesById(FeatureCollection response, NotificationParams notificationParams) throws Exception {
    throw new ErrorResponseException(streamId, XyzError.NOT_IMPLEMENTED, "Not implemented by this processor");
  }

  protected GetFeaturesByBBoxEvent processGetFeaturesByBBox(GetFeaturesByBBoxEvent event, NotificationParams notificationParams)
      throws Exception {
    throw new ErrorResponseException(streamId, XyzError.NOT_IMPLEMENTED, "Not implemented by this processor");
  }

  protected FeatureCollection processGetFeaturesByBBox(FeatureCollection response, NotificationParams notificationParams) throws Exception {
    throw new ErrorResponseException(streamId, XyzError.NOT_IMPLEMENTED, "Not implemented by this processor");
  }

  protected GetFeaturesByGeometryEvent processGetFeaturesByGeometry(GetFeaturesByGeometryEvent event, NotificationParams notificationParams) throws Exception {
    throw new ErrorResponseException(streamId, XyzError.NOT_IMPLEMENTED, "Not implemented by this processor");
  }

  protected FeatureCollection processGetFeaturesByGeometry(FeatureCollection response, NotificationParams notificationParams) throws Exception {
    throw new ErrorResponseException(streamId, XyzError.NOT_IMPLEMENTED, "Not implemented by this processor");
  }

  protected GetFeaturesByTileEvent processGetFeaturesByTile(GetFeaturesByTileEvent event, NotificationParams notificationParams)
      throws Exception {
    throw new ErrorResponseException(streamId, XyzError.NOT_IMPLEMENTED, "Not implemented by this processor");
  }

  protected FeatureCollection processGetFeaturesByTile(FeatureCollection response, NotificationParams notificationParams) throws Exception {
    throw new ErrorResponseException(streamId, XyzError.NOT_IMPLEMENTED, "Not implemented by this processor");
  }

  protected IterateFeaturesEvent processIterateFeatures(IterateFeaturesEvent event, NotificationParams notificationParams)
      throws Exception {
    throw new ErrorResponseException(streamId, XyzError.NOT_IMPLEMENTED, "Not implemented by this processor");
  }

  protected FeatureCollection processIterateFeatures(FeatureCollection response, NotificationParams notificationParams) throws Exception {
    throw new ErrorResponseException(streamId, XyzError.NOT_IMPLEMENTED, "Not implemented by this processor");
  }

  protected SearchForFeaturesEvent processSearchForFeatures(SearchForFeaturesEvent event, NotificationParams notificationParams)
      throws Exception {
    throw new ErrorResponseException(streamId, XyzError.NOT_IMPLEMENTED, "Not implemented by this processor");
  }

  protected FeatureCollection processSearchForFeatures(FeatureCollection response, NotificationParams notificationParams) throws Exception {
    throw new ErrorResponseException(streamId, XyzError.NOT_IMPLEMENTED, "Not implemented by this processor");
  }

  protected DeleteFeaturesByTagEvent processDeleteFeaturesByTag(DeleteFeaturesByTagEvent event, NotificationParams notificationParams)
      throws Exception {
    throw new ErrorResponseException(streamId, XyzError.NOT_IMPLEMENTED, "Not implemented by this processor");
  }

  protected FeatureCollection processDeleteFeaturesByTag(FeatureCollection response, NotificationParams notificationParams)
      throws Exception {
    throw new ErrorResponseException(streamId, XyzError.NOT_IMPLEMENTED, "Not implemented by this processor");
  }

  protected ModifyFeaturesEvent processModifyFeatures(ModifyFeaturesEvent event, NotificationParams notificationParams) throws Exception {
    throw new ErrorResponseException(streamId, XyzError.NOT_IMPLEMENTED, "Not implemented by this processor");
  }

  protected FeatureCollection processModifyFeatures(FeatureCollection response, NotificationParams notificationParams) throws Exception {
    throw new ErrorResponseException(streamId, XyzError.NOT_IMPLEMENTED, "Not implemented by this processor");
  }

  protected ModifySpaceEvent processModifySpace(ModifySpaceEvent event, NotificationParams notificationParams) throws Exception {
    throw new ErrorResponseException(streamId, XyzError.NOT_IMPLEMENTED, "Not implemented by this processor");
  }

  protected SuccessResponse processModifySpace(SuccessResponse response, NotificationParams notificationParams) throws Exception {
    throw new ErrorResponseException(streamId, XyzError.NOT_IMPLEMENTED, "Not implemented by this processor");
  }

  protected ModifySubscriptionEvent processModifySubscription(ModifySubscriptionEvent event, NotificationParams notificationParams) throws Exception {
    throw new ErrorResponseException(streamId, XyzError.NOT_IMPLEMENTED, "Not implemented by this processor");
  }

  protected SuccessResponse processModifySubscription(SuccessResponse response, NotificationParams notificationParams) throws Exception {
    throw new ErrorResponseException(streamId, XyzError.NOT_IMPLEMENTED, "Not implemented by this processor");
  }

  protected GetStatisticsEvent processGetStatistics(GetStatisticsEvent event, NotificationParams notificationParams) throws Exception {
    throw new ErrorResponseException(streamId, XyzError.NOT_IMPLEMENTED, "Not implemented by this processor");
  }

  protected StatisticsResponse processGetStatistics(StatisticsResponse response, NotificationParams notificationParams) throws Exception {
    throw new ErrorResponseException(streamId, XyzError.NOT_IMPLEMENTED, "Not implemented by this processor");
  }

  protected ErrorResponse processErrorResponse(ErrorResponse response, String eventType, NotificationParams notificationParams)
      throws Exception {
    throw new ErrorResponseException(streamId, XyzError.NOT_IMPLEMENTED, "Not implemented by this processor");
  }
}
