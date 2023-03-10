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

package com.here.xyz.connectors;

import com.here.xyz.Payload;
import com.here.xyz.events.Event;
import com.here.xyz.events.EventNotification;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.HealthCheckEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.ModifySubscriptionEvent;
import com.here.xyz.events.SearchForFeaturesEvent;
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
    } else if (event instanceof EventNotification) {
      return processEventNotification((EventNotification) event);
    } else {
      throw new ErrorResponseException(streamId, XyzError.NOT_IMPLEMENTED,
          "Unknown notification type '" + event.getClass().getSimpleName() + "'");
    }
  }

  public XyzResponse processEventNotification(EventNotification notification) throws Exception {
    if (notification == null) {
      throw new ErrorResponseException(streamId, XyzError.NOT_IMPLEMENTED, "Unknown event type");
    }

    final NotificationParams notificationParams = new NotificationParams(
        eventDecryptor.decryptParams(notification.getParams(), notification.getSpace()),
        eventDecryptor.decryptParams(notification.getConnectorParams(), notification.getSpace()),
        eventDecryptor.decryptParams(notification.getMetadata(), notification.getSpace()),
        notification.getTid(), notification.getAid(), notification.getJwt());

    if (notification.getEvent() instanceof ErrorResponse) {
      return processErrorResponse((ErrorResponse) notification.getEvent(), notification.getEventType(), notificationParams);
    }

    final String eventType = notification.getEventType();

    if ((ModifySpaceEvent.class.getSimpleName() + REQUEST).equals(eventType)) {
      return wrapEvent(processModifySpace((ModifySpaceEvent) notification.getEvent(), notificationParams));
    }
    if ((ModifySpaceEvent.class.getSimpleName() + RESPONSE).equals(eventType)) {
      return wrapResponse(processModifySpace((SuccessResponse) notification.getEvent(), notificationParams));
    }
    if ((ModifySubscriptionEvent.class.getSimpleName() + REQUEST).equals(eventType)) {
      return wrapEvent(processModifySubscription((ModifySubscriptionEvent) notification.getEvent(), notificationParams));
    }
    if ((ModifySubscriptionEvent.class.getSimpleName() + RESPONSE).equals(eventType)) {
      return wrapResponse(processModifySubscription((SuccessResponse) notification.getEvent(), notificationParams));
    }
    if ((GetStatisticsEvent.class.getSimpleName() + REQUEST).equals(eventType)) {
      return wrapEvent(processGetStatistics((GetStatisticsEvent) notification.getEvent(), notificationParams));
    }
    if ((GetStatisticsEvent.class.getSimpleName() + RESPONSE).equals(eventType)) {
      return wrapResponse(processGetStatistics((StatisticsResponse) notification.getEvent(), notificationParams));
    }
    if ((GetFeaturesByIdEvent.class.getSimpleName() + REQUEST).equals(eventType)) {
      return wrapEvent(processGetFeaturesById((GetFeaturesByIdEvent) notification.getEvent(), notificationParams));
    }
    if ((GetFeaturesByIdEvent.class.getSimpleName() + RESPONSE).equals(eventType)) {
      return wrapResponse(processGetFeaturesById((FeatureCollection) notification.getEvent(), notificationParams));
    }
    if ((SearchForFeaturesEvent.class.getSimpleName() + REQUEST).equals(eventType)) {
      return wrapEvent(processSearchForFeatures((SearchForFeaturesEvent) notification.getEvent(), notificationParams));
    }
    if ((SearchForFeaturesEvent.class.getSimpleName() + RESPONSE).equals(eventType)) {
      return wrapResponse(processSearchForFeatures((FeatureCollection) notification.getEvent(), notificationParams));
    }
    if ((GetFeaturesByGeometryEvent.class.getSimpleName() + REQUEST).equals(eventType)) {
      return wrapEvent(processGetFeaturesByGeometry((GetFeaturesByGeometryEvent) notification.getEvent(), notificationParams));
    }
    if ((GetFeaturesByGeometryEvent.class.getSimpleName() + RESPONSE).equals(eventType)) {
      return wrapResponse(processGetFeaturesByGeometry((FeatureCollection) notification.getEvent(), notificationParams));
    }
    if ((IterateFeaturesEvent.class.getSimpleName() + REQUEST).equals(eventType)) {
      return wrapEvent(processIterateFeatures((IterateFeaturesEvent) notification.getEvent(), notificationParams));
    }
    if ((IterateFeaturesEvent.class.getSimpleName() + RESPONSE).equals(eventType)) {
      return wrapResponse(processIterateFeatures((FeatureCollection) notification.getEvent(), notificationParams));
    }
    if ((GetFeaturesByTileEvent.class.getSimpleName() + REQUEST).equals(eventType)) {
      return wrapEvent(processGetFeaturesByTile((GetFeaturesByTileEvent) notification.getEvent(), notificationParams));
    }
    if ((GetFeaturesByTileEvent.class.getSimpleName() + RESPONSE).equals(eventType)) {
      return wrapResponse(processGetFeaturesByTile((FeatureCollection) notification.getEvent(), notificationParams));
    }
    if ((GetFeaturesByBBoxEvent.class.getSimpleName() + REQUEST).equals(eventType)) {
      return wrapEvent(processGetFeaturesByBBox((GetFeaturesByBBoxEvent) notification.getEvent(), notificationParams));
    }
    if ((GetFeaturesByBBoxEvent.class.getSimpleName() + RESPONSE).equals(eventType)) {
      return wrapResponse(processGetFeaturesByBBox((FeatureCollection) notification.getEvent(), notificationParams));
    }
    if ((ModifyFeaturesEvent.class.getSimpleName() + REQUEST).equals(eventType)) {
      return wrapEvent(processModifyFeatures((ModifyFeaturesEvent) notification.getEvent(), notificationParams));
    }
    if ((ModifyFeaturesEvent.class.getSimpleName() + RESPONSE).equals(eventType)) {
      return wrapResponse(processModifyFeatures((FeatureCollection) notification.getEvent(), notificationParams));
    }

    // if any of the events were caught, throws an error.
    throw new ErrorResponseException(streamId, XyzError.NOT_IMPLEMENTED, "Unknown event type '" + eventType + "'");
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
