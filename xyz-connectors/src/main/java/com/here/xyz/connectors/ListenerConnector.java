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


import com.here.xyz.Typed;
import com.here.xyz.events.DeleteFeaturesByTagEvent;
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
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.SuccessResponse;

/**
 * This class could be extended by any listener connector implementations.
 */
@SuppressWarnings({"WeakerAccess", "EmptyMethod", "unused"})
public abstract class ListenerConnector extends AbstractConnectorHandler {

  @Override
  protected Typed processEvent(Event event) throws Exception {
    if (event instanceof HealthCheckEvent) {
      return processHealthCheckEvent((HealthCheckEvent) event);
    } else if (event instanceof EventNotification) {
      processEventNotification((EventNotification) event);
    } else {
      throw new ErrorResponseException(streamId, XyzError.NOT_IMPLEMENTED,
          "Unknown notification type '" + event.getClass().getSimpleName() + "'");
    }
    return null;
  }

  @SuppressWarnings("RedundantThrows")
  @Override
  protected void initialize(Event event) throws Exception {
  }

  public void processEventNotification(EventNotification notification) throws Exception {
    if (notification == null) {
      throw new ErrorResponseException(streamId, XyzError.NOT_IMPLEMENTED, "Unknown event type");
    }

    final NotificationParams notificationParams = new NotificationParams(
        eventDecryptor.decryptParams(notification.getParams()),
        eventDecryptor.decryptParams(notification.getConnectorParams()),
        eventDecryptor.decryptParams(notification.getMetadata()),
        notification.getTid(), notification.getAid(), notification.getJwt());

    if (notification.getEvent() instanceof ErrorResponse) {
      processErrorResponse((ErrorResponse) notification.getEvent(), notification.getEventType(), notificationParams);
    }

    final String eventType = notification.getEventType();

    if ((ModifySpaceEvent.class.getSimpleName() + REQUEST).equals(eventType)) {
      processModifySpace((ModifySpaceEvent) notification.getEvent(), notificationParams);
    }
    if ((ModifySpaceEvent.class.getSimpleName() + RESPONSE).equals(eventType)) {
      processModifySpace((SuccessResponse) notification.getEvent(), notificationParams);
    }
    if ((GetStatisticsEvent.class.getSimpleName() + REQUEST).equals(eventType)) {
      processGetStatistics((GetStatisticsEvent) notification.getEvent(), notificationParams);
    }
    if ((GetStatisticsEvent.class.getSimpleName() + RESPONSE).equals(eventType)) {
      processGetStatistics((StatisticsResponse) notification.getEvent(), notificationParams);
    }
    if ((GetFeaturesByIdEvent.class.getSimpleName() + REQUEST).equals(eventType)) {
      processGetFeaturesById((GetFeaturesByIdEvent) notification.getEvent(), notificationParams);
    }
    if ((GetFeaturesByIdEvent.class.getSimpleName() + RESPONSE).equals(eventType)) {
      processGetFeaturesById((FeatureCollection) notification.getEvent(), notificationParams);
    }
    if ((SearchForFeaturesEvent.class.getSimpleName() + REQUEST).equals(eventType)) {
      processSearchForFeatures((SearchForFeaturesEvent) notification.getEvent(), notificationParams);
    }
    if ((SearchForFeaturesEvent.class.getSimpleName() + RESPONSE).equals(eventType)) {
      processSearchForFeatures((FeatureCollection) notification.getEvent(), notificationParams);
    }
    if ((IterateFeaturesEvent.class.getSimpleName() + REQUEST).equals(eventType)) {
      processIterateFeatures((IterateFeaturesEvent) notification.getEvent(), notificationParams);
    }
    if ((IterateFeaturesEvent.class.getSimpleName() + RESPONSE).equals(eventType)) {
      processIterateFeatures((FeatureCollection) notification.getEvent(), notificationParams);
    }
    if ((GetFeaturesByTileEvent.class.getSimpleName() + REQUEST).equals(eventType)) {
      processGetFeaturesByTile((GetFeaturesByTileEvent) notification.getEvent(), notificationParams);
    }
    if ((GetFeaturesByTileEvent.class.getSimpleName() + RESPONSE).equals(eventType)) {
      processGetFeaturesByTile((FeatureCollection) notification.getEvent(), notificationParams);
    }
    if ((GetFeaturesByBBoxEvent.class.getSimpleName() + REQUEST).equals(eventType)) {
      processGetFeaturesByBBox((GetFeaturesByBBoxEvent) notification.getEvent(), notificationParams);
    }
    if ((GetFeaturesByBBoxEvent.class.getSimpleName() + RESPONSE).equals(eventType)) {
      processGetFeaturesByBBox((FeatureCollection) notification.getEvent(), notificationParams);
    }
    if ((GetFeaturesByGeometryEvent.class.getSimpleName() + REQUEST).equals(eventType)) {
      processGetFeaturesByGeometry((GetFeaturesByGeometryEvent) notification.getEvent(), notificationParams);
    }
    if ((GetFeaturesByGeometryEvent.class.getSimpleName() + RESPONSE).equals(eventType)) {
      processGetFeaturesByGeometry((FeatureCollection) notification.getEvent(), notificationParams);
    }
    if ((ModifyFeaturesEvent.class.getSimpleName() + REQUEST).equals(eventType)) {
      processModifyFeatures((ModifyFeaturesEvent) notification.getEvent(), notificationParams);
    }
    if ((ModifyFeaturesEvent.class.getSimpleName() + RESPONSE).equals(eventType)) {
      processModifyFeatures((FeatureCollection) notification.getEvent(), notificationParams);
    }
    if ((DeleteFeaturesByTagEvent.class.getSimpleName() + REQUEST).equals(eventType)) {
      processDeleteFeaturesByTag((DeleteFeaturesByTagEvent) notification.getEvent(), notificationParams);
    }
    if ((DeleteFeaturesByTagEvent.class.getSimpleName() + RESPONSE).equals(eventType)) {
      processDeleteFeaturesByTag((FeatureCollection) notification.getEvent(), notificationParams);
    }
  }

  @SuppressWarnings("RedundantThrows")
  protected void processGetFeaturesById(GetFeaturesByIdEvent event, NotificationParams notificationParams) throws Exception {
  }

  @SuppressWarnings("RedundantThrows")
  protected void processGetFeaturesById(FeatureCollection response, NotificationParams notificationParams) throws Exception {
  }

  @SuppressWarnings("RedundantThrows")
  protected void processGetFeaturesByBBox(GetFeaturesByBBoxEvent event, NotificationParams notificationParams) throws Exception {
  }

  @SuppressWarnings("RedundantThrows")
  protected void processGetFeaturesByBBox(FeatureCollection response, NotificationParams notificationParams) throws Exception {
  }

  @SuppressWarnings("RedundantThrows")
  protected void processGetFeaturesByGeometry(GetFeaturesByGeometryEvent event, NotificationParams notificationParams) throws Exception {
  }

  @SuppressWarnings("RedundantThrows")
  protected void processGetFeaturesByGeometry(FeatureCollection response, NotificationParams notificationParams) throws Exception {
  }

  @SuppressWarnings("RedundantThrows")
  protected void processGetFeaturesByTile(GetFeaturesByTileEvent event, NotificationParams notificationParams) throws Exception {
  }

  @SuppressWarnings("RedundantThrows")
  protected void processGetFeaturesByTile(FeatureCollection response, NotificationParams notificationParams) throws Exception {
  }

  @SuppressWarnings("RedundantThrows")
  protected void processIterateFeatures(IterateFeaturesEvent event, NotificationParams notificationParams) throws Exception {
  }

  @SuppressWarnings("RedundantThrows")
  protected void processIterateFeatures(FeatureCollection response, NotificationParams notificationParams) throws Exception {
  }

  @SuppressWarnings("RedundantThrows")
  protected void processSearchForFeatures(SearchForFeaturesEvent event, NotificationParams notificationParams) throws Exception {
  }

  @SuppressWarnings("RedundantThrows")
  protected void processSearchForFeatures(FeatureCollection response, NotificationParams notificationParams) throws Exception {
  }

  @SuppressWarnings("RedundantThrows")
  protected void processDeleteFeaturesByTag(DeleteFeaturesByTagEvent event, NotificationParams notificationParams) throws Exception {
  }

  @SuppressWarnings("RedundantThrows")
  protected void processDeleteFeaturesByTag(FeatureCollection response, NotificationParams notificationParams) throws Exception {
  }

  @SuppressWarnings("RedundantThrows")
  protected void processModifyFeatures(ModifyFeaturesEvent event, NotificationParams notificationParams) throws Exception {
  }

  @SuppressWarnings("RedundantThrows")
  protected void processModifyFeatures(FeatureCollection response, NotificationParams notificationParams) throws Exception {
  }

  @SuppressWarnings("RedundantThrows")
  protected void processModifySpace(ModifySpaceEvent event, NotificationParams notificationParams) throws Exception {
  }

  @SuppressWarnings("RedundantThrows")
  protected void processModifySpace(SuccessResponse response, NotificationParams notificationParams) throws Exception {
  }

  @SuppressWarnings("RedundantThrows")
  protected void processGetStatistics(GetStatisticsEvent event, NotificationParams notificationParams) throws Exception {
  }

  @SuppressWarnings("RedundantThrows")
  protected void processGetStatistics(StatisticsResponse response, NotificationParams notificationParams) throws Exception {
  }

  @SuppressWarnings("RedundantThrows")
  protected void processErrorResponse(ErrorResponse response, String eventType, NotificationParams notificationParams) throws Exception {
  }
}
