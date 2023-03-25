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
import com.here.xyz.events.feature.SearchForFeaturesEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.SuccessResponse;

/**
 * This class could be extended by any listener connector implementations.
 */
@SuppressWarnings({"WeakerAccess", "EmptyMethod", "unused", "rawtypes"})
public abstract class ListenerConnector extends AbstractConnectorHandler {

  @Override
  protected Typed processEvent(Event event) throws Exception {
    if (event instanceof HealthCheckEvent) {
      return processHealthCheckEvent((HealthCheckEvent) event);
    } else {
      throw new ErrorResponseException(streamId, XyzError.NOT_IMPLEMENTED,
          "Unknown notification type '" + event.getClass().getSimpleName() + "'");
    }
  }

  @SuppressWarnings("RedundantThrows")
  @Override
  protected void initialize(Event event) throws Exception {
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

  protected void processSearchForFeatures(FeatureCollection response, NotificationParams notificationParams) throws Exception {
  }

  @SuppressWarnings("RedundantThrows")
  protected void processDeleteFeaturesByTag(DeleteFeaturesByTagEvent event, NotificationParams notificationParams) throws Exception {
  }

  @SuppressWarnings("RedundantThrows")
  protected void processDeleteFeaturesByTag(FeatureCollection response, NotificationParams notificationParams) throws Exception {
  }

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
