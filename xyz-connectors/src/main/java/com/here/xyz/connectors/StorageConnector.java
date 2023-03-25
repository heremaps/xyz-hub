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

package com.here.xyz.connectors;

import com.here.xyz.events.feature.DeleteFeaturesByTagEvent;
import com.here.xyz.events.Event;
import com.here.xyz.events.feature.GetFeaturesByBBoxEvent;
import com.here.xyz.events.feature.GetFeaturesByGeometryEvent;
import com.here.xyz.events.feature.GetFeaturesByIdEvent;
import com.here.xyz.events.feature.GetFeaturesByTileEvent;
import com.here.xyz.events.info.GetStatisticsEvent;
import com.here.xyz.events.info.GetHistoryStatisticsEvent;
import com.here.xyz.events.info.GetStorageStatisticsEvent;
import com.here.xyz.events.info.HealthCheckEvent;
import com.here.xyz.events.feature.IterateFeaturesEvent;
import com.here.xyz.events.feature.LoadFeaturesEvent;
import com.here.xyz.events.feature.ModifyFeaturesEvent;
import com.here.xyz.events.space.ModifySpaceEvent;
import com.here.xyz.events.admin.ModifySubscriptionEvent;
import com.here.xyz.events.feature.SearchForFeaturesEvent;
import com.here.xyz.events.feature.history.IterateHistoryEvent;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;

public abstract class StorageConnector extends AbstractConnectorHandler {

  @Override
  public XyzResponse processEvent(Event event) throws Exception {
    if (event == null) {
      return new ErrorResponse()
          .withStreamId(streamId)
          .withError(XyzError.EXCEPTION)
          .withErrorMessage("Cannot parse the event or the the received event is empty.");
    }
    return _processEvent(event);
  }

  private XyzResponse _processEvent(Event event) throws Exception {
    if (event instanceof ModifySpaceEvent) {
      return processModifySpaceEvent((ModifySpaceEvent) event);
    }
    if (event instanceof ModifySubscriptionEvent) {
      return processModifySubscriptionEvent((ModifySubscriptionEvent) event);
    }
    if (event instanceof ModifyFeaturesEvent) {
      return processModifyFeaturesEvent((ModifyFeaturesEvent) event);
    }
    if (event instanceof DeleteFeaturesByTagEvent) {
      return processDeleteFeaturesByTagEvent((DeleteFeaturesByTagEvent) event);
    }
    if (event instanceof GetFeaturesByGeometryEvent) {
      return processGetFeaturesByGeometryEvent((GetFeaturesByGeometryEvent) event);
    }
    if (event instanceof GetFeaturesByTileEvent) {
      return processGetFeaturesByTileEvent((GetFeaturesByTileEvent) event);
    }
    if (event instanceof GetFeaturesByBBoxEvent) {
      return processGetFeaturesByBBoxEvent((GetFeaturesByBBoxEvent) event);
    }
    if (event instanceof IterateFeaturesEvent) {
      return processIterateFeaturesEvent((IterateFeaturesEvent) event);
    }
    if (event instanceof IterateHistoryEvent) {
      return processIterateHistoryEvent((IterateHistoryEvent) event);
    }
    if (event instanceof SearchForFeaturesEvent) {
      return processSearchForFeaturesEvent((SearchForFeaturesEvent) event);
    }
    if (event instanceof GetStatisticsEvent) {
      return processGetStatistics((GetStatisticsEvent) event);
    }
    if (event instanceof GetHistoryStatisticsEvent) {
      return processGetHistoryStatisticsEvent((GetHistoryStatisticsEvent) event);
    }
    if (event instanceof HealthCheckEvent) {
      return processHealthCheckEvent((HealthCheckEvent) event);
    }
    if (event instanceof GetFeaturesByIdEvent) {
      return processGetFeaturesByIdEvent((GetFeaturesByIdEvent) event);
    }
    if (event instanceof LoadFeaturesEvent) {
      return processLoadFeaturesEvent((LoadFeaturesEvent) event);
    }
    if (event instanceof GetStorageStatisticsEvent) {
      return processGetStorageStatisticsEvent((GetStorageStatisticsEvent) event);
    }

    return new ErrorResponse()
        .withStreamId(streamId)
        .withError(XyzError.NOT_IMPLEMENTED)
        .withErrorMessage("Unknown event type '" + event.getClass().getSimpleName() + "'");
  }

  /**
   * Processes a GetStatistics event.
   */
  @SuppressWarnings("WeakerAccess")
  protected abstract XyzResponse processGetStatistics(GetStatisticsEvent event) throws Exception;

  /**
   * Processes a GetStatistics event.
   */
  @SuppressWarnings("WeakerAccess")
  protected abstract XyzResponse processGetHistoryStatisticsEvent(GetHistoryStatisticsEvent event) throws Exception;

  /**
   * Processes a GetFeaturesById event.
   */
  @SuppressWarnings("WeakerAccess")
  protected abstract XyzResponse processGetFeaturesByIdEvent(GetFeaturesByIdEvent event) throws Exception;

  /**
   * Processes a GetFeaturesByGeometryEvent event.
   */
  @SuppressWarnings("WeakerAccess")
  protected abstract XyzResponse processGetFeaturesByGeometryEvent(GetFeaturesByGeometryEvent event) throws Exception;

  /**
   * Processes a GetFeaturesByBBox event.
   */
  @SuppressWarnings("WeakerAccess")
  protected abstract XyzResponse processGetFeaturesByBBoxEvent(GetFeaturesByBBoxEvent event) throws Exception;

  /**
   * Processes a GetFeaturesByTile event.
   */
  @SuppressWarnings("WeakerAccess")
  protected abstract XyzResponse processGetFeaturesByTileEvent(GetFeaturesByTileEvent event) throws Exception;

  /**
   * Processes a IterateFeatures event.
   */
  @SuppressWarnings("WeakerAccess")
  protected abstract XyzResponse processIterateFeaturesEvent(IterateFeaturesEvent event) throws Exception;

  /**
   * Processes a SearchForFeatures event.
   */
  @SuppressWarnings("WeakerAccess")
  protected abstract XyzResponse processSearchForFeaturesEvent(SearchForFeaturesEvent event) throws Exception;

  /**
   * Processes a DeleteFeaturesEvent event.
   */
  @SuppressWarnings("WeakerAccess")
  @Deprecated
  protected abstract XyzResponse processDeleteFeaturesByTagEvent(DeleteFeaturesByTagEvent event) throws Exception;

  /**
   * Processes a LoadFeaturesEvent event.
   */
  @SuppressWarnings("WeakerAccess")
  protected abstract XyzResponse processLoadFeaturesEvent(LoadFeaturesEvent event) throws Exception;

  /**
   * Processes a ModifyFeaturesEvent event.
   */
  @SuppressWarnings("WeakerAccess")
  protected abstract XyzResponse processModifyFeaturesEvent(ModifyFeaturesEvent event) throws Exception;

  /**
   * Processes a DeleteSpaceEvent event.
   */
  @SuppressWarnings("WeakerAccess")
  protected abstract XyzResponse processModifySpaceEvent(ModifySpaceEvent event) throws Exception;

  /**
   * Processes a ModifySubscriptionEvent event.
   */
  @SuppressWarnings("WeakerAccess")
  protected abstract XyzResponse processModifySubscriptionEvent(ModifySubscriptionEvent event) throws Exception;

  /**
   * Processes a IterateFeatures event.
   */
  @SuppressWarnings("WeakerAccess")
  protected abstract XyzResponse processIterateHistoryEvent(IterateHistoryEvent event) throws Exception;

  protected abstract XyzResponse processGetStorageStatisticsEvent(GetStorageStatisticsEvent event) throws Exception;
}
