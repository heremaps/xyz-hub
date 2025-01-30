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

package com.here.xyz.connectors;

import static com.here.xyz.events.GetFeaturesByTileEvent.ResponseType.MVT;
import static com.here.xyz.events.GetFeaturesByTileEvent.ResponseType.MVT_FLATTENED;
import static com.here.xyz.responses.XyzError.EXCEPTION;
import static com.here.xyz.responses.XyzError.NOT_IMPLEMENTED;
import static java.lang.Boolean.TRUE;

import com.here.xyz.events.DeleteChangesetsEvent;
import com.here.xyz.events.Event;
import com.here.xyz.events.GetChangesetStatisticsEvent;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.GetStorageStatisticsEvent;
import com.here.xyz.events.HealthCheckEvent;
import com.here.xyz.events.IterateChangesetsEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.LoadFeaturesEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.ModifySubscriptionEvent;
import com.here.xyz.events.OneTimeActionEvent;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.events.WriteFeaturesEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.responses.BinaryResponse;
import com.here.xyz.responses.ChangesetsStatisticsResponse;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.StorageStatistics;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.responses.changesets.ChangesetCollection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class StorageConnector extends AbstractConnectorHandler {
  private static final Logger logger = LogManager.getLogger();

  @Override
  public XyzResponse processEvent(Event event) throws Exception {
    if (event == null) {
      return new ErrorResponse()
          .withStreamId(streamId)
          .withError(EXCEPTION)
          .withErrorMessage("Cannot parse the event or the the received event is empty.");
    }
    return _processEvent(event);
  }

  private XyzResponse _processEvent(Event event) throws Exception {
    logger.info("{} Received " + event.getClass().getSimpleName(), traceItem);
    try {
      if (event instanceof ModifySpaceEvent)
        return processModifySpaceEvent((ModifySpaceEvent) event);
      if (event instanceof ModifySubscriptionEvent)
        return processModifySubscriptionEvent((ModifySubscriptionEvent) event);
      if (event instanceof ModifyFeaturesEvent)
        return processModifyFeaturesEvent((ModifyFeaturesEvent) event);
      if (event instanceof WriteFeaturesEvent writeFeaturesEvent)
        return processWriteFeaturesEvent(writeFeaturesEvent);
      if (event instanceof GetFeaturesByGeometryEvent)
        return processGetFeaturesByGeometryEvent((GetFeaturesByGeometryEvent) event);
      if (event instanceof GetFeaturesByTileEvent tileEvent) {
        if (tileEvent.getResponseType() == MVT || tileEvent.getResponseType() == MVT_FLATTENED) {
          try {
            return processBinaryGetFeaturesByTileEvent(tileEvent);
          }
          catch (UnsupportedOperationException e) {
            //Fall back to GeoJSON, the service will perform the transformation into MVT instead
            return processGetFeaturesByTileEvent(tileEvent);
          }
        }
        return processGetFeaturesByTileEvent(tileEvent);
      }
      if (event instanceof GetFeaturesByBBoxEvent)
        return processGetFeaturesByBBoxEvent((GetFeaturesByBBoxEvent) event);
      if (event instanceof IterateFeaturesEvent)
        return processIterateFeaturesEvent((IterateFeaturesEvent) event);
      if (event instanceof IterateChangesetsEvent)
        return processIterateChangesetsEvent((IterateChangesetsEvent) event);
      if (event instanceof GetChangesetStatisticsEvent)
        return processGetChangesetsStatisticsEvent((GetChangesetStatisticsEvent) event);
      if (event instanceof SearchForFeaturesEvent)
        return processSearchForFeaturesEvent((SearchForFeaturesEvent) event);
      if (event instanceof GetStatisticsEvent)
        return processGetStatistics((GetStatisticsEvent) event);
      if (event instanceof HealthCheckEvent)
        return processHealthCheckEvent((HealthCheckEvent) event);
      if (event instanceof GetFeaturesByIdEvent)
        return processGetFeaturesByIdEvent((GetFeaturesByIdEvent) event);
      if (event instanceof LoadFeaturesEvent)
        return processLoadFeaturesEvent((LoadFeaturesEvent) event);
      if (event instanceof GetStorageStatisticsEvent)
        return processGetStorageStatisticsEvent((GetStorageStatisticsEvent) event);
      if (event instanceof DeleteChangesetsEvent)
        return processDeleteChangesetsEvent((DeleteChangesetsEvent) event);
      if (event instanceof OneTimeActionEvent)
        return processOneTimeActionEvent((OneTimeActionEvent) event);

      return new ErrorResponse()
          .withStreamId(streamId)
          .withError(NOT_IMPLEMENTED)
          .withErrorMessage("Unknown event type: " + event.getClass().getSimpleName());
    }
    catch (Exception e) {
      handleProcessingException(e, event);
      return new ErrorResponse()
          .withStreamId(streamId)
          .withError(EXCEPTION)
          .withErrorMessage("Unhandled exception: " + e.getMessage());
    }
    finally {
      logger.info("{} Finished " + event.getClass().getSimpleName(), traceItem);
    }
  }

  protected static boolean mvtSupported(Event event) {
    return event.getConnectorParams() != null && event.getConnectorParams().get("mvtSupport") == TRUE;
  }

  /**
   * Processes a GetStatistics event.
   */
  @SuppressWarnings("WeakerAccess")
  protected abstract StatisticsResponse processGetStatistics(GetStatisticsEvent event) throws Exception;

  /**
   * Processes a GetFeaturesById event.
   */
  @SuppressWarnings("WeakerAccess")
  protected abstract FeatureCollection processGetFeaturesByIdEvent(GetFeaturesByIdEvent event) throws Exception;

  /**
   * Processes a GetFeaturesByGeometryEvent event.
   */
  @SuppressWarnings("WeakerAccess")
  protected abstract FeatureCollection processGetFeaturesByGeometryEvent(GetFeaturesByGeometryEvent event) throws Exception;

  /**
   * Processes a GetFeaturesByBBox event.
   */
  @SuppressWarnings("WeakerAccess")
  protected abstract FeatureCollection processGetFeaturesByBBoxEvent(GetFeaturesByBBoxEvent event) throws Exception;

  /**
   * Processes a GetFeaturesByTile event.
   */
  @SuppressWarnings("WeakerAccess")
  protected abstract FeatureCollection processGetFeaturesByTileEvent(GetFeaturesByTileEvent event) throws Exception;

  /**
   * Processes a binary GetFeaturesByTile event.
   */
  protected BinaryResponse processBinaryGetFeaturesByTileEvent(GetFeaturesByTileEvent event) throws Exception {
    throw new UnsupportedOperationException(event.getClass().getSimpleName() + ": No binary support was implemented.");
  }

  /**
   * Processes a IterateFeatures event.
   */
  @SuppressWarnings("WeakerAccess")
  protected abstract FeatureCollection processIterateFeaturesEvent(IterateFeaturesEvent event) throws Exception;

  /**
   * Processes a SearchForFeatures event.
   */
  @SuppressWarnings("WeakerAccess")
  protected abstract FeatureCollection processSearchForFeaturesEvent(SearchForFeaturesEvent event) throws Exception;

  /**
   * Processes a LoadFeaturesEvent event.
   */
  @SuppressWarnings("WeakerAccess")
  protected abstract FeatureCollection processLoadFeaturesEvent(LoadFeaturesEvent event) throws Exception;

  /**
   * Processes a ModifyFeaturesEvent event.
   */
  @SuppressWarnings("WeakerAccess")
  protected abstract FeatureCollection processModifyFeaturesEvent(ModifyFeaturesEvent event) throws Exception;

  /**
   * Processes a {@link WriteFeaturesEvent} event.
   */
  @SuppressWarnings("WeakerAccess")
  protected abstract FeatureCollection processWriteFeaturesEvent(WriteFeaturesEvent event) throws Exception;

  /**
   * Processes a DeleteSpaceEvent event.
   */
  @SuppressWarnings("WeakerAccess")
  protected abstract SuccessResponse processModifySpaceEvent(ModifySpaceEvent event) throws Exception;

  /**
   * Processes a ModifySubscriptionEvent event.
   */
  @SuppressWarnings("WeakerAccess")
  protected abstract SuccessResponse processModifySubscriptionEvent(ModifySubscriptionEvent event) throws Exception;

  protected abstract StorageStatistics processGetStorageStatisticsEvent(GetStorageStatisticsEvent event) throws Exception;

  protected XyzResponse processOneTimeActionEvent(OneTimeActionEvent event) throws Exception {
    //Default implementation does nothing but may be overridden
    return new SuccessResponse();
  }

  protected abstract SuccessResponse processDeleteChangesetsEvent(DeleteChangesetsEvent event) throws Exception;

  protected abstract ChangesetCollection processIterateChangesetsEvent(IterateChangesetsEvent event) throws Exception;

  protected abstract ChangesetsStatisticsResponse processGetChangesetsStatisticsEvent(GetChangesetStatisticsEvent event) throws Exception;

  protected abstract void handleProcessingException(Exception exception, Event event) throws Exception;
}
