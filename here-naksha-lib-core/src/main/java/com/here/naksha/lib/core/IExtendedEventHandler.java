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
package com.here.naksha.lib.core;

import static com.here.naksha.lib.core.NakshaLogger.currentLogger;

import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.events.admin.ModifySubscriptionEvent;
import com.here.naksha.lib.core.models.payload.events.feature.DeleteFeaturesByTagEvent;
import com.here.naksha.lib.core.models.payload.events.feature.GetFeaturesByBBoxEvent;
import com.here.naksha.lib.core.models.payload.events.feature.GetFeaturesByGeometryEvent;
import com.here.naksha.lib.core.models.payload.events.feature.GetFeaturesByIdEvent;
import com.here.naksha.lib.core.models.payload.events.feature.GetFeaturesByTileEvent;
import com.here.naksha.lib.core.models.payload.events.feature.IterateFeaturesEvent;
import com.here.naksha.lib.core.models.payload.events.feature.LoadFeaturesEvent;
import com.here.naksha.lib.core.models.payload.events.feature.ModifyFeaturesEvent;
import com.here.naksha.lib.core.models.payload.events.feature.SearchForFeaturesEvent;
import com.here.naksha.lib.core.models.payload.events.feature.history.IterateHistoryEvent;
import com.here.naksha.lib.core.models.payload.events.info.GetHistoryStatisticsEvent;
import com.here.naksha.lib.core.models.payload.events.info.GetStatisticsEvent;
import com.here.naksha.lib.core.models.payload.events.info.GetStorageStatisticsEvent;
import com.here.naksha.lib.core.models.payload.events.info.HealthCheckEvent;
import com.here.naksha.lib.core.models.payload.events.space.ModifySpaceEvent;
import com.here.naksha.lib.core.models.payload.responses.ErrorResponse;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;

/**
 * An extended version of the standard {@link IEventHandler} interface that simplifies the
 * implementation of processors. This comes with a default implementation of the {@link
 * #processEvent(IEvent)} method and distributes to dedicated handlers, and it has has a
 * basic exception handling.
 */
@Deprecated
public interface IExtendedEventHandler {

  /**
   * The method invoked by the XYZ-Hub directly (embedded) or indirectly, when running in an HTTP vertx or as AWS lambda.
   *
   * @param eventContext the event context to process.
   * @return the response to send.
   */
  @Deprecated
  default @NotNull XyzResponse processEvent(@NotNull IEvent eventContext) {
    final Event event = null;
    try {
      if (event instanceof ModifySpaceEvent) {
        initialize(eventContext);
        return processModifySpaceEvent((ModifySpaceEvent) event);
      }
      if (event instanceof ModifySubscriptionEvent) {
        initialize(eventContext);
        return processModifySubscriptionEvent((ModifySubscriptionEvent) event);
      }
      if (event instanceof ModifyFeaturesEvent) {
        initialize(eventContext);
        return processModifyFeaturesEvent((ModifyFeaturesEvent) event);
      }
      if (event instanceof DeleteFeaturesByTagEvent) {
        initialize(eventContext);
        return processDeleteFeaturesByTagEvent((DeleteFeaturesByTagEvent) event);
      }
      if (event instanceof GetFeaturesByGeometryEvent) {
        initialize(eventContext);
        return processGetFeaturesByGeometryEvent((GetFeaturesByGeometryEvent) event);
      }
      if (event instanceof GetFeaturesByTileEvent) {
        initialize(eventContext);
        return processGetFeaturesByTileEvent((GetFeaturesByTileEvent) event);
      }
      if (event instanceof GetFeaturesByBBoxEvent) {
        initialize(eventContext);
        return processGetFeaturesByBBoxEvent((GetFeaturesByBBoxEvent) event);
      }
      if (event instanceof IterateFeaturesEvent) {
        initialize(eventContext);
        return processIterateFeaturesEvent((IterateFeaturesEvent) event);
      }
      if (event instanceof IterateHistoryEvent) {
        initialize(eventContext);
        return processIterateHistoryEvent((IterateHistoryEvent) event);
      }
      if (event instanceof SearchForFeaturesEvent) {
        initialize(eventContext);
        return processSearchForFeaturesEvent((SearchForFeaturesEvent) event);
      }
      if (event instanceof GetStatisticsEvent) {
        initialize(eventContext);
        return processGetStatistics((GetStatisticsEvent) event);
      }
      if (event instanceof GetHistoryStatisticsEvent) {
        initialize(eventContext);
        return processGetHistoryStatisticsEvent((GetHistoryStatisticsEvent) event);
      }
      if (event instanceof HealthCheckEvent) {
        initialize(eventContext);
        return processHealthCheckEvent((HealthCheckEvent) event);
      }
      if (event instanceof GetFeaturesByIdEvent) {
        initialize(eventContext);
        return processGetFeaturesByIdEvent((GetFeaturesByIdEvent) event);
      }
      if (event instanceof LoadFeaturesEvent) {
        initialize(eventContext);
        return processLoadFeaturesEvent((LoadFeaturesEvent) event);
      }
      if (event instanceof GetStorageStatisticsEvent) {
        initialize(eventContext);
        return processGetStorageStatisticsEvent((GetStorageStatisticsEvent) event);
      }
      return new ErrorResponse()
          .withStreamId(event.getStreamId())
          .withError(XyzError.NOT_IMPLEMENTED)
          .withErrorMessage("Unknown event type '" + event.getClass().getSimpleName() + "'");
    } catch (XyzErrorException e) {
      return new ErrorResponse(e, event.getStreamId());
    } catch (Exception e) {
      currentLogger()
          .atError("Uncaught exception in event processor")
          .setCause(e)
          .log();
      return new ErrorResponse()
          .withStreamId(event.getStreamId())
          .withError(XyzError.EXCEPTION)
          .withErrorMessage("Unexpected exception in storage connector: " + e.getMessage());
    }
  }

  /**
   * Called from {@link #processEvent(IEvent)} before the real process method.
   *
   * @param ctx the context.
   */
  @Deprecated
  void initialize(@NotNull IEvent ctx);

  /**
   * Processes a HealthCheckEvent event.
   *
   * <p>This type of events are sent in regular intervals to the storage connector and should be
   * used to perform maintenance work. One important task is for example to ensure that all
   * partitions of tables with history exist.
   */
  @NotNull
  @Deprecated
  XyzResponse processHealthCheckEvent(@NotNull HealthCheckEvent event);

  /** Processes a GetStatistics event. */
  @NotNull
  @Deprecated
  XyzResponse processGetStatistics(@NotNull GetStatisticsEvent event) throws Exception;

  /** Processes a GetStatistics event. */
  @NotNull
  @Deprecated
  XyzResponse processGetHistoryStatisticsEvent(@NotNull GetHistoryStatisticsEvent event) throws Exception;

  /** Processes a GetFeaturesById event. */
  @NotNull
  @Deprecated
  XyzResponse processGetFeaturesByIdEvent(@NotNull GetFeaturesByIdEvent event) throws Exception;

  /** Processes a GetFeaturesByGeometryEvent event. */
  @NotNull
  @Deprecated
  XyzResponse processGetFeaturesByGeometryEvent(@NotNull GetFeaturesByGeometryEvent event) throws Exception;

  /** Processes a GetFeaturesByBBox event. */
  @NotNull
  @Deprecated
  XyzResponse processGetFeaturesByBBoxEvent(@Nonnull GetFeaturesByBBoxEvent event) throws Exception;

  /** Processes a GetFeaturesByTile event. */
  @NotNull
  @Deprecated
  XyzResponse processGetFeaturesByTileEvent(@NotNull GetFeaturesByTileEvent event) throws Exception;

  /** Processes a IterateFeatures event. */
  @NotNull
  @Deprecated
  XyzResponse processIterateFeaturesEvent(@NotNull IterateFeaturesEvent event) throws Exception;

  /** Processes a SearchForFeatures event. */
  @NotNull
  @Deprecated
  XyzResponse processSearchForFeaturesEvent(@NotNull SearchForFeaturesEvent event) throws Exception;

  /** Processes a DeleteFeaturesEvent event. */
  @NotNull
  @Deprecated
  XyzResponse processDeleteFeaturesByTagEvent(@NotNull DeleteFeaturesByTagEvent event) throws Exception;

  /** Processes a LoadFeaturesEvent event. */
  @NotNull
  @Deprecated
  XyzResponse processLoadFeaturesEvent(@NotNull LoadFeaturesEvent event) throws Exception;

  /** Processes a ModifyFeaturesEvent event. */
  @NotNull
  @Deprecated
  XyzResponse processModifyFeaturesEvent(@NotNull ModifyFeaturesEvent event) throws Exception;

  /** Processes a DeleteSpaceEvent event. */
  @NotNull
  @Deprecated
  XyzResponse processModifySpaceEvent(@NotNull ModifySpaceEvent event) throws Exception;

  /** Processes a ModifySubscriptionEvent event. */
  @NotNull
  @Deprecated
  XyzResponse processModifySubscriptionEvent(@NotNull ModifySubscriptionEvent event) throws Exception;

  /** Processes a IterateFeatures event. */
  @NotNull
  @Deprecated
  XyzResponse processIterateHistoryEvent(@NotNull IterateHistoryEvent event) throws Exception;

  @NotNull
  @Deprecated
  XyzResponse processGetStorageStatisticsEvent(@NotNull GetStorageStatisticsEvent event) throws Exception;
}
