package com.here.xyz;

import static com.here.xyz.EventTask.currentTask;

import com.here.xyz.events.feature.DeleteFeaturesByTagEvent;
import com.here.xyz.events.Event;
import com.here.xyz.events.feature.GetFeaturesByBBoxEvent;
import com.here.xyz.events.feature.GetFeaturesByGeometryEvent;
import com.here.xyz.events.feature.GetFeaturesByIdEvent;
import com.here.xyz.events.feature.GetFeaturesByTileEvent;
import com.here.xyz.events.info.GetHistoryStatisticsEvent;
import com.here.xyz.events.info.GetStatisticsEvent;
import com.here.xyz.events.info.GetStorageStatisticsEvent;
import com.here.xyz.events.info.HealthCheckEvent;
import com.here.xyz.events.feature.IterateFeaturesEvent;
import com.here.xyz.events.feature.history.IterateHistoryEvent;
import com.here.xyz.events.feature.LoadFeaturesEvent;
import com.here.xyz.events.feature.ModifyFeaturesEvent;
import com.here.xyz.events.space.ModifySpaceEvent;
import com.here.xyz.events.admin.ModifySubscriptionEvent;
import com.here.xyz.events.feature.SearchForFeaturesEvent;
import com.here.xyz.exceptions.XyzErrorException;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;

/**
 * An extended version of the standard {@link IEventHandler} interface that simplifies the implementation of processors. This comes with a
 * default implementation of the {@link #processEvent(IEventContext)} method and distributes to dedicated handlers, and it has has a basic
 * exception handling.
 */
public interface IExtendedEventHandler extends IEventHandler {

  /**
   * The method invoked by the XYZ-Hub directly (embedded) or indirectly, when running in an HTTP vertx or as AWS lambda.
   *
   * @param eventContext the event context to process.
   * @return the response to send.
   */
  default @NotNull XyzResponse processEvent(@NotNull IEventContext eventContext) {
    final Event event = eventContext.event();
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
      return e.toErrorResponse(event.getStreamId());
    } catch (Exception e) {
      currentTask().error("Uncaught exception in event processor", e);
      return new ErrorResponse()
          .withStreamId(event.getStreamId())
          .withError(XyzError.EXCEPTION)
          .withErrorMessage("Unexpected exception in storage connector: " + e.getMessage());
    }
  }

  /**
   * Called from {@link #processEvent(IEventContext)} before the real process method.
   *
   * @param ctx the context.
   */
  void initialize(@NotNull IEventContext ctx);

  /**
   * Processes a HealthCheckEvent event.
   *
   * <p>This type of events are sent in regular intervals to the storage connector and should be used to perform maintenance work. One
   * important task is for example to ensure that all partitions of tables with history exist.
   */
  @NotNull XyzResponse processHealthCheckEvent(@NotNull HealthCheckEvent event);

  /**
   * Processes a GetStatistics event.
   */
  @NotNull XyzResponse processGetStatistics(@NotNull GetStatisticsEvent event) throws Exception;

  /**
   * Processes a GetStatistics event.
   */
  @NotNull XyzResponse processGetHistoryStatisticsEvent(@NotNull GetHistoryStatisticsEvent event) throws Exception;

  /**
   * Processes a GetFeaturesById event.
   */
  @NotNull XyzResponse processGetFeaturesByIdEvent(@NotNull GetFeaturesByIdEvent event) throws Exception;

  /**
   * Processes a GetFeaturesByGeometryEvent event.
   */
  @NotNull XyzResponse processGetFeaturesByGeometryEvent(@NotNull GetFeaturesByGeometryEvent event) throws Exception;

  /**
   * Processes a GetFeaturesByBBox event.
   */
  @NotNull XyzResponse processGetFeaturesByBBoxEvent(@Nonnull GetFeaturesByBBoxEvent event) throws Exception;

  /**
   * Processes a GetFeaturesByTile event.
   */
  @NotNull XyzResponse processGetFeaturesByTileEvent(@NotNull GetFeaturesByTileEvent event) throws Exception;

  /**
   * Processes a IterateFeatures event.
   */
  @NotNull XyzResponse processIterateFeaturesEvent(@NotNull IterateFeaturesEvent event) throws Exception;

  /**
   * Processes a SearchForFeatures event.
   */
  @NotNull
  XyzResponse processSearchForFeaturesEvent(@NotNull SearchForFeaturesEvent event) throws Exception;

  /**
   * Processes a DeleteFeaturesEvent event.
   */
  @NotNull XyzResponse processDeleteFeaturesByTagEvent(@NotNull DeleteFeaturesByTagEvent event) throws Exception;

  /**
   * Processes a LoadFeaturesEvent event.
   */
  @NotNull XyzResponse processLoadFeaturesEvent(@NotNull LoadFeaturesEvent event) throws Exception;

  /**
   * Processes a ModifyFeaturesEvent event.
   */
  @NotNull XyzResponse processModifyFeaturesEvent(@NotNull ModifyFeaturesEvent event) throws Exception;

  /**
   * Processes a DeleteSpaceEvent event.
   */
  @NotNull XyzResponse processModifySpaceEvent(@NotNull ModifySpaceEvent event) throws Exception;

  /**
   * Processes a ModifySubscriptionEvent event.
   */
  @NotNull XyzResponse processModifySubscriptionEvent(@NotNull ModifySubscriptionEvent event) throws Exception;

  /**
   * Processes a IterateFeatures event.
   */
  @NotNull XyzResponse processIterateHistoryEvent(@NotNull IterateHistoryEvent event) throws Exception;

  @NotNull XyzResponse processGetStorageStatisticsEvent(@NotNull GetStorageStatisticsEvent event) throws Exception;

}
