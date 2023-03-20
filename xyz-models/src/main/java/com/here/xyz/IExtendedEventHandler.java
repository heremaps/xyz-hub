package com.here.xyz;

import com.here.xyz.events.DeleteFeaturesByTagEvent;
import com.here.xyz.events.Event;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.GetHistoryStatisticsEvent;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.GetStorageStatisticsEvent;
import com.here.xyz.events.HealthCheckEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.IterateHistoryEvent;
import com.here.xyz.events.LoadFeaturesEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.ModifySubscriptionEvent;
import com.here.xyz.events.RevisionEvent;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

/**
 * An extended version of the standard {@link IEventHandler} interface that is specifically for the event processors. This comes with a
 * default implementation of the {@link #processEvent(IEventContext)} method and distributes to dedicated handlers, and has a basic
 * exception handling.
 */
public interface IExtendedEventHandler extends IEventHandler {

  /**
   * Returns the logger of the implementation.
   *
   * @return The logger of the implementation.
   */
  @NotNull Logger logger();

  /**
   * The method invoked by the XYZ-Hub directly (embedded) or indirectly, when running in an HTTP vertx or as AWS lambda.
   *
   * @param ctx the event context to process.
   * @return the response to send.
   */
  default @NotNull XyzResponse<?> processEvent(@NotNull IEventContext ctx) {
    final Event<?> event = ctx.event();
    try {
      if (event instanceof ModifySpaceEvent) {
        initialize(ctx);
        return processModifySpaceEvent((ModifySpaceEvent) event);
      }
      if (event instanceof ModifySubscriptionEvent) {
        initialize(ctx);
        return processModifySubscriptionEvent((ModifySubscriptionEvent) event);
      }
      if (event instanceof ModifyFeaturesEvent) {
        initialize(ctx);
        return processModifyFeaturesEvent((ModifyFeaturesEvent) event);
      }
      if (event instanceof DeleteFeaturesByTagEvent) {
        initialize(ctx);
        return processDeleteFeaturesByTagEvent((DeleteFeaturesByTagEvent) event);
      }
      if (event instanceof GetFeaturesByGeometryEvent) {
        initialize(ctx);
        return processGetFeaturesByGeometryEvent((GetFeaturesByGeometryEvent) event);
      }
      if (event instanceof GetFeaturesByTileEvent) {
        initialize(ctx);
        return processGetFeaturesByTileEvent((GetFeaturesByTileEvent) event);
      }
      if (event instanceof GetFeaturesByBBoxEvent) {
        initialize(ctx);
        return processGetFeaturesByBBoxEvent((GetFeaturesByBBoxEvent<?>) event);
      }
      if (event instanceof IterateFeaturesEvent) {
        initialize(ctx);
        return processIterateFeaturesEvent((IterateFeaturesEvent) event);
      }
      if (event instanceof IterateHistoryEvent) {
        initialize(ctx);
        return processIterateHistoryEvent((IterateHistoryEvent) event);
      }
      if (event instanceof SearchForFeaturesEvent) {
        initialize(ctx);
        return processSearchForFeaturesEvent((SearchForFeaturesEvent<?>) event);
      }
      if (event instanceof GetStatisticsEvent) {
        initialize(ctx);
        return processGetStatistics((GetStatisticsEvent) event);
      }
      if (event instanceof GetHistoryStatisticsEvent) {
        initialize(ctx);
        return processGetHistoryStatisticsEvent((GetHistoryStatisticsEvent) event);
      }
      if (event instanceof HealthCheckEvent) {
        initialize(ctx);
        return processHealthCheckEvent((HealthCheckEvent) event);
      }
      if (event instanceof GetFeaturesByIdEvent) {
        initialize(ctx);
        return processGetFeaturesByIdEvent((GetFeaturesByIdEvent) event);
      }
      if (event instanceof LoadFeaturesEvent) {
        initialize(ctx);
        return processLoadFeaturesEvent((LoadFeaturesEvent) event);
      }
      if (event instanceof GetStorageStatisticsEvent) {
        initialize(ctx);
        return processGetStorageStatisticsEvent((GetStorageStatisticsEvent) event);
      }
      if (event instanceof RevisionEvent) {
        initialize(ctx);
        return new SuccessResponse();
      }
      return new ErrorResponse()
          .withStreamId(event.getStreamId())
          .withError(XyzError.NOT_IMPLEMENTED)
          .withErrorMessage("Unknown event type '" + event.getClass().getSimpleName() + "'");
    } catch (Exception e) {
      ctx.logger().error("{}:{}:{} - Uncaught exception in event processor", event.logId(), event.logStream(), event.logTime(), e);
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
  @NotNull XyzResponse<?> processHealthCheckEvent(@NotNull HealthCheckEvent event);

  /**
   * Processes a GetStatistics event.
   */
  @NotNull XyzResponse<?> processGetStatistics(@NotNull GetStatisticsEvent event) throws Exception;

  /**
   * Processes a GetStatistics event.
   */
  @NotNull XyzResponse<?> processGetHistoryStatisticsEvent(@NotNull GetHistoryStatisticsEvent event) throws Exception;

  /**
   * Processes a GetFeaturesById event.
   */
  @NotNull XyzResponse<?> processGetFeaturesByIdEvent(@NotNull GetFeaturesByIdEvent event) throws Exception;

  /**
   * Processes a GetFeaturesByGeometryEvent event.
   */
  @NotNull XyzResponse<?> processGetFeaturesByGeometryEvent(@NotNull GetFeaturesByGeometryEvent event) throws Exception;

  /**
   * Processes a GetFeaturesByBBox event.
   */
  @NotNull XyzResponse<?> processGetFeaturesByBBoxEvent(@Nonnull GetFeaturesByBBoxEvent<?> event) throws Exception;

  /**
   * Processes a GetFeaturesByTile event.
   */
  @NotNull XyzResponse<?> processGetFeaturesByTileEvent(@NotNull GetFeaturesByTileEvent event) throws Exception;

  /**
   * Processes a IterateFeatures event.
   */
  @NotNull XyzResponse<?> processIterateFeaturesEvent(@NotNull IterateFeaturesEvent event) throws Exception;

  /**
   * Processes a SearchForFeatures event.
   */
  @NotNull
  XyzResponse<?> processSearchForFeaturesEvent(@NotNull SearchForFeaturesEvent<?> event) throws Exception;

  /**
   * Processes a DeleteFeaturesEvent event.
   */
  @NotNull XyzResponse<?> processDeleteFeaturesByTagEvent(@NotNull DeleteFeaturesByTagEvent event) throws Exception;

  /**
   * Processes a LoadFeaturesEvent event.
   */
  @NotNull XyzResponse<?> processLoadFeaturesEvent(@NotNull LoadFeaturesEvent event) throws Exception;

  /**
   * Processes a ModifyFeaturesEvent event.
   */
  @NotNull XyzResponse<?> processModifyFeaturesEvent(@NotNull ModifyFeaturesEvent event) throws Exception;

  /**
   * Processes a DeleteSpaceEvent event.
   */
  @NotNull XyzResponse<?> processModifySpaceEvent(@NotNull ModifySpaceEvent event) throws Exception;

  /**
   * Processes a ModifySubscriptionEvent event.
   */
  @NotNull XyzResponse<?> processModifySubscriptionEvent(@NotNull ModifySubscriptionEvent event) throws Exception;

  /**
   * Processes a IterateFeatures event.
   */
  @NotNull XyzResponse<?> processIterateHistoryEvent(@NotNull IterateHistoryEvent event) throws Exception;

  @NotNull XyzResponse<?> processGetStorageStatisticsEvent(@NotNull GetStorageStatisticsEvent event) throws Exception;

}
