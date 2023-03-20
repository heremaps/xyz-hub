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
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.responses.XyzResponse;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

/**
 * Default implementation of a processor that only needs to take care about those events it is interested in and optionally of the
 * post-processing.
 */
public class EventProcessor implements IExtendedEventHandler {

  @Override
  public @NotNull Logger logger() {
    return logger;
  }

  @Override
  public @NotNull XyzResponse<?> processEvent(@NotNull IEventContext ctx) {
    return postProcess(IExtendedEventHandler.super.processEvent(ctx));
  }

  @Override
  public void initialize(@NotNull IEventContext ctx) {
    this.ctx = ctx;
    this.logger = ctx.logger();
    this.event = ctx.event();
  }

  protected Event<?> event;
  protected Logger logger;
  protected IEventContext ctx;

  /**
   * Can be overridden to post-process any response.
   *
   * @param response the response.
   * @return the post-processed response.
   */
  protected @NotNull XyzResponse<?> postProcess(@NotNull XyzResponse<?> response) {
    return response;
  }

  /**
   * Send the currently processed event upstream towards the storage.
   *
   * @return the response returned by the storage and before post-processing.
   */
  protected @NotNull XyzResponse<?> sendUpstream() {
    return ctx.sendUpstream();
  }

  @Override
  public @NotNull XyzResponse<?> processHealthCheckEvent(@NotNull HealthCheckEvent event) {
    return sendUpstream();
  }

  @Override
  public @NotNull XyzResponse<?> processGetStatistics(@NotNull GetStatisticsEvent event) throws Exception {
    return sendUpstream();
  }

  @Override
  public @NotNull XyzResponse<?> processGetHistoryStatisticsEvent(@NotNull GetHistoryStatisticsEvent event) throws Exception {
    return sendUpstream();
  }

  @Override
  public @NotNull XyzResponse<?> processGetFeaturesByIdEvent(@NotNull GetFeaturesByIdEvent event) throws Exception {
    return sendUpstream();
  }

  @Override
  public @NotNull XyzResponse<?> processGetFeaturesByGeometryEvent(@NotNull GetFeaturesByGeometryEvent event) throws Exception {
    return sendUpstream();
  }

  @Override
  public @NotNull XyzResponse<?> processGetFeaturesByBBoxEvent(@Nonnull GetFeaturesByBBoxEvent<?> event) throws Exception {
    return sendUpstream();
  }

  @Override
  public @NotNull XyzResponse<?> processGetFeaturesByTileEvent(@NotNull GetFeaturesByTileEvent event) throws Exception {
    return sendUpstream();
  }

  @Override
  public @NotNull XyzResponse<?> processIterateFeaturesEvent(@NotNull IterateFeaturesEvent event) throws Exception {
    return sendUpstream();
  }

  @Override
  public @NotNull XyzResponse<?> processSearchForFeaturesEvent(@NotNull SearchForFeaturesEvent<?> event) throws Exception {
    return sendUpstream();
  }

  @Override
  public @NotNull XyzResponse<?> processDeleteFeaturesByTagEvent(@NotNull DeleteFeaturesByTagEvent event) throws Exception {
    return sendUpstream();
  }

  @Override
  public @NotNull XyzResponse<?> processLoadFeaturesEvent(@NotNull LoadFeaturesEvent event) throws Exception {
    return sendUpstream();
  }

  @Override
  public @NotNull XyzResponse<?> processModifyFeaturesEvent(@NotNull ModifyFeaturesEvent event) throws Exception {
    return sendUpstream();
  }

  @Override
  public @NotNull XyzResponse<?> processModifySpaceEvent(@NotNull ModifySpaceEvent event) throws Exception {
    return sendUpstream();
  }

  @Override
  public @NotNull XyzResponse<?> processModifySubscriptionEvent(@NotNull ModifySubscriptionEvent event) throws Exception {
    return sendUpstream();
  }

  @Override
  public @NotNull XyzResponse<?> processIterateHistoryEvent(@NotNull IterateHistoryEvent event) throws Exception {
    return sendUpstream();
  }

  @Override
  public @NotNull XyzResponse<?> processGetStorageStatisticsEvent(@NotNull GetStorageStatisticsEvent event) throws Exception {
    return sendUpstream();
  }
}
