package com.here.xyz.hub.task.feature;

import com.here.xyz.events.info.GetHistoryStatisticsEvent;
import com.here.xyz.hub.auth.FeatureAuthorization;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.task.TaskPipeline;
import io.vertx.ext.web.RoutingContext;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;

public class GetHistoryStatistics extends FeatureTask<GetHistoryStatisticsEvent> {

  public GetHistoryStatistics(
      @NotNull GetHistoryStatisticsEvent event,
      @NotNull RoutingContext context,
      ApiResponseType apiResponseTypeType
  ) {
    super(event, context, apiResponseTypeType);
  }

  @Nonnull
  @Override
  public @NotNull TaskPipeline<GetHistoryStatisticsEvent> initPipeline() {
    return new TaskPipeline(routingContext, this)
        .then(FeatureTaskHandler::resolveSpace)
        .then(FeatureAuthorization::authorize)
        .then(FeatureTaskHandler::readCache)
        .then(FeatureTaskHandler::validate)
        .then(FeatureTaskHandler::invoke)
        .then(FeatureTaskHandler::writeCache);
  }
}
