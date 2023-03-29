package com.here.xyz.hub.task.feature;

import com.here.xyz.events.info.GetStatisticsEvent;
import com.here.xyz.hub.auth.FeatureAuthorization;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.task.TaskPipeline;
import io.vertx.ext.web.RoutingContext;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;

public class GetStatistics extends FeatureTask<GetStatisticsEvent> {

  public GetStatistics(
      @NotNull GetStatisticsEvent event,
      @NotNull RoutingContext context,
      ApiResponseType apiResponseTypeType
  ) {
    super(event, context, apiResponseTypeType);
  }

  @Nonnull
  @Override
  public @NotNull TaskPipeline<GetStatisticsEvent> initPipeline() {
    return new TaskPipeline(routingContext, this)
        .then(FeatureTaskHandler::resolveSpace)
        .then(FeatureAuthorization::authorize)
        .then(FeatureTaskHandler::readCache)
        .then(FeatureTaskHandler::invoke)
        .then(FeatureTaskHandler::convertResponse)
        .then(FeatureTaskHandler::writeCache);
  }
}
