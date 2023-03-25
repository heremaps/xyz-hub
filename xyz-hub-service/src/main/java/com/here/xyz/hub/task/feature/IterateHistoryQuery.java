package com.here.xyz.hub.task.feature;

import com.here.xyz.events.feature.history.IterateHistoryEvent;
import com.here.xyz.hub.auth.FeatureAuthorization;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.task.TaskPipeline;
import io.vertx.ext.web.RoutingContext;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;

public class IterateHistoryQuery extends ReadQuery<IterateHistoryEvent> {

  public IterateHistoryQuery(
      @NotNull IterateHistoryEvent event,
      @NotNull RoutingContext context,
      ApiResponseType apiResponseTypeType
  ) {
    super(event, context, apiResponseTypeType);
  }

  @Nonnull
  @Override
  public @NotNull TaskPipeline<IterateHistoryEvent> initPipeline() {
    return new TaskPipeline(context, this)
        .then(FeatureTaskHandler::resolveSpace)
        .then(FeatureAuthorization::authorize)
        .then(FeatureTaskHandler::validate)
        .then(FeatureTaskHandler::readCache)
        .then(FeatureTaskHandler::invoke)
        .then(FeatureTaskHandler::writeCache);
  }
}
