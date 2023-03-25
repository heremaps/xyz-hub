package com.here.xyz.hub.task.feature;

import com.here.xyz.events.feature.IterateFeaturesEvent;
import com.here.xyz.hub.auth.FeatureAuthorization;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.task.TaskPipeline;
import io.vertx.ext.web.RoutingContext;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;

public class IterateQuery extends ReadQuery<IterateFeaturesEvent> {

  public IterateQuery(
      @NotNull IterateFeaturesEvent event,
      @NotNull RoutingContext context,
      ApiResponseType apiResponseTypeType
  ) {
    super(event, context, apiResponseTypeType);
  }

  @Nonnull
  @Override
  public @NotNull TaskPipeline<IterateFeaturesEvent> initPipeline() {
    return new TaskPipeline(context, this)
        .then(FeatureTaskHandler::resolveSpace)
        .then(FeatureAuthorization::authorize)
        .then(FeatureTaskHandler::validate)
        .then(FeatureTaskHandler::readCache)
        .then(FeatureTaskHandler::invoke)
        .then(FeatureTaskHandler::writeCache);
  }
}
