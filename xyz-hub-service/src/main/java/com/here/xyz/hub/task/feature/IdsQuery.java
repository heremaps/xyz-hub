package com.here.xyz.hub.task.feature;

import com.here.xyz.events.feature.GetFeaturesByIdEvent;
import com.here.xyz.hub.auth.FeatureAuthorization;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.task.TaskPipeline;
import io.vertx.ext.web.RoutingContext;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;

public class IdsQuery extends FeatureTask<GetFeaturesByIdEvent> {

  public IdsQuery(@NotNull GetFeaturesByIdEvent event, @NotNull RoutingContext context, ApiResponseType apiResponseTypeType) {
    super(event, context, apiResponseTypeType);
  }

  @Nonnull
  public @NotNull TaskPipeline<GetFeaturesByIdEvent> initPipeline() {
    return new TaskPipeline(context, this)
        .then(FeatureTaskHandler::validateReadFeaturesParams)
        .then(FeatureTaskHandler::resolveSpace)
        .then(FeatureAuthorization::authorize)
        .then(FeatureTaskHandler::readCache)
        .then(FeatureTaskHandler::invoke)
        .then(FeatureTaskHandler::convertResponse)
        .then(FeatureTaskHandler::writeCache);
  }
}
