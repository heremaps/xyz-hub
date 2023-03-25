package com.here.xyz.hub.task.feature;

import com.here.xyz.events.feature.GetFeaturesByBBoxEvent;
import com.here.xyz.hub.auth.FeatureAuthorization;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.task.TaskPipeline;
import io.vertx.ext.web.RoutingContext;
import org.jetbrains.annotations.NotNull;

public final class GetFeaturesByBBoxTask<EVENT extends GetFeaturesByBBoxEvent> extends ReadQuery<EVENT, GetFeaturesByBBoxTask<EVENT>> {

  public GetFeaturesByBBoxTask(@NotNull EVENT event, @NotNull RoutingContext context, ApiResponseType apiResponseTypeType) {
    super(event, context, apiResponseTypeType);
  }

  @Override
  protected void initPipeline(@NotNull TaskPipeline<EVENT, GetFeaturesByBBoxTask<EVENT>> pipeline) {
    pipeline
        .then(FeatureAuthorization::authorize)
        .then(FeatureTaskHandler::validate)
        .then(FeatureTaskHandler::readCache)
        .then(FeatureTaskHandler::invoke)
        .then(FeatureTaskHandler::writeCache);
  }
}
