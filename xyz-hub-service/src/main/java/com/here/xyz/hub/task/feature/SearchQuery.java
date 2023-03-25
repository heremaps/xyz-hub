package com.here.xyz.hub.task.feature;

import com.here.xyz.events.feature.SearchForFeaturesEvent;
import com.here.xyz.hub.auth.FeatureAuthorization;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.task.TaskPipeline;
import io.vertx.ext.web.RoutingContext;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;

public class SearchQuery<EVENT extends SearchForFeaturesEvent, TASK extends ReadQuery<EVENT, TASK>> extends ReadQuery<EVENT, TASK> {

  public SearchQuery(
      @NotNull EVENT event,
      @NotNull RoutingContext context,
      ApiResponseType apiResponseTypeType
  ) {
    super(event, context, apiResponseTypeType);
  }

  @Override
  public void initPipeline(@NotNull TaskPipeline<EVENT, TASK> pipeline) {
    pipeline
        .then(FeatureAuthorization::authorize)
        .then(FeatureTaskHandler::validate)
        .then(FeatureTaskHandler::readCache)
        .then(FeatureTaskHandler::invoke)
        .then(FeatureTaskHandler::writeCache);
  }
}
