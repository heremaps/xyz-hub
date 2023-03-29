package com.here.xyz.hub.task.feature;

import com.here.xyz.events.feature.LoadFeaturesEvent;
import com.here.xyz.hub.auth.FeatureAuthorization;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.task.ICallback;
import com.here.xyz.hub.task.TaskPipeline;
import io.vertx.ext.web.RoutingContext;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;

public class LoadFeaturesQuery extends FeatureTask<LoadFeaturesEvent> {

  public LoadFeaturesQuery(
      @NotNull LoadFeaturesEvent event,
      @NotNull RoutingContext context,
      ApiResponseType apiResponseTypeType) {
    super(event, context, apiResponseTypeType);
  }

  @Nonnull
  public @NotNull TaskPipeline<LoadFeaturesEvent> initPipeline() {
    return new TaskPipeline(routingContext, this)
        .then(FeatureTaskHandler::resolveSpace)
        .then(this::postResolveSpace)
        .then(FeatureAuthorization::authorize)
        .then(FeatureTaskHandler::readCache)
        .then(FeatureTaskHandler::invoke)
        .then(FeatureTaskHandler::convertResponse)
        .then(FeatureTaskHandler::writeCache);
  }

  private void postResolveSpace(
      com.here.xyz.hub.task.feature.LoadFeaturesQuery task, ICallback<com.here.xyz.hub.task.feature.LoadFeaturesQuery> callback) {
    task.getEvent().setEnableHistory(task.space.isEnableHistory());
    callback.success(task);
  }
}
