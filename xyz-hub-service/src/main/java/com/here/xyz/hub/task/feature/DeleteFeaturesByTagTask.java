package com.here.xyz.hub.task.feature;

import com.here.xyz.events.feature.DeleteFeaturesByTagEvent;
import com.here.xyz.hub.auth.FeatureAuthorization;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.task.TaskPipeline;
import io.vertx.ext.web.RoutingContext;
import org.jetbrains.annotations.NotNull;

public class DeleteFeaturesByTagTask extends AbstractFeatureTask<DeleteFeaturesByTagEvent, DeleteFeaturesByTagTask> {

  public DeleteFeaturesByTagTask(
      @NotNull DeleteFeaturesByTagEvent event,
      @NotNull RoutingContext context,
      ApiResponseType apiResponseTypeType
  ) {
    super(event, context, apiResponseTypeType);
  }

  @Override
  protected void initPipeline(@NotNull TaskPipeline<DeleteFeaturesByTagEvent, DeleteFeaturesByTagTask> pipeline) {
    pipeline
        .then(FeatureTaskHandler::resolveSpace)
        .then(FeatureTaskHandler::checkPreconditions)
        .then(FeatureAuthorization::authorize)
        .then(FeatureTaskHandler::invoke);
  }
}
