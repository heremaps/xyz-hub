package com.here.xyz.hub.task.feature;

import com.here.xyz.events.space.ModifySpaceEvent;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.task.TaskPipeline;
import io.vertx.ext.web.RoutingContext;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;

public class ModifySpaceQuery extends AbstractFeatureTask<ModifySpaceEvent> {

  public ModifySpaceQuery(
      @NotNull ModifySpaceEvent event,
      @NotNull RoutingContext context,
      ApiResponseType apiResponseTypeType
  ) {
    super(event, context, apiResponseTypeType);
  }

  @Nonnull
  @Override
  public @NotNull TaskPipeline<ModifySpaceEvent> initPipeline() {
    return new TaskPipeline(routingContext, this)
        .then(FeatureTaskHandler::resolveSpace)
        .then(FeatureTaskHandler::invoke);
  }
}
