package com.here.xyz.hub.task.feature;

import com.here.xyz.events.admin.ModifySubscriptionEvent;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.task.TaskPipeline;
import io.vertx.ext.web.RoutingContext;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;

public class ModifySubscriptionQuery extends FeatureTask<ModifySubscriptionEvent> {

  public ModifySubscriptionQuery(
      @NotNull ModifySubscriptionEvent event,
      @NotNull RoutingContext context,
      ApiResponseType apiResponseTypeType
  ) {
    super(event, context, apiResponseTypeType);
  }

  @Nonnull
  @Override
  public @NotNull TaskPipeline<ModifySubscriptionEvent> initPipeline() {
    return new TaskPipeline(context, this)
        .then(FeatureTaskHandler::resolveSpace)
        .then(FeatureTaskHandler::invoke);
  }
}
