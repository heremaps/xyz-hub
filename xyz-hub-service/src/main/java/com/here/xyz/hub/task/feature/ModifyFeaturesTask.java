package com.here.xyz.hub.task.feature;

import com.here.xyz.events.feature.ModifyFeaturesEvent;
import com.here.xyz.hub.auth.FeatureAuthorization;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.task.TaskPipeline;
import io.vertx.ext.web.RoutingContext;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;

/**
 * Minimal version of Conditional Operation used on admin events endpoint. Contains some limitations because doesn't implement the full
 * pipeline. If you want to use the full conditional operation pipeline, you should request through Features API. Known limitations: -
 * Cannot perform validation of existing resources per operation type
 */
public class ModifyFeaturesTask extends FeatureTask<ModifyFeaturesEvent> {

  public ModifyFeaturesTask(
      @NotNull ModifyFeaturesEvent event,
      @NotNull RoutingContext context,
      ApiResponseType responseType
  ) {
    super(event, context, responseType);
  }

  @Nonnull
  @Override
  public @NotNull TaskPipeline<ModifyFeaturesEvent> initPipeline() {
    return new TaskPipeline(context, this)
        .then(FeatureTaskHandler::resolveSpace)
        .then(FeatureTaskHandler::checkPreconditions)
        .then(FeatureTaskHandler::injectSpaceParams)
        .then(FeatureAuthorization::authorize)
        .then(FeatureTaskHandler::enforceUsageQuotas)
        .then(FeatureTaskHandler::invoke);
  }
}
