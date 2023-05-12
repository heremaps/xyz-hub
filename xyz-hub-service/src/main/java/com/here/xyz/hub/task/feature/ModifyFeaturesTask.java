package com.here.xyz.hub.task.feature;

import com.here.xyz.events.feature.ModifyFeaturesEvent;
import com.here.xyz.exceptions.ParameterError;
import com.here.xyz.hub.auth.FeatureAuthorization;
import com.here.xyz.hub.rest.ApiResponseType;
import io.vertx.ext.web.RoutingContext;
import org.jetbrains.annotations.NotNull;

/**
 * Minimal version of Conditional Operation used on admin events endpoint. Contains some limitations because doesn't implement the full
 * pipeline. If you want to use the full conditional operation pipeline, you should request through Features API. Known limitations: -
 * Cannot perform validation of existing resources per operation type
 */
public class ModifyFeaturesTask extends AbstractFeatureTask<ModifyFeaturesEvent> {

  public ModifyFeaturesTask() {
    super(new ModifyFeaturesEvent());
  }

  @Override
  protected void initEventFromRoutingContext(@NotNull RoutingContext routingContext, @NotNull ApiResponseType responseType)
      throws ParameterError {
    super.initEventFromRoutingContext(routingContext, responseType);

    assert queryParameters != null;

    requestMatrix.createFeatures(event.getSpace());


    return new TaskPipeline(routingContext, this)
        .then(FeatureTaskHandler::resolveSpace)
        .then(FeatureTaskHandler::checkPreconditions)
        .then(FeatureTaskHandler::injectSpaceParams)
        .then(FeatureAuthorization::authorize)
        .then(FeatureTaskHandler::enforceUsageQuotas)
        .then(FeatureTaskHandler::invoke);
  }
}
