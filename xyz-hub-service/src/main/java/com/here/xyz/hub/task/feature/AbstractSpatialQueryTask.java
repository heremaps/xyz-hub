package com.here.xyz.hub.task.feature;

import com.here.xyz.events.feature.SpatialQueryEvent;
import com.here.xyz.exceptions.ParameterError;
import com.here.xyz.hub.rest.ApiResponseType;
import io.vertx.ext.web.RoutingContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class AbstractSpatialQueryTask<EVENT extends SpatialQueryEvent> extends AbstractSearchForFeaturesTask<EVENT> {

  protected AbstractSpatialQueryTask(@Nullable String streamId) {
    super(streamId);
  }

  @Override
  protected void initEventFromRoutingContext(@NotNull RoutingContext routingContext, @NotNull ApiResponseType responseType)
      throws ParameterError {
    super.initEventFromRoutingContext(routingContext, responseType);
    assert queryParameters != null;
    event.setClip(queryParameters.getClip());
  }
}
