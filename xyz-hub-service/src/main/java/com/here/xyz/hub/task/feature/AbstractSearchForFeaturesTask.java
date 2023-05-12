package com.here.xyz.hub.task.feature;

import com.here.xyz.events.feature.SearchForFeaturesEvent;
import com.here.xyz.exceptions.ParameterError;
import com.here.xyz.hub.rest.ApiResponseType;
import io.vertx.ext.web.RoutingContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class AbstractSearchForFeaturesTask<EVENT extends SearchForFeaturesEvent> extends AbstractFeatureTask<EVENT> {

  protected AbstractSearchForFeaturesTask(@Nullable String streamId) {
    super(streamId);
  }

  @Override
  protected void initEventFromRoutingContext(@NotNull RoutingContext routingContext, @NotNull ApiResponseType responseType)
      throws ParameterError {
    super.initEventFromRoutingContext(routingContext, responseType);

    assert queryParameters != null;
    event.setLimit(queryParameters.getLimit());
    event.setTags(queryParameters.getTagsQuery());
    event.setPropertiesQuery(queryParameters.getPropertiesQuery());
    event.setForce2D(queryParameters.getForce2D());
    event.setSelection(queryParameters.getSelection());
  }
}