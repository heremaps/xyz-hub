package com.here.xyz.hub.task.feature;

import com.here.xyz.events.feature.GetFeaturesByIdEvent;
import com.here.xyz.exceptions.ParameterError;
import com.here.xyz.hub.rest.ApiResponseType;
import io.vertx.ext.web.RoutingContext;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class GetFeaturesByIdTask extends AbstractFeatureTask<GetFeaturesByIdEvent> {

  public GetFeaturesByIdTask(@Nullable String streamId) {
    super(streamId);
  }

  @Override
  public @NotNull GetFeaturesByIdEvent createEvent() {
    return new GetFeaturesByIdEvent();
  }

  @Override
  public void initFromRoutingContext(@NotNull RoutingContext routingContext, @NotNull ApiResponseType responseType) throws ParameterError {
    super.initFromRoutingContext(routingContext, responseType);

    assert queryParameters != null;
    event.setForce2D(queryParameters.getForce2D());
    final List<@NotNull String> ids = queryParameters.getIds();
    if (ids == null || ids.size() == 0) {
      throw new ParameterError("Missing or empty 'id' parameter");
    }
    if (responseType == ApiResponseType.FEATURE) {
      if (ids.size() > 1) {
        throw new ParameterError("Too many ids given");
      }
    } else if (responseType != ApiResponseType.FEATURE_COLLECTION) {
      throw new ParameterError("Invalid content-type requested: " + responseType.contentType);
    }
    event.setIds(ids);
  }
}