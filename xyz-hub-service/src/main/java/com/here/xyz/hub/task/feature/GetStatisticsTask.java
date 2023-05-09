package com.here.xyz.hub.task.feature;

import com.here.xyz.events.info.GetStatisticsEvent;
import com.here.xyz.exceptions.ParameterError;
import com.here.xyz.hub.rest.ApiResponseType;
import io.vertx.ext.web.RoutingContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class GetStatisticsTask extends AbstractFeatureTask<GetStatisticsEvent> {

  public GetStatisticsTask(@Nullable String streamId) {
    super(streamId);
  }

  @Override
  public @NotNull GetStatisticsEvent createEvent() {
    return new GetStatisticsEvent();
  }

  @Override
  public void initEventFromRoutingContext(@NotNull RoutingContext routingContext, @NotNull ApiResponseType responseType) throws ParameterError {
    super.initEventFromRoutingContext(routingContext, responseType);
    // TODO: Do we need any additional arguments?
  }
}
