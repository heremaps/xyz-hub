package com.here.xyz.hub.task.feature;

import com.here.xyz.events.feature.GetFeaturesByBBoxEvent;
import com.here.xyz.exceptions.ParameterError;
import com.here.xyz.hub.rest.ApiParam.Query;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.models.geojson.coordinates.BBox;
import io.vertx.ext.web.RoutingContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class AbstractGetFeaturesByBBoxTask<EVENT extends GetFeaturesByBBoxEvent> extends AbstractSpatialQueryTask<EVENT> {

  protected AbstractGetFeaturesByBBoxTask(@Nullable String streamId) {
    super(streamId);
  }

  @Override
  public void initFromRoutingContext(@NotNull RoutingContext routingContext, @NotNull ApiResponseType responseType) throws ParameterError {
    super.initFromRoutingContext(routingContext, responseType);
    assert queryParameters != null;
    final BBox bBox = queryParameters.getBBox();
    if (bBox == null) {
      throw new ParameterError("Missing bounding box parameters (north, east, south and west)");
    }
    event.setBbox(bBox);
    event.setClustering(queryParameters.getClustering());
    event.setTweaks(queryParameters.getTweaks());
    event.setOptimizationMode(queryParameters.getOptimizationMode());
    event.setVizSampling(queryParameters.getOptimizationVizSampling());
  }
}
