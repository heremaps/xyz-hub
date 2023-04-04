package com.here.xyz.hub.task.feature;

import static com.here.xyz.hub.rest.ApiParam.Query.FORCE_2D;

import com.here.xyz.events.feature.GetFeaturesByIdEvent;
import com.here.xyz.exceptions.XyzErrorException;
import com.here.xyz.hub.auth.XyzHubActionMatrix;
import com.here.xyz.hub.rest.ApiParam.Query;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.params.XyzHubQueryParameters;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import io.vertx.ext.web.RoutingContext;
import java.util.Collections;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public class GetFeaturesByIdTask extends FeatureTask {

  public GetFeaturesByIdTask(@NotNull RoutingContext routingContext, @NotNull ApiResponseType apiResponseTypeType)
      throws XyzErrorException {
    super(routingContext, apiResponseTypeType);
  }

  @Override
  protected @NotNull XyzResponse execute() throws XyzErrorException {
    final XyzHubQueryParameters queryParams = queryParams();
    final @NotNull List<@NotNull String> ids;
    if (responseType == ApiResponseType.FEATURE_COLLECTION) {
      final List<@NotNull String> all = queryParams.getAll(Query.FEATURE_ID);
      if (all == null) {
        throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, "Missing 'id' query parameter");
      }
      ids = all;
    } else if (responseType == ApiResponseType.FEATURE) {
      final String id = queryParams.get(Query.FEATURE_ID);
      if (id == null) {
        throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, "Missing 'id' query parameter");
      }
      ids = Collections.singletonList(id);
    } else {
      throw new XyzErrorException(XyzError.EXCEPTION, "Internal error, wrong response type.");
    }
    final boolean force2D = queryParams.getBoolean(FORCE_2D, false);

    final XyzHubActionMatrix requestMatrix = new XyzHubActionMatrix();
    requestMatrix.readFeatures(space);

    final GetFeaturesByIdEvent event = new GetFeaturesByIdEvent();
    event.setForce2D(force2D);
    event.setIds(ids);
    initEvent(event, space);

    addSpaceHandler(space);

    return sendEvent(event, requestMatrix);
  }
}
