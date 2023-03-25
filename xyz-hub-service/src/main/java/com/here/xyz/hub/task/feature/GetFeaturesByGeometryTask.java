package com.here.xyz.hub.task.feature;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

import com.here.mapcreator.ext.naksha.Naksha;
import com.here.xyz.events.feature.GetFeaturesByGeometryEvent;
import com.here.xyz.events.feature.LoadFeaturesEvent;
import com.here.xyz.hub.auth.FeatureAuthorization;
import com.here.xyz.hub.rest.Api;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.task.ICallback;
import com.here.xyz.hub.task.TaskPipeline;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Connector;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Space.ConnectorRef;
import com.here.xyz.responses.XyzResponse;
import io.vertx.core.AsyncResult;
import io.vertx.ext.web.RoutingContext;
import java.util.HashMap;
import java.util.List;
import org.jetbrains.annotations.NotNull;

/**
 * Search using the geometry of a reference feature from a reference space. So basically, search all features in the current space, in range
 * of the given reference feature in the given reference space.
 */
public final class GetFeaturesByGeometryTask extends ReadQuery<GetFeaturesByGeometryEvent, GetFeaturesByGeometryTask> {

  public final @NotNull String refSpaceId;
  final @NotNull String refFeatureId;
  public final @NotNull Space refSpace;
  final @NotNull Connector refConnector;

  public GetFeaturesByGeometryTask(
      @NotNull GetFeaturesByGeometryEvent event,
      @NotNull RoutingContext context,
      ApiResponseType apiResponseTypeType,
      @NotNull String refSpaceId,
      @NotNull String refFeatureId
  ) {
    super(event, context, apiResponseTypeType);
    this.refFeatureId = refFeatureId;

    final Space space = Naksha.spaces.get(refSpaceId);
    if (space == null) {
      throw new IllegalStateException("Unknown reference space " + refSpaceId);
    }
    this.refSpaceId = refSpaceId;
    refSpace = space;

    final ConnectorRef connectorRef = refSpace.getConnectorId();
    final String connectorId = connectorRef.getId();
    final Connector connector = Naksha.connectors.get(connectorId);
    if (connector == null) {
      throw new IllegalStateException("Unknown reference storage " + connectorId + " configured for reference space " + refSpaceId);
    }
    this.refConnector = connector;
  }

  @Override
  protected void initPipeline(@NotNull TaskPipeline<GetFeaturesByGeometryEvent, GetFeaturesByGeometryTask> pipeline) {
    pipeline
        .then(FeatureAuthorization::authorize)
        .then(this::loadObject)
        .then(this::verifyResourceExists)
        .then(FeatureTaskHandler::validate)
        .then(FeatureTaskHandler::readCache)
        .then(FeatureTaskHandler::invoke)
        .then(FeatureTaskHandler::writeCache);
  }

  private void verifyResourceExists(@NotNull GetFeaturesByGeometryTask task, @NotNull ICallback callback) {
    if (this.getEvent().getGeometry() == null && this.getEvent().getH3Index() == null) {
      callback.throwException(new HttpException(NOT_FOUND, "The 'refFeatureId' : '" + refFeatureId + "' does not exist."));
    } else {
      callback.success();
    }
  }

  private void loadObject(@NotNull GetFeaturesByGeometryTask task, @NotNull ICallback callback) {
    if (task.getEvent().getGeometry() != null || task.getEvent().getH3Index() != null) {
      callback.success();
      return;
    }
    final LoadFeaturesEvent event = new LoadFeaturesEvent();
    event.setStreamId(task.logStream());
    event.setSpace(refSpaceId);
    event.setParams(this.refSpace.getConnectorId().getParams());
    final HashMap<String, String> idsMaps = new HashMap<>();
    idsMaps.put(refFeatureId, null);
    event.setIdsMap(idsMaps);
    try {
      getRpcClient(refConnector).execute(getMarker(), event, r -> processLoadEvent(callback, event, r));
    } catch (Exception e) {
      logger.warn(task.getMarker(), "Error trying to process LoadFeaturesEvent.", e);
      callback.throwException(e);
    }
  }

  void processLoadEvent(ICallback<GetFeaturesByGeometryTask> callback, LoadFeaturesEvent event,
      AsyncResult<XyzResponse> r) {
    if (r.failed()) {
      if (r.cause() instanceof Exception) {
        callback.throwException((Exception) r.cause());
      } else {
        callback.throwException(new Exception(r.cause()));
      }
      return;
    }

    try {
      final XyzResponse response = r.result();
      if (!(response instanceof FeatureCollection)) {
        callback.throwException(Api.responseToHttpException(response));
        return;
      }
      final FeatureCollection collection = (FeatureCollection) response;
      final List<Feature> features = collection.getFeatures();

      if (features.size() == 1) {
        this.getEvent().setGeometry(features.get(0).getGeometry());
      }

      callback.success(this);
    } catch (Exception e) {
      callback.throwException(e);
    }
  }
}
