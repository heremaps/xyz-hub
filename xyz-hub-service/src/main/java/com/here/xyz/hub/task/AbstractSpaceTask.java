package com.here.xyz.hub.task;

import com.here.xyz.events.Event;
import com.here.xyz.exceptions.ParameterError;
import com.here.xyz.hub.rest.ApiParam;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.models.hub.Connector;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.JsonUtils;
import io.vertx.ext.web.RoutingContext;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A task that related to a space.
 */
public abstract class AbstractSpaceTask<EVENT extends Event> extends AbstractEventTask<EVENT> {

  protected AbstractSpaceTask(@Nullable String streamId) {
    super(streamId);
  }

  @Override
  public void initFromRoutingContext(@NotNull RoutingContext routingContext, @NotNull ApiResponseType responseType) throws ParameterError {
    super.initFromRoutingContext(routingContext, responseType);

    if (routingContext.pathParam(ApiParam.Path.SPACE_ID) == null) {
      throw new ParameterError("Missing space path parameter");
    }
    final @NotNull String spaceId = routingContext.pathParam(ApiParam.Path.SPACE_ID);
    if (spaceId == null) {
      throw new ParameterError("Missing space path parameter");
    }
    final Space space = Space.getById(spaceId);
    if (space == null) {
      throw new ParameterError("Unknown space " + spaceId);
    }
    setSpace(space);
  }

  /**
   * Sets the space.
   *
   * @param space The space to use.
   */
  public void setSpace(@NotNull Space space) {
    event.setSpaceId(space.getId());
    event.setParams(JsonUtils.deepCopy(space.params));

    final List<@NotNull String> connectors = space.connectors;
    if (connectors != null && connectors.size() > 0) {
      final String storageConnectorId = connectors.get(connectors.size() - 1);
      final Connector storageConnector = Connector.getConnectorById(storageConnectorId);
      if (storageConnector != null) {
        event.setConnectorId(storageConnector.id);
        event.setConnectorNumber(storageConnector.number);
        event.setConnectorParams(JsonUtils.deepCopy(storageConnector.params));
      }
    }
  }

  /**
   * The space on which this task operates.
   */
  private Space space;

  /**
   * Returns the space.
   * @return The space.
   * @throws ParameterError If the space parameter is missing or invalid.
   */
  public @NotNull Space getSpace() throws ParameterError {
    if (space == null) {
      throw new ParameterError("Missing space");
    }
    return space;
  }

  @Override
  protected @NotNull XyzResponse execute() throws Exception {
    pipeline.addSpaceHandler(getSpace());
    return sendAuthorizedEvent(event, requestMatrix());
  }
}