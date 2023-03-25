/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */

package com.here.xyz.hub.task.feature;

import com.here.mapcreator.ext.naksha.Naksha;
import com.here.xyz.events.Event;
import com.here.xyz.hub.rest.ApiParam;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.task.Task;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Connector;
import com.here.xyz.EventHandler;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Space.ConnectorRef;
import com.here.xyz.responses.XyzResponse;
import io.vertx.ext.web.RoutingContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * All tasks related to features in a space need to extend this class.
 *
 * @param <EVENT> the event type.
 * @param <TASK> the task type.
 */
public abstract class FeatureTask<EVENT extends Event, TASK extends FeatureTask<EVENT, TASK>> extends Task<EVENT, TASK> {

  /**
   * The space for this operation.
   */
  public final @NotNull Space space;

  /**
   * The storage connector to be used for this operation.
   */
  public final @NotNull Connector connector;

  /**
   * The storage processor.
   */
  public final @NotNull EventHandler processor;

  /**
   * The response.
   */
  @SuppressWarnings("rawtypes")
  private @Nullable XyzResponse response;

  public static final class FeatureKey {

    public static final String ID = "id";
    public static final String TYPE = "type";
    public static final String BBOX = "bbox";
    public static final String PROPERTIES = "properties";
    public static final String SPACE = "space";
    public static final String CREATED_AT = "createdAt";
    public static final String UPDATED_AT = "updatedAt";
    public static final String UUID = "uuid";
    public static final String PUUID = "puuid";
    public static final String MUUID = "muuid";
    public static final String REVISION = "revision";
  }

  /**
   * Create a new feature task, extract space-id from path-parameters.
   *
   * @param event        the event to process.
   * @param context      the routing context.
   * @param responseType the response type requested.
   * @throws IllegalStateException if something is not in a valid state, missing ids, storages ...
   */
  FeatureTask(@NotNull EVENT event, @NotNull RoutingContext context, ApiResponseType responseType) {
    super(event, context, responseType);
    if (context.pathParam(ApiParam.Path.SPACE_ID) != null) {
      event.setSpace(context.pathParam(ApiParam.Path.SPACE_ID));
    }
    final @NotNull String spaceId = event.getSpace();
    // Ensure space.
    {
      final Space space = Naksha.spaces.get(spaceId);
      if (space == null) {
        throw new IllegalStateException("Unknown space " + spaceId);
      }
      this.space = space;
    }
    // Ensure storage.
    {
      final @NotNull ConnectorRef storageRef = space.getConnectorId();
      final @NotNull String connectorId = storageRef.getId();
      final Connector connector = Naksha.connectors.get(connectorId);
      if (connector == null) {
        throw new IllegalStateException("Unknown connector " + connectorId);
      }
      this.connector = connector;
    }
    // Ensure processor.
    {
      final String processorId = connector.eventHandlerId;
      if (processorId == null) {
        throw new IllegalStateException("Missing 'processor' in connector configuration");
      }
      final EventHandler processor = EventHandler.get(processorId);
      if (processor == null) {
        throw new IllegalStateException("The referred processor " + processorId + " does not exist");
      }
      this.processor = processor;
    }
  }

  @Override
  public @Nullable String getEtag() {
    if (response == null) {
      return null;
    }
    return response.getEtag();
  }

  /**
   * Returns the current response.
   *
   * @return the current response.
   */
  public @Nullable XyzResponse<?> getResponse() {
    return response;
  }

  /**
   * Sets the response to the given value.
   *
   * @param response the response.
   * @return the previously set value.
   */
  @SuppressWarnings("rawtypes")
  public XyzResponse setResponse(XyzResponse response) {
    final XyzResponse old = this.response;
    this.response = response;
    return old;
  }

  /**
   * Returns the response feature collection, if the response is a feature collection.
   *
   * @return the response feature collection, if the response is a feature collection.
   * @deprecated please rather use {@link #getResponse()}
   */
  public @Nullable FeatureCollection responseCollection() {
    if (response instanceof FeatureCollection) {
      return (FeatureCollection) response;
    }
    return null;
  }

}
