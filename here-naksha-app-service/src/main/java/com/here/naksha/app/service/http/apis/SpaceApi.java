/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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
package com.here.naksha.app.service.http.apis;

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;

import com.here.naksha.app.service.http.HttpResponseType;
import com.here.naksha.app.service.http.NakshaHttpVerticle;
import com.here.naksha.lib.core.EventPipeline;
import com.here.naksha.lib.core.models.naksha.EventHandler;
import com.here.naksha.lib.core.models.naksha.Space;
import com.here.naksha.lib.core.models.naksha.Storage;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeatureCollection;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.events.space.ModifySpaceEvent;
import com.here.naksha.lib.core.models.payload.responses.ErrorResponse;
import com.here.naksha.lib.core.storage.*;
import com.here.naksha.lib.core.util.json.Json;
import com.here.naksha.lib.core.view.ViewDeserialize;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;
import java.sql.SQLException;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpaceApi extends Api {

  private static final Logger logger = LoggerFactory.getLogger(SpaceApi.class);

  public SpaceApi(final @NotNull NakshaHttpVerticle verticle) {
    super(verticle);
  }

  @Override
  public void addOperations(final @NotNull RouterBuilder rb) {
    rb.operation("getSpaces").handler(this::getSpaces);
    rb.operation("postSpace").handler(this::createSpace);
  }

  @Override
  public void addManualRoutes(final @NotNull Router router) {}

  private void getSpaces(final @NotNull RoutingContext routingContext) {
    app().executeTask(() -> {
      try (final IReadTransaction tx = naksha().storage().openReplicationTransaction(naksha().settings())) {
        // TODO HP : Fix reading of all spaces
        /*
        final IResultSet<Space> rs = tx.readFeatures(Space.class, NakshaAdminCollection.SPACES)
        .getAll(0, Integer.MAX_VALUE);
        */
        final IResultSet<Space> rs = null;
        final List<@NotNull Space> featureList = rs.toList(0, Integer.MAX_VALUE);
        rs.close();
        final XyzFeatureCollection response = new XyzFeatureCollection();
        response.setFeatures(featureList);
        verticle.sendXyzResponse(routingContext, HttpResponseType.FEATURE_COLLECTION, response);
        return response;
      }
    });
  }

  private void createSpace(final @NotNull RoutingContext routingContext) {
    // TODO HP_QUERY : How to ensure streamId is logged correctly throughout code?
    app().executeTask(() -> {
      Space space = null;
      // Read request JSON
      try (final Json json = Json.get()) {
        final String bodyJson = routingContext.body().asString();
        space = json.reader(ViewDeserialize.User.class)
            .forType(Space.class)
            .readValue(bodyJson);
      } catch (Exception e) {
        throw unchecked(e);
      }
      // Validate and Insert space in Admin database
      try (final IMasterTransaction tx = naksha().storage().openMasterTransaction(naksha().settings())) {
        // TODO : Validate if spaceId already exist
        // TODO : Validate if connectorIds exist
        // Insert space details in Admin database
        // TODO HP : Fix adding new space
        /*
        final ModifyFeaturesResp modifyResponse = tx.writeFeatures(Space.class, NakshaAdminCollection.SPACES)
        .modifyFeatures(new ModifyFeaturesReq<Space>(true).insert(space));
        */
        final ModifyFeaturesResp modifyResponse = null;
        tx.commit();

        // TODO HP_QUERY : How to trigger feature task pipeline and create table as part of pipeline?
        // With connectorId (attached to a storage) ensure backend tables are created successfully
        // TODO HP : Fix fetching connectors
        /*
        final IResultSet<Connector> rsc = tx.readFeatures(Connector.class, NakshaAdminCollection.CONNECTORS)
        .getFeaturesById(space.getConnectorIds());
        */
        final IResultSet<EventHandler> rsc = null;
        final List<EventHandler> eventHandlerList = rsc.toList(0, Integer.MAX_VALUE);
        rsc.close();
        final EventPipeline eventPipeline = new EventPipeline(naksha());
        for (final EventHandler eventHandler : eventHandlerList) {
          // fetch storage associated with this connector
          Storage storage = null;
          if (eventHandler.getStorageId() != null) {
            // TODO HP : Fix fetching storages
            /*
            final IResultSet<Storage> rss = tx.readFeatures(Storage.class, NakshaAdminCollection.STORAGES)
            .getFeaturesById(connector.getStorageId());
            */
            final IResultSet<Storage> rss = null;
            if (rss.next()) {
              storage = rss.getFeature();
            }
            rss.close();
          }
          eventHandler.setStorage(storage);
          eventPipeline.addEventHandler(eventHandler);
        }
        // Send ModifySpaceEvent through all connectors, one of which, will create backend table(s)
        final ModifySpaceEvent event = new ModifySpaceEvent()
            .withOperation(ModifySpaceEvent.Operation.CREATE)
            .withSpaceDefinition(space);
        event.setSpace(space);
        final XyzResponse pipelineResponse = eventPipeline.sendEvent(event);
        if (pipelineResponse instanceof ErrorResponse errorResponse) {
          verticle.sendXyzResponse(routingContext, HttpResponseType.FEATURE, errorResponse);
          return errorResponse;
        }

        // return success response (if we reach to this stage)
        final XyzFeatureCollection response = verticle.transformModifyResponse(modifyResponse);
        verticle.sendXyzResponse(routingContext, HttpResponseType.FEATURE, response);
        return response;
      } catch (final Throwable t) {
        if (t.getCause() instanceof SQLException se) {
          // TODO HP_QUERY : Correct way to access streamId?
          logger.warn("Error processing request. ", se);
          final XyzResponse errResponse = new ErrorResponse(se, verticle.streamId(routingContext));
          verticle.sendXyzResponse(routingContext, HttpResponseType.FEATURE, errResponse);
          return errResponse;
        }
        // TODO HP_QUERY : Is there a common recommended way to handle other errors? (and return appropriate
        // error response)
        throw t;
      }
    });
  }
}
