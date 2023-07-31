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
import com.here.naksha.lib.core.NakshaAdminCollection;
import com.here.naksha.lib.core.models.features.Connector;
import com.here.naksha.lib.core.models.features.Space;
import com.here.naksha.lib.core.models.features.Storage;
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
import java.util.List;
import org.jetbrains.annotations.NotNull;

public class SpaceApi extends Api {

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
    naksha().executeTask(() -> {
      try (final IReadTransaction tx = naksha().storage().openReplicationTransaction(naksha().settings())) {
        final IResultSet<Space> rs = tx.readFeatures(Space.class, NakshaAdminCollection.SPACES)
            .getAll(0, Integer.MAX_VALUE);
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
    naksha().executeTask(() -> {
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
        final ModifyFeaturesResp modifyResponse = tx.writeFeatures(Space.class, NakshaAdminCollection.SPACES)
            .modifyFeatures(new ModifyFeaturesReq<Space>(true).insert(space));
        tx.commit();

        // TODO HP_QUERY : How to trigger feature task pipeline and create table as part of pipeline?
        // With connectorId (attached to a storage) ensure backend tables are created successfully
        final IResultSet<Connector> rsc = tx.readFeatures(Connector.class, NakshaAdminCollection.CONNECTORS)
            .getFeaturesById(space.getConnectorIds());
        final List<Connector> connectorList = rsc.toList(0, Integer.MAX_VALUE);
        rsc.close();
        for (final Connector connector : connectorList) {
          // fetch storage associated with this connector
          Storage storage = null;
          if (connector.getStorageId() != null) {
            final IResultSet<Storage> rss = tx.readFeatures(Storage.class, NakshaAdminCollection.STORAGES)
                .getFeaturesById(connector.getStorageId());
            if (rss.next()) {
              storage = rss.getFeature();
            }
            rss.close();
          }
          connector.setStorage(storage);
          // Create backend table(s) using this connector
          final ModifySpaceEvent event = new ModifySpaceEvent()
              .withOperation(ModifySpaceEvent.Operation.CREATE)
              .withSpaceDefinition(space);
          event.setSpace(space);
          final XyzResponse response = new EventPipeline(naksha())
              .addEventHandler(connector)
              .sendEvent(event);
          if (response instanceof ErrorResponse) {
            return response;
          }
        }

        // return success response (if we reach to this stage)
        final XyzFeatureCollection response = verticle.transformModifyResponse(modifyResponse);
        verticle.sendXyzResponse(routingContext, HttpResponseType.FEATURE_COLLECTION, response);
        return response;
      }
    });
  }
}
