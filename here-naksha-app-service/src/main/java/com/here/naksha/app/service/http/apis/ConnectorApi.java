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
import com.here.naksha.lib.core.models.naksha.EventHandler;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeatureCollection;
import com.here.naksha.lib.core.models.payload.XyzResponse;
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

public class ConnectorApi extends Api {

  private static final Logger logger = LoggerFactory.getLogger(ConnectorApi.class);

  public ConnectorApi(final @NotNull NakshaHttpVerticle verticle) {
    super(verticle);
  }

  @Override
  public void addOperations(final @NotNull RouterBuilder rb) {
    rb.operation("getConnectors").handler(this::getConnectors);
    rb.operation("postConnector").handler(this::createConnector);
  }

  @Override
  public void addManualRoutes(final @NotNull Router router) {}

  private void getConnectors(final @NotNull RoutingContext routingContext) {
    // TODO : To support filtering id's supplied as query parameters
    app().executeTask(() -> {
      try (final IReadTransaction tx = naksha().storage().openReplicationTransaction(naksha().settings())) {
        // TODO HP : Fix reading all connectors
        /*
        final IResultSet<Connector> rs = tx.readFeatures(Connector.class, NakshaAdminCollection.CONNECTORS)
        .getAll(0, Integer.MAX_VALUE);
         */
        final IResultSet<EventHandler> rs = null;
        final List<@NotNull EventHandler> featureList = rs.toList(0, Integer.MAX_VALUE);
        // TODO HP_QUERY : Is this right place to close resource? (change in StorageApi accordingly)
        rs.close();
        final XyzFeatureCollection response = new XyzFeatureCollection();
        response.setFeatures(featureList);
        verticle.sendXyzResponse(routingContext, HttpResponseType.FEATURE_COLLECTION, response);
        return response;
      }
    });
  }

  private void createConnector(final @NotNull RoutingContext routingContext) {
    app().executeTask(() -> {
      EventHandler eventHandler = null;
      // Read request JSON
      try (final Json json = Json.get()) {
        final String bodyJson = routingContext.body().asString();
        eventHandler = json.reader(ViewDeserialize.User.class)
            .forType(EventHandler.class)
            .readValue(bodyJson);
      } catch (Exception e) {
        throw unchecked(e);
      }
      // Insert connector in database
      try (final IMasterTransaction tx = naksha().storage().openMasterTransaction(naksha().settings())) {
        // TODO HP : Fix adding new connector
        /*
        final ModifyFeaturesResp modifyResponse = tx.writeFeatures(
        Connector.class, NakshaAdminCollection.CONNECTORS)
        .modifyFeatures(new ModifyFeaturesReq<Connector>(true).insert(connector));
         */
        final ModifyFeaturesResp modifyResponse = null;
        tx.commit();
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