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

import com.here.naksha.app.service.http.HttpResponseType;
import com.here.naksha.app.service.http.NakshaHttpVerticle;
import com.here.naksha.lib.core.NakshaAdminCollection;
import com.here.naksha.lib.core.models.features.Connector;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeatureCollection;
import com.here.naksha.lib.core.storage.IReadTransaction;
import com.here.naksha.lib.core.storage.IResultSet;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public class ConnectorApi extends Api {

  public ConnectorApi(final @NotNull NakshaHttpVerticle verticle) {
    super(verticle);
  }

  @Override
  public void addOperations(final @NotNull RouterBuilder rb) {
    rb.operation("getConnectors").handler(this::getConnectors);
  }

  @Override
  public void addManualRoutes(final @NotNull Router router) {}

  private void getConnectors(final @NotNull RoutingContext routingContext) {
    naksha().executeTask(() -> {
      try (final IReadTransaction tx = naksha().adminStorage().openReplicationTransaction()) {
        final IResultSet<Connector> rs = tx.readFeatures(Connector.class, NakshaAdminCollection.CONNECTORS)
            .getAll(0, Integer.MAX_VALUE);
        final List<@NotNull Connector> featureList = rs.toList(0, Integer.MAX_VALUE);
        final XyzFeatureCollection response = new XyzFeatureCollection();
        response.setFeatures(featureList);
        verticle.sendXyzResponse(routingContext, HttpResponseType.FEATURE_COLLECTION, response);
        return response;
      }
    });
  }
}
