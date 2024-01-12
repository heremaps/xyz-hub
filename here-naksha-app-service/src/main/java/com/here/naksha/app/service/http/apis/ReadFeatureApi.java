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

import static com.here.naksha.app.service.http.tasks.ReadFeatureApiTask.ReadFeatureApiReqType.*;

import com.here.naksha.app.service.http.NakshaHttpVerticle;
import com.here.naksha.app.service.http.tasks.ReadFeatureApiTask;
import com.here.naksha.app.service.http.tasks.ReadFeatureApiTask.ReadFeatureApiReqType;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadFeatureApi extends Api {

  private static final Logger logger = LoggerFactory.getLogger(ReadFeatureApi.class);

  public ReadFeatureApi(final @NotNull NakshaHttpVerticle verticle) {
    super(verticle);
  }

  @Override
  public void addOperations(final @NotNull RouterBuilder rb) {
    rb.operation("getFeature").handler(this::getFeatureById);
    rb.operation("getFeatures").handler(this::getFeaturesById);
    rb.operation("getFeaturesByBBox").handler(this::getFeaturesByBBox);
    rb.operation("getFeaturesByTile").handler(this::getFeaturesByTile);
    rb.operation("searchForFeatures").handler(this::searchFeatures);
    rb.operation("iterateFeatures").handler(this::iterateFeatures);
  }

  @Override
  public void addManualRoutes(final @NotNull Router router) {}

  private void getFeaturesById(final @NotNull RoutingContext routingContext) {
    startReadFeatureApiTask(GET_BY_IDS, routingContext);
  }

  private void getFeatureById(final @NotNull RoutingContext routingContext) {
    startReadFeatureApiTask(GET_BY_ID, routingContext);
  }

  private void getFeaturesByBBox(final @NotNull RoutingContext routingContext) {
    startReadFeatureApiTask(GET_BY_BBOX, routingContext);
  }

  private void getFeaturesByTile(final @NotNull RoutingContext routingContext) {
    startReadFeatureApiTask(GET_BY_TILE, routingContext);
  }

  private void searchFeatures(final @NotNull RoutingContext routingContext) {
    startReadFeatureApiTask(SEARCH, routingContext);
  }

  private void iterateFeatures(final @NotNull RoutingContext routingContext) {
    startReadFeatureApiTask(ITERATE, routingContext);
  }

  private void startReadFeatureApiTask(ReadFeatureApiReqType reqType, RoutingContext routingContext) {
    new ReadFeatureApiTask<>(
            reqType, verticle, naksha(), routingContext, verticle.createNakshaContext(routingContext))
        .start();
  }
}
