/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

import static com.here.naksha.app.service.http.tasks.WriteFeatureApiTask.WriteFeatureApiReqType.*;

import com.here.naksha.app.service.http.NakshaHttpVerticle;
import com.here.naksha.app.service.http.tasks.WriteFeatureApiTask;
import com.here.naksha.app.service.http.tasks.WriteFeatureApiTask.WriteFeatureApiReqType;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteFeatureApi extends Api {

  private static final Logger logger = LoggerFactory.getLogger(WriteFeatureApi.class);

  public WriteFeatureApi(final @NotNull NakshaHttpVerticle verticle) {
    super(verticle);
  }

  @Override
  public void addOperations(final @NotNull RouterBuilder rb) {
    rb.operation("postFeatures").handler(this::createFeatures);
    rb.operation("putFeatures").handler(this::upsertFeatures);
    rb.operation("putFeature").handler(this::updateFeature);
    rb.operation("deleteFeatures").handler(this::deleteFeatures);
    rb.operation("deleteFeature").handler(this::deleteFeature);
    rb.operation("patchFeature").handler(this::patchFeature);
  }

  @Override
  public void addManualRoutes(final @NotNull Router router) {}

  private void createFeatures(final @NotNull RoutingContext routingContext) {
    startWriteFeatureApiTask(CREATE_FEATURES, routingContext);
  }

  private void upsertFeatures(final @NotNull RoutingContext routingContext) {
    startWriteFeatureApiTask(UPSERT_FEATURES, routingContext);
  }

  private void updateFeature(final @NotNull RoutingContext routingContext) {
    startWriteFeatureApiTask(UPDATE_BY_ID, routingContext);
  }

  private void deleteFeatures(final @NotNull RoutingContext routingContext) {
    startWriteFeatureApiTask(DELETE_FEATURES, routingContext);
  }

  private void deleteFeature(final @NotNull RoutingContext routingContext) {
    startWriteFeatureApiTask(DELETE_BY_ID, routingContext);
  }

  private void patchFeature(final @NotNull RoutingContext routingContext) {
    startWriteFeatureApiTask(PATCH_BY_ID, routingContext);
  }

  private void startWriteFeatureApiTask(WriteFeatureApiReqType reqType, RoutingContext routingContext) {
    new WriteFeatureApiTask<>(
            reqType, verticle, naksha(), routingContext, verticle.createNakshaContext(routingContext))
        .start();
  }
}
