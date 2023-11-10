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

import static com.here.naksha.app.service.http.tasks.StorageApiTask.StorageApiReqType.CREATE_STORAGE;
import static com.here.naksha.app.service.http.tasks.StorageApiTask.StorageApiReqType.GET_ALL_STORAGES;
import static com.here.naksha.app.service.http.tasks.StorageApiTask.StorageApiReqType.GET_STORAGE_BY_ID;
import static com.here.naksha.app.service.http.tasks.StorageApiTask.StorageApiReqType.UPDATE_STORAGE;

import com.here.naksha.app.service.http.NakshaHttpVerticle;
import com.here.naksha.app.service.http.tasks.StorageApiTask;
import com.here.naksha.app.service.http.tasks.StorageApiTask.StorageApiReqType;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;
import org.jetbrains.annotations.NotNull;

public class StorageApi extends Api {

  public StorageApi(final @NotNull NakshaHttpVerticle verticle) {
    super(verticle);
  }

  @Override
  public void addOperations(final @NotNull RouterBuilder rb) {
    rb.operation("getStorages").handler(this::getStorages);
    rb.operation("getStorageById").handler(this::getStorageById);
    rb.operation("postStorage").handler(this::createStorage);
    rb.operation("putStorage").handler(this::updateStorage);
  }

  @Override
  public void addManualRoutes(final @NotNull Router router) {}

  private void getStorages(final @NotNull RoutingContext routingContext) {
    startStorageApiTask(GET_ALL_STORAGES, routingContext);
  }

  private void getStorageById(final @NotNull RoutingContext routingContext) {
    startStorageApiTask(GET_STORAGE_BY_ID, routingContext);
  }

  private void createStorage(final @NotNull RoutingContext routingContext) {
    startStorageApiTask(CREATE_STORAGE, routingContext);
  }

  private void updateStorage(final @NotNull RoutingContext routingContext) {
    startStorageApiTask(UPDATE_STORAGE, routingContext);
  }

  private void startStorageApiTask(StorageApiReqType reqType, RoutingContext routingContext) {
    new StorageApiTask<>(reqType, verticle, naksha(), routingContext, verticle.createNakshaContext(routingContext))
        .start();
  }
}
