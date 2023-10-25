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
package com.here.naksha.app.service.http.tasks;

import com.here.naksha.app.service.http.NakshaHttpVerticle;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaAdminCollection;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.naksha.Storage;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.storage.IReadSession;
import com.here.naksha.lib.core.storage.IWriteSession;
import com.here.naksha.lib.core.util.json.Json;
import com.here.naksha.lib.core.util.storage.RequestHelper;
import com.here.naksha.lib.core.view.ViewDeserialize;
import io.vertx.ext.web.RoutingContext;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageApiTask<T extends XyzResponse> extends AbstractApiTask<XyzResponse> {

  private static final Logger logger = LoggerFactory.getLogger(StorageApiTask.class);
  private final @NotNull StorageApiReqType reqType;

  public enum StorageApiReqType {
    GET_ALL_STORAGES,
    GET_STORAGE_BY_ID,
    CREATE_STORAGE,
    UPDATE_STORAGE,
    DELETE_STORAGE
  }

  public StorageApiTask(
      final @NotNull StorageApiReqType reqType,
      final @NotNull NakshaHttpVerticle verticle,
      final @NotNull INaksha nakshaHub,
      final @NotNull RoutingContext routingContext,
      final @NotNull NakshaContext nakshaContext) {
    super(verticle, nakshaHub, routingContext, nakshaContext);
    this.reqType = reqType;
  }

  /**
   * Initializes this task.
   */
  @Override
  protected void init() {}

  /**
   * Execute this task.
   *
   * @return the response.
   */
  @Override
  protected @NotNull XyzResponse execute() {
    try {
      switch (this.reqType) {
        case GET_ALL_STORAGES:
          return executeGetStorages();
        case CREATE_STORAGE:
          return executeCreateStorage();
        default:
          return executeUnsupported();
      }
    } catch (Exception ex) {
      // unexpected exception
      return verticle.sendErrorResponse(
          routingContext, XyzError.EXCEPTION, "Internal error : " + ex.getMessage());
    }
  }

  private @NotNull XyzResponse executeGetStorages() {
    // Create ReadFeatures Request to read all storages from Admin DB
    final ReadFeatures request = new ReadFeatures(NakshaAdminCollection.STORAGES);
    // Submit request to NH Space Storage
    try (final IReadSession reader = naksha().getSpaceStorage().newReadSession(context(), false)) {
      final Result rdResult = reader.execute(request);
      // transform ReadResult to Http FeatureCollection response
      return transformReadResultToXyzCollectionResponse(rdResult, Storage.class);
    }
  }

  private @NotNull XyzResponse executeCreateStorage() throws Exception {
    // Read request JSON
    Storage newStorage = null;
    try (final Json json = Json.get()) {
      final String bodyJson = routingContext.body().asString();
      newStorage = json.reader(ViewDeserialize.User.class)
          .forType(Storage.class)
          .readValue(bodyJson);
    }
    // persist new storage in Admin DB (if doesn't exist already)
    try (final IWriteSession writer = naksha().getSpaceStorage().newWriteSession(context(), true)) {
      final WriteFeatures<Storage> wrRequest =
          RequestHelper.createFeatureRequest(NakshaAdminCollection.STORAGES, newStorage, false);
      final Result wrResult = writer.execute(wrRequest);
      // transform WriteResult to Http Feature response
      return transformWriteResultToXyzFeatureResponse(wrResult, Storage.class);
    }
  }
}
