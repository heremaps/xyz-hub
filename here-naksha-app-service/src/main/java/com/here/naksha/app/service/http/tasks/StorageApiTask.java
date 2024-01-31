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

import static com.here.naksha.app.service.http.apis.ApiParams.STORAGE_ID;
import static com.here.naksha.app.service.http.ops.PasswordMaskingUtil.removePasswordFromProps;
import static com.here.naksha.lib.core.NakshaAdminCollection.STORAGES;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.naksha.app.service.http.NakshaHttpVerticle;
import com.here.naksha.app.service.http.apis.ApiParams;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.naksha.Storage;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.storage.POp;
import com.here.naksha.lib.core.models.storage.PRef;
import com.here.naksha.lib.core.models.storage.ReadFeatures;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.WriteXyzFeatures;
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
      return switch (this.reqType) {
        case GET_ALL_STORAGES -> executeGetStorages();
        case GET_STORAGE_BY_ID -> executeGetStorageById();
        case CREATE_STORAGE -> executeCreateStorage();
        case UPDATE_STORAGE -> executeUpdateStorage();
        case DELETE_STORAGE -> executeDeleteStorage();
        default -> executeUnsupported();
      };
    } catch (Exception ex) {
      if (ex instanceof XyzErrorException xyz) {
        logger.warn("Known exception while processing request. ", ex);
        return verticle.sendErrorResponse(routingContext, xyz.xyzError, xyz.getMessage());
      } else {
        logger.error("Unexpected error while processing request. ", ex);
        return verticle.sendErrorResponse(
            routingContext, XyzError.EXCEPTION, "Internal error : " + ex.getMessage());
      }
    }
  }

  private @NotNull XyzResponse executeGetStorages() {
    final ReadFeatures request = new ReadFeatures(STORAGES);
    try (Result rdResult = executeReadRequestFromSpaceStorage(request)) {
      return transformReadResultToXyzCollectionResponse(
          rdResult, Storage.class, f -> removePasswordFromProps(f.getProperties()));
    }
  }

  private @NotNull XyzResponse executeGetStorageById() {
    final String storageId = ApiParams.extractMandatoryPathParam(routingContext, STORAGE_ID);
    final ReadFeatures request = new ReadFeatures(STORAGES).withPropertyOp(POp.eq(PRef.id(), storageId));
    try (Result rdResult = executeReadRequestFromSpaceStorage(request)) {
      return transformReadResultToXyzFeatureResponse(
          rdResult, Storage.class, f -> removePasswordFromProps(f.getProperties()));
    }
  }

  private @NotNull XyzResponse executeCreateStorage() throws JsonProcessingException {
    final Storage newStorage = storageFromRequestBody();
    final WriteXyzFeatures wrRequest = RequestHelper.createFeatureRequest(STORAGES, newStorage, false);
    try (Result wrResult = executeWriteRequestFromSpaceStorage(wrRequest)) {
      return transformWriteResultToXyzFeatureResponse(
          wrResult, Storage.class, f -> removePasswordFromProps(f.getProperties()));
    }
  }

  private @NotNull XyzResponse executeUpdateStorage() throws JsonProcessingException {
    final String storageIdFromPath = ApiParams.extractMandatoryPathParam(routingContext, STORAGE_ID);
    final Storage storageFromBody = storageFromRequestBody();
    if (!storageFromBody.getId().equals(storageIdFromPath)) {
      return verticle.sendErrorResponse(
          routingContext, XyzError.ILLEGAL_ARGUMENT, mismatchMsg(storageIdFromPath, storageFromBody));
    } else {
      final WriteXyzFeatures updateStorageReq = RequestHelper.updateFeatureRequest(STORAGES, storageFromBody);
      try (Result updateStorageResult = executeWriteRequestFromSpaceStorage(updateStorageReq)) {
        return transformWriteResultToXyzFeatureResponse(
            updateStorageResult, Storage.class, f -> removePasswordFromProps(f.getProperties()));
      }
    }
  }

  private @NotNull XyzResponse executeDeleteStorage() {
    final String storageId = ApiParams.extractMandatoryPathParam(routingContext, STORAGE_ID);
    final WriteXyzFeatures wrRequest = RequestHelper.deleteFeatureByIdRequest(STORAGES, storageId);
    try (Result wrResult = executeWriteRequestFromSpaceStorage(wrRequest)) {
      return transformDeleteResultToXyzFeatureResponse(
          wrResult, Storage.class, f -> removePasswordFromProps(f.getProperties()));
    }
  }

  private @NotNull Storage storageFromRequestBody() throws JsonProcessingException {
    try (final Json json = Json.get()) {
      final String bodyJson = routingContext.body().asString();
      return json.reader(ViewDeserialize.User.class)
          .forType(Storage.class)
          .readValue(bodyJson);
    }
  }

  private static String mismatchMsg(String storageIdFromPath, Storage storageFromBody) {
    return "Mismatch between storage ids. Path storage id: %s, body storage id: %s"
        .formatted(storageIdFromPath, storageFromBody.getId());
  }
}
