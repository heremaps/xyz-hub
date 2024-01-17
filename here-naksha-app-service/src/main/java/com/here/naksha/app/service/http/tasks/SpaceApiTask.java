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

import static com.here.naksha.app.service.http.apis.ApiParams.SPACE_ID;
import static com.here.naksha.lib.core.NakshaAdminCollection.SPACES;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.naksha.app.service.http.NakshaHttpVerticle;
import com.here.naksha.app.service.http.apis.ApiParams;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.naksha.Space;
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

public class SpaceApiTask<T extends XyzResponse> extends AbstractApiTask<XyzResponse> {

  private static final Logger logger = LoggerFactory.getLogger(SpaceApiTask.class);
  private final @NotNull SpaceApiReqType reqType;

  public enum SpaceApiReqType {
    GET_ALL_SPACES,
    GET_SPACE_BY_ID,
    CREATE_SPACE,
    UPDATE_SPACE,
    DELETE_SPACE
  }

  public SpaceApiTask(
      final @NotNull SpaceApiReqType reqType,
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
        case CREATE_SPACE -> executeCreateSpace();
        case UPDATE_SPACE -> executeUpdateSpace();
        case GET_ALL_SPACES -> executeGetSpaces();
        case GET_SPACE_BY_ID -> executeGetSpaceById();
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

  private @NotNull XyzResponse executeCreateSpace() throws JsonProcessingException {
    final Space newSpace = spaceFromRequestBody();
    final WriteXyzFeatures wrRequest = RequestHelper.createFeatureRequest(SPACES, newSpace, false);
    try (Result wrResult = executeWriteRequestFromSpaceStorage(wrRequest)) {
      return transformWriteResultToXyzFeatureResponse(wrResult, Space.class);
    }
  }

  private @NotNull XyzResponse executeUpdateSpace() throws JsonProcessingException {
    final String spaceIdFromPath = ApiParams.extractMandatoryPathParam(routingContext, SPACE_ID);
    final Space spaceFromBody = spaceFromRequestBody();
    if (!spaceFromBody.getId().equals(spaceIdFromPath)) {
      return verticle.sendErrorResponse(
          routingContext, XyzError.ILLEGAL_ARGUMENT, mismatchMsg(spaceIdFromPath, spaceFromBody));
    } else {
      final WriteXyzFeatures updateSpaceReq = RequestHelper.updateFeatureRequest(SPACES, spaceFromBody);
      try (Result updateSpaceResult = executeWriteRequestFromSpaceStorage(updateSpaceReq)) {
        return transformWriteResultToXyzFeatureResponse(updateSpaceResult, Space.class);
      }
    }
  }

  private @NotNull XyzResponse executeGetSpaces() {
    final ReadFeatures request = new ReadFeatures(SPACES);
    try (Result rdResult = executeReadRequestFromSpaceStorage(request)) {
      return transformReadResultToXyzCollectionResponse(rdResult, Space.class);
    }
  }

  private @NotNull XyzResponse executeGetSpaceById() {
    final String spaceId = ApiParams.extractMandatoryPathParam(routingContext, SPACE_ID);
    final ReadFeatures request = new ReadFeatures(SPACES).withPropertyOp(POp.eq(PRef.id(), spaceId));
    try (Result rdResult = executeReadRequestFromSpaceStorage(request)) {
      return transformReadResultToXyzFeatureResponse(rdResult, Space.class);
    }
  }

  private Space spaceFromRequestBody() throws JsonProcessingException {
    try (final Json json = Json.get()) {
      final String bodyJson = routingContext.body().asString();
      return json.reader(ViewDeserialize.User.class).forType(Space.class).readValue(bodyJson);
    }
  }

  private static String mismatchMsg(String spaceIdFromPath, Space spaceFromBody) {
    return "Mismatch between space ids. Path space id: %s, body space id: %s"
        .formatted(spaceIdFromPath, spaceFromBody.getId());
  }
}
